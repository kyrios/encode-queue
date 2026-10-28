package queue

import (
	"context"
	"encoding/json"
	"testing"
)

// "Sofort" (Thorsten, 2026-10-27): a job someone in Funkhaus waits for goes first in line, takes
// a worker's extra slot beside a long encode when its RAM allows ("Extra slot"), and never
// preempts.

const gib = int64(1) << 30

// sofortWhisper submits captions from the audio at priority now, whisper medium.
func (e *env) sofortWhisper(id string) {
	e.t.Helper()
	f := e.source(id+"/sound.wav", "RIFF "+id)
	if _, _, err := e.s.Submit(id, JobSubmit{Kind: KindWhisper, Priority: PriorityNow,
		Params: json.RawMessage(`{"model_sha256":"` + modelSHA + `","model":"ggml-medium.bin","language":"de"}`),
		Source: SourceIn{FileIn: f, Container: "wav"}, Result: ResultSpec{Name: "whisper.json", MaxBytes: 1 << 20}}); err != nil {
		e.t.Fatal(err)
	}
}

// machine gives a worker its RAM: total from its hardware, in use from its presence.
func (e *env) machine(wid string, total, used int64) {
	e.t.Helper()
	hw, _ := json.Marshal(map[string]any{"cpu_threads": 6, "memory_bytes": total})
	if _, err := e.s.PutCapabilities(wid, Capabilities{AppVersion: "0.2.5", OS: "test", Tools: reference, Models: []Model{{"ggml-medium.bin", modelSHA}}, Hardware: hw}); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.s.PutPresence(wid, Presence{State: "working", MemoryUsed: &used}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) extra(wid string) (*LeaseView, error) {
	return e.s.Claim(context.Background(), wid, LeaseRequest{Extra: true}, 0)
}

func TestSofortGoesFirst(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("a.stage1", "stage1")
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if l := e.claim(w); l == nil || l.Job.ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("claimed %+v, want the Sofort job before stage 1", l)
	}
}

func TestSofortNeverPreempts(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("old.archive", "archive")
	l := e.claim(w)
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("a Sofort job preempted")
	}
	if v := must(e.s.GetJob("old.archive")); v.State != "leased" || v.Attempts[0].LeaseID != l.ID {
		t.Fatalf("the archive job: %+v", v)
	}
	// A stage 1 job still preempts as before.
	e.rung("new.stage1", "stage1")
	if !e.s.preempt(e.clock.Now()) {
		t.Fatal("stage 1 no longer preempts")
	}
}

func TestExtraSlotBesideAnEncode(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference) // max_parallel_jobs 1, "Aufträge gleichzeitig 1"
	e.machine(w, 32*gib, 6*gib)
	e.rung("old.2160-archive", "archive")
	enc := e.claim(w)
	if enc == nil {
		t.Fatal("no encode")
	}
	e.rung("b.stage2", "stage2")
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	// The regular slot is full; the extra slot takes the Sofort job, never the stage 2 one.
	if _, err := e.s.Claim(context.Background(), w, LeaseRequest{}, 0); err == nil {
		t.Fatal("the regular slot took a second job")
	}
	x, err := e.extra(w)
	if err != nil || x == nil || x.Job.ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("extra slot: %+v %v", x, err)
	}
	if !e.s.st.Leases[x.ID].High {
		t.Fatal("the lease is not marked extra")
	}
	v := must(e.s.GetWorker(w))
	extra := map[string]bool{}
	for _, j := range v.Jobs {
		extra[j.JobID] = j.Extra
	}
	if len(v.Jobs) != 2 || extra["old.2160-archive"] || !extra["k3v9x2m1q7pz.whisper"] {
		t.Fatalf("the worker's jobs: %+v", v.Jobs)
	}
	// One extra lease at a time.
	e.sofortWhisper("z9y8x7w6v5u4.whisper")
	if _, err := e.extra(w); err == nil {
		t.Fatal("a second extra lease")
	}
	// The encode still counts as the one regular slot; the extra lease does not hold one.
	if fa := e.s.freeAt(e.s.st.Workers[w], e.clock.Now(), "", false); fa != nil && !fa.After(e.clock.Now()) {
		t.Fatalf("free at %v while the encode runs", fa)
	}
	if err := e.s.Release(w, enc.ID, "user"); err != nil {
		t.Fatal(err)
	}
	if l := e.claim(w); l == nil || l.Job.ID != "z9y8x7w6v5u4.whisper" {
		t.Fatalf("the regular slot after the encode: %+v", l)
	}
}

func TestExtraSlotOnlyWhenTheRAMAllows(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	// encoder: 8 GB, a 4K slower encode in use takes 5.3 of it. 8 - 5.3 - 1 GB headroom < 2 GB.
	e.machine(w, 8*gib, 53*gib/10)
	e.rung("old.2160-archive", "archive")
	e.claim(w)
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if x, err := e.extra(w); err != nil || x != nil {
		t.Fatalf("an extra lease without the RAM: %+v %v", x, err)
	}
	// It waits first in line instead.
	if q := e.s.queued(); len(q) == 0 || q[0].ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("the Sofort job is not first in line: %+v", q)
	}
	// Unknown RAM never fits.
	e.s.st.Workers[w].MemoryUsed = nil
	if x, _ := e.extra(w); x != nil {
		t.Fatal("an extra lease with unknown RAM")
	}
	// Enough RAM: 8 - 4.4 - 1 >= 2.5.
	used := 44 * gib / 10
	e.s.PutPresence(w, Presence{State: "working", MemoryUsed: &used})
	if x, err := e.extra(w); err != nil || x == nil {
		t.Fatalf("no extra lease with the RAM: %v", err)
	}
}

func TestExtraSlotOnAnIdleWorker(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.rung("a.stage1", "stage1")
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	// Nothing runs: no RAM is asked, and the regular slot stays free for the stage 1 job.
	x, err := e.extra(w)
	if err != nil || x == nil || x.Job.ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("extra on an idle worker: %+v %v", x, err)
	}
	// Beside the high-priority slot's job the normal slot needs the RAM too (v0.2.14): unknown
	// RAM never fits, 32 GB do.
	if l, err := e.s.Claim(context.Background(), w, LeaseRequest{}, 0); err != nil || l != nil {
		t.Fatalf("regular beside it with unknown RAM: %+v %v", l, err)
	}
	e.machine(w, 32*gib, 6*gib)
	if l := e.claim(w); l == nil || l.Job.ID != "a.stage1" {
		t.Fatalf("regular beside it: %+v", l)
	}
}

func TestPeakBytes(t *testing.T) {
	j := func(w, h int, preset string) *Job {
		return &Job{rung: &RungParams{Width: w, Height: h, Preset: preset}}
	}
	for _, c := range []struct {
		job  *Job
		want int64
	}{
		{j(3840, 2160, "slower"), 11 * gib / 2},
		{j(3840, 2160, "ultrafast"), 4 * gib},
		{j(1920, 1080, "veryfast"), 2 * gib},
		{j(1280, 720, "medium"), gib},
		{&Job{whisper: &WhisperParams{Model: "ggml-medium.bin"}}, 5 * gib / 2},
		{&Job{whisper: &WhisperParams{Model: "ggml-large-v3.bin"}}, 4 * gib},
		{&Job{whisper: &WhisperParams{}}, 5 * gib / 2},
	} {
		if got := peakBytes(c.job); got != c.want {
			t.Errorf("%+v %+v: %d, want %d", c.job.rung, c.job.whisper, got, c.want)
		}
	}
}

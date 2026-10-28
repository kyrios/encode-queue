package queue

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/aligner"
)

// The align job (v0.2.16): Qwen3-ForcedAligner times a text's words. A Mac with MLX first,
// encoder's CPU as the fallback (Thorsten, 2026-10-28: "The macs are not always there. We need a
// fallback that works").

// aligns makes a worker one of 0.2.16 that runs this aligner engine and holds the model.
func (e *env) aligns(wid, engine string) {
	c := e.s.st.Workers[wid].Caps
	c.AppVersion = "0.2.16"
	c.Tools = append(append([]Tool(nil), c.Tools...), Tool{engine, map[string]string{aligner.EngineMLX: "0.4.4", aligner.EngineTorch: "0.0.6"}[engine]})
	c.Models = append(append([]Model(nil), c.Models...), Model{Name: aligner.Name, SHA256: aligner.SHA256})
}

func (e *env) words(id string) FileIn {
	return e.source(id+"/words.json", `{"words":[{"text":"Ein","start":0,"end":0.3}]}`)
}

// alignJob submits the Socials version's timing of a 30 s preview at priority now.
func (e *env) alignJob(id string, params string, container string) error {
	f := e.source(id+"/preview.mp4", "preview "+id)
	if container == "wav" {
		f = e.source(id+"/speech.wav", "RIFF "+id)
	}
	_, _, err := e.s.Submit(id, JobSubmit{Kind: KindAlign, Priority: PriorityNow, Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, Container: container, DurationS: 30}, Inputs: map[string]FileIn{"text": e.words(id)},
		Result: ResultSpec{Name: "align.json", MaxBytes: 1 << 24}})
	return err
}

const alignParams = `{"model_sha256":"` + aligner.SHA256 + `","model":"Qwen3-ForcedAligner-0.6B","language":"de","text":"text"}`

func TestAlignGoesToAMacFirst(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.aligns(enc, aligner.EngineTorch)
	e.aligns(mac, aligner.EngineMLX)
	if err := e.alignJob("a.align", alignParams, "mp4"); err != nil {
		t.Fatal(err)
	}
	// encoder asks first: the Mac, connected and asking, runs it with MLX, so encoder leaves it.
	if l := e.claim(enc); l != nil {
		t.Fatalf("encoder's CPU took a job the Mac's MLX runs: %+v", l)
	}
	l := e.claim(mac)
	if l == nil || l.Job.ID != "a.align" || string(l.Params) != alignParams || l.Job.Inputs["text"].URL == "" {
		t.Fatalf("the Mac: %+v", l)
	}
	v := must(e.s.GetJob("a.align"))
	if v.Attempts[0].ModelSHA256 == nil || *v.Attempts[0].ModelSHA256 != aligner.SHA256 || v.Quality == nil || *v.Quality != "Qwen3-ForcedAligner-0.6B de" {
		t.Fatalf("the job: %+v %+v", v.Quality, v.Attempts[0])
	}
	// The Mac gone, encoder's CPU takes the next.
	if err := e.alignJob("b.align", alignParams, "mp4"); err != nil {
		t.Fatal(err)
	}
	e.s.st.Workers[mac].LastSeen = nil
	if l := e.claim(enc); l == nil || l.Job.ID != "b.align" {
		t.Fatalf("encoder as the fallback: %+v", l)
	}
}

func TestAlignNeedsItsEngineModelAndVersion(t *testing.T) {
	e := newEnv(t, "")
	old := e.worker("old", reference)
	bare := e.worker("bare", reference)
	nomodel := e.worker("nomodel", reference)
	ok := e.worker("ok", reference)
	e.aligns(old, aligner.EngineMLX)
	e.s.st.Workers[old].Caps.AppVersion = "0.2.15"
	e.s.st.Workers[bare].Caps.AppVersion = "0.2.16"
	e.aligns(nomodel, aligner.EngineTorch)
	e.s.st.Workers[nomodel].Caps.Models = e.s.st.Workers[nomodel].Caps.Models[:1]
	e.aligns(ok, aligner.EngineTorch)
	if err := e.alignJob("a.align", alignParams, "wav"); err != nil {
		t.Fatal(err)
	}
	j := e.s.st.Jobs["a.align"]
	for wid, want := range map[string]string{
		old:     "worker 0.2.15 runs no align jobs (0.2.16)",
		bare:    "no aligner engine (mlx-qwen3-asr or qwen-asr)",
		nomodel: "model Qwen3-ForcedAligner-0.6B missing",
		ok:      "",
	} {
		if got := e.s.mismatch(e.s.st.Workers[wid], j); got != want {
			t.Errorf("%s: %q, want %q", e.s.st.Workers[wid].Name, got, want)
		}
	}
	// The capabilities' verdict: an older worker neither takes nor refuses the kind.
	for wid, want := range map[string]string{old: "", bare: "refused", ok: "accepted"} {
		acc, ref := e.s.kinds(e.s.st.Workers[wid])
		got := ""
		if contains(acc, KindAlign) {
			got = "accepted"
		}
		for _, r := range ref {
			if r.Kind == KindAlign {
				got = "refused"
			}
		}
		if got != want {
			t.Errorf("%s: %q, want %q", e.s.st.Workers[wid].Name, got, want)
		}
	}
	if w := e.s.waiting(j, e.clock.Now()); w != nil {
		t.Fatalf("a job a worker may run waits: %+v", w)
	}
}

func TestAlignIsChecked(t *testing.T) {
	e := newEnv(t, "")
	for _, bad := range []string{
		`{"model_sha256":"nope","language":"de"}`,
		`{"model_sha256":"` + aligner.SHA256 + `","language":"ja"}`,
		`{"model_sha256":"` + aligner.SHA256 + `","language":"de","text":"words"}`,
		`{"model_sha256":"` + aligner.SHA256 + `","language":"de","pieces":3}`,
	} {
		if err := e.alignJob("x.align", bad, "wav"); err == nil {
			t.Errorf("took %s", bad)
		}
	}
	big := FileIn{Path: "x/words.json", Bytes: aligner.MaxWordsBytes + 1, SHA256: strings.Repeat("0", 64)}
	_, _, err := e.s.Submit("y.align", JobSubmit{Kind: KindAlign, Priority: PriorityNow, Params: json.RawMessage(alignParams),
		Source: SourceIn{FileIn: e.source("y/s.wav", "RIFF"), Container: "wav"}, Inputs: map[string]FileIn{"text": big},
		Result: ResultSpec{Name: "align.json", MaxBytes: 1 << 24}})
	if err == nil || !strings.Contains(err.Error(), "more than 8388608") {
		t.Fatalf("a words file of more than 8 MiB: %v", err)
	}
	if err := e.alignJob("z.align", alignParams, "mp4"); err != nil {
		t.Fatal(err)
	}
}

// With a reference worker, a source that is not WAV needs the reference's ffmpeg, as whisper's,
// and nothing else of its tools.
func TestAlignRequiresTheReferencesFFmpegForMedia(t *testing.T) {
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", []Tool{{"ffmpeg", "9.0.2"}, {"whisper.cpp", "v1.9.5"}})
	e.aligns(enc, aligner.EngineTorch)
	e.aligns(mac, aligner.EngineMLX)
	if err := e.alignJob("a.align", alignParams, "mp4"); err != nil {
		t.Fatal(err)
	}
	if err := e.alignJob("b.align", alignParams, "wav"); err != nil {
		t.Fatal(err)
	}
	req, _ := e.s.requires(e.s.st.Jobs["a.align"])
	if len(req.Tools) != 1 || req.Tools[0] != reference[0] {
		t.Fatalf("a media source requires %+v", req.Tools)
	}
	if why := e.s.mismatch(e.s.st.Workers[mac], e.s.st.Jobs["a.align"]); !strings.HasPrefix(why, "ffmpeg differs") {
		t.Fatalf("the Mac's other ffmpeg: %q", why)
	}
	if why := e.s.mismatch(e.s.st.Workers[mac], e.s.st.Jobs["b.align"]); why != "" {
		t.Fatalf("a WAV source on the Mac: %q", why)
	}
}

// On encoder (10 GB for its worker) a 4K `slower` archive encode runs in the normal slot. The
// CPU aligner's 3 GiB (lean) fit beside it in the high-priority slot, so the timing of a Socials
// version takes no archive lease back.
func TestAlignOnTheCPUFitsBesideA4KEncode(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	e.machine(enc, 10*gib, 1*gib)
	e.aligns(enc, aligner.EngineTorch)
	e.limits(enc, 1, 1)
	e.rung("old.2160-archive", "archive")
	if e.claim(enc) == nil {
		t.Fatal("no archive encode")
	}
	e.clock.Add(time.Second)
	used := 53 * gib / 10
	e.s.PutPresence(enc, Presence{State: "working", MemoryUsed: &used})
	if err := e.alignJob("k3v9x2m1q7pz.align", alignParams, "mp4"); err != nil {
		t.Fatal(err)
	}
	if x, err := e.high(enc); err != nil || x == nil || x.Job.ID != "k3v9x2m1q7pz.align" {
		t.Fatalf("the align job beside the encode: %+v %v", x, err)
	}
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("the align job took something back")
	}
	w := e.s.st.Workers[enc]
	j := e.s.st.Jobs["k3v9x2m1q7pz.align"]
	if peakOn(w, j) != 3*gib || peakBytes(j) != 7*gib/2 {
		t.Fatalf("peaks %d %d", peakOn(w, j), peakBytes(j))
	}
	e.aligns(enc, aligner.EngineMLX) // a worker with both runs MLX
	if peakOn(w, j) != 7*gib/2 {
		t.Fatalf("MLX's peak %d", peakOn(w, j))
	}
}

func TestAlignPointsAndSpeed(t *testing.T) {
	e := newEnv(t, "")
	mac := e.worker("mac", reference)
	enc := e.worker("encoder", reference)
	e.aligns(mac, aligner.EngineMLX)
	e.aligns(enc, aligner.EngineTorch)
	if err := e.alignJob("a.align", alignParams, "wav"); err != nil {
		t.Fatal(err)
	}
	j := e.s.st.Jobs["a.align"]
	j.Spec.Source.DurationS = 3600
	if got := jobPoints(j, &Lease{}); math.Abs(got-0.66) > 1e-9 {
		t.Fatalf("an hour earns %v", got)
	}
	if run, ok := e.s.runSeconds(e.s.st.Workers[mac], j); !ok || math.Abs(run-(1.5+40)) > 1e-9 {
		t.Fatalf("the Mac: %v %v", run, ok)
	}
	if run, ok := e.s.runSeconds(e.s.st.Workers[enc], j); !ok || math.Abs(run-(8+3600/6.7)) > 1e-9 {
		t.Fatalf("encoder: %v %v", run, ok)
	}
	// One accepted result of the Mac's earns it its points.
	l, err := e.s.Claim(context.Background(), mac, LeaseRequest{}, 0)
	if err != nil || l == nil {
		t.Fatalf("%+v %v", l, err)
	}
}

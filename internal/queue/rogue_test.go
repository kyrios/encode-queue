package queue

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// **A rogue worker cannot hold everything** (v0.2.9; security review M2, 2026-10-27). The
// review's rogue set max_parallel_jobs to 100000 through PATCH /me, reported a calibrated
// 1e12 fps, and renewed every lease every 30 s: it held all 20 jobs for an hour and the
// reference worker got nothing. Each of the three rules is tested on its own here, then the
// review's attack whole.

// frames submits a 4K rung job of 3000 frames, so its expected completion time is known.
func (e *env) frames(id, prio string) {
	e.t.Helper()
	f := e.source(strings.Split(id, ".")[0]+"/master.mov", "master of "+id)
	params := `{"rung":"2160","codec":"libx265","preset":"slower","crf":29,"width":3840,"height":2160,"vbv_maxrate_kbps":5000,"audio":{"mode":"copy"}}`
	if _, _, err := e.s.Submit(id, JobSubmit{Kind: KindRung, Priority: prio, Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, Width: 3840, Height: 2160, Frames: 3000}, Result: ResultSpec{Name: "2160.mp4", MaxBytes: 1 << 30}}); err != nil {
		e.t.Fatal(err)
	}
}

// rogue pairs the review's worker: as many slots as it likes, 1e12 fps on the calibration clip.
func (e *env) rogue() string {
	e.t.Helper()
	id := e.worker("rogue", reference)
	e.calibrated(id, clipA, map[string]float64{k4slower: 1e12})
	if _, err := e.s.PatchMe(id, WorkerPatch{Limits: &Limits{MaxParallelJobs: 100000}}); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func TestAWorkersOwnLimitIsCappedByTheQueue(t *testing.T) {
	e := newEnv(t, "encoder")
	e.worker("encoder", reference)
	r := e.rogue()
	for i := 0; i < 5; i++ {
		e.frames(fmt.Sprintf("j%02d.archive", i), "archive")
	}
	held := 0
	for i := 0; i < 5; i++ {
		l, err := e.s.Claim(t.Context(), r, LeaseRequest{}, 0)
		if err != nil {
			if code(err) != "limit_reached" {
				t.Fatalf("claim %d: %v", i, err)
			}
			break
		}
		if l != nil {
			held++
		}
	}
	if held != 2 {
		t.Fatalf("the rogue holds %d leases, want the queue's cap of 2", held)
	}
	if n := e.s.maxJobs(e.s.st.Workers[r]); n != 2 {
		t.Fatalf("maxJobs %d", n)
	}
}

func TestACalibratedSpeedCountsAtMostFourTimesTheReference(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	e.calibrated(ref, clipA, map[string]float64{k4slower: 2.5})
	r := e.rogue()
	if rel := e.s.calibratedRelative(e.s.st.Workers[r], k4slower); rel == nil || *rel != 4 {
		t.Fatalf("relative %v, want 4", rel)
	}
	if f := e.s.fpsOn(e.s.st.Workers[r], k4slower); f != 10 {
		t.Fatalf("fps %v, want 4 × 2.5", f)
	}
	// An honest Mac at 2.4× keeps its calibration (ect_test.go: the Mac Studio gets the
	// compressed source).
	mac := e.worker("mac", reference)
	e.calibrated(mac, clipA, map[string]float64{k4slower: 6})
	if f := e.s.fpsOn(e.s.st.Workers[mac], k4slower); f != 6 {
		t.Fatalf("the Mac's fps %v, want its own 6", f)
	}
}

func TestAMeasuredSpeedIsNoFasterThanTheLeaseWasHeld(t *testing.T) {
	e := newEnv(t, "encoder")
	e.worker("encoder", reference)
	r := e.rogue()
	e.rung("m1.archive", "archive")
	l := e.claim(r)
	e.clock.Add(100 * time.Second)
	// It says 1000 frames in a millisecond; the queue saw it hold the lease for 100 s.
	e.result(r, l.ID, 1000, 0.001)
	h := e.s.st.Workers[r].speed(k4slower)
	if h == nil || len(h.FPS) != 1 || math.Abs(h.FPS[0]-10) > 1e-9 {
		t.Fatalf("measured %+v, want 1000 frames / 100 s", h)
	}
}

func TestALeaseHeldPastItsHoldTimeGoesBack(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	e.calibrated(ref, clipA, map[string]float64{k4slower: 2.5})
	r := e.rogue()
	e.frames("j01.archive", "archive")
	l := e.claim(r)
	if l == nil {
		t.Fatal("no lease")
	}
	lease := e.s.st.Leases[l.ID]
	// 3000 frames at the capped 10 fps is 300 s of work; three times that is below HoldMin.
	if got := lease.HoldUntil.Sub(lease.CreatedAt); got != 30*time.Minute {
		t.Fatalf("hold %v, want HoldMin", got)
	}
	var err error
	for i := 0; i < 61 && err == nil; i++ {
		e.clock.Add(30 * time.Second)
		e.presence(r, ref)
		_, err = e.s.Renew(r, l.ID, Progress{Phase: "encoding"})
	}
	if code(err) != "overdue" {
		t.Fatalf("renewal after 30 min: %v", err)
	}
	if !contains(e.s.st.Jobs["j01.archive"].Excluded, r) || e.s.st.Jobs["j01.archive"].Failures != 0 {
		t.Fatalf("job after the overdue lease: %+v", e.s.st.Jobs["j01.archive"])
	}
	if got := e.claim(ref); got == nil || got.Job.ID != "j01.archive" {
		t.Fatalf("the reference got %+v", got)
	}
	// A lease from before v0.2.9 has no hold time and gets HoldMax from its start.
	old := &Lease{CreatedAt: e.clock.Now()}
	if e.s.overdue(old, e.clock.Now().Add(12*time.Hour)) || !e.s.overdue(old, e.clock.Now().Add(12*time.Hour+time.Second)) {
		t.Fatal("HoldMax for an old lease")
	}
}

// The review's attack, whole: 20 jobs, the rogue claims and renews for an hour of simulated
// time, the reference worker asks for work all along.
func TestARogueWorkerCannotStallTheQueue(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	e.calibrated(ref, clipA, map[string]float64{k4slower: 2.5})
	r := e.rogue()
	for i := 0; i < 20; i++ {
		e.frames(fmt.Sprintf("j%02d.stage1", i), "stage1")
	}
	mine := map[string]bool{}
	refGot := 0
	for step := 0; step < 120; step++ { // an hour, every 30 s
		for {
			l, err := e.s.Claim(t.Context(), r, LeaseRequest{}, 0)
			if err != nil || l == nil {
				break
			}
			mine[l.ID] = true
		}
		for id := range mine {
			if _, err := e.s.Renew(r, id, Progress{Phase: "encoding"}); err != nil {
				delete(mine, id)
			}
		}
		if len(mine) > 2 {
			t.Fatalf("step %d: the rogue holds %d leases", step, len(mine))
		}
		if l, err := e.s.Claim(t.Context(), ref, LeaseRequest{}, 0); err == nil && l != nil {
			refGot++
			e.result(ref, l.ID, 3000, 300)
		}
		e.clock.Add(30 * time.Second)
		e.presence(r, ref)
		e.s.Tick()
	}
	if refGot < 10 {
		t.Fatalf("the reference worker got %d of 20 jobs in an hour", refGot)
	}
}

package queue

import (
	"encoding/json"
	"testing"
	"time"
)

// mac pairs a calibrated Mac with 12 threads beside the reference worker, as the Mac Studio.
func (e *env) mac() (mac, ref string) {
	e.t.Helper()
	ref = e.worker("encoder", reference)
	e.calibrated(ref, clipA, map[string]float64{k4slower: 2.5})
	mac = e.worker("mac", reference)
	cal := &Calibration{ClipSHA256: clipA, Tools: reference[:3], At: e.clock.Now(),
		Classes: []CalibratedClass{{Class: k4slower, Frames: 100, FPS: 2.5}}}
	if _, err := e.s.PutCapabilities(mac, Capabilities{AppVersion: "0.2.17", OS: "test", Tools: reference,
		Models: []Model{{"ggml-medium.bin", modelSHA}}, Calibration: cal, Hardware: json.RawMessage(`{"cpu_threads":12}`)}); err != nil {
		e.t.Fatal(err)
	}
	e.presence(mac, ref)
	return mac, ref
}

// pause pauses w for d, as Slipstream's worker tells the queue.
func (e *env) pause(w string, d time.Duration) {
	e.t.Helper()
	until := ptrTime(e.clock.Now().Add(d))
	if _, err := e.s.PatchMe(w, WorkerPatch{PausedUntil: &until}); err != nil {
		e.t.Fatal(err)
	}
}

// pausedFor renews lease l in phase every 30 s for d, paused since since, with presence, and
// ticks; the error of the first renewal that fails.
func (e *env) pausedFor(w, l string, since time.Time, d time.Duration, phase string, others ...string) error {
	for t := time.Duration(0); t < d; t += 30 * time.Second {
		e.clock.Add(30 * time.Second)
		e.presence(append([]string{w}, others...)...)
		p := Progress{Phase: phase}
		if phase == "paused" {
			p.PausedSince = &since
		}
		if _, err := e.s.Renew(w, l, p); err != nil {
			return err
		}
		e.s.Tick()
	}
	return nil
}

// Pause (v0.2.18; Thorsten, 2026-10-28): a suspended archive or stage 2 lease stays through the
// pause, its hold clock stopped, up to PauseHoldMax; a stage 1 lease goes back after the give-back
// time as before; a lease whose tool runs on (a pause after the job) is left alone.
func TestAPauseHoldsAnArchiveLease(t *testing.T) {
	e := newEnv(t, "encoder")
	mac, ref := e.mac()
	e.frames("j01.archive", "archive")
	l := e.claim(mac)
	if l == nil || l.Job.ID != "j01.archive" || !l.PauseHold {
		t.Fatalf("the archive lease: %+v", l)
	}
	if _, err := e.s.Renew(mac, l.ID, Progress{Phase: "encoding"}); err != nil {
		t.Fatal(err)
	}
	hold := e.s.st.Leases[l.ID].HoldUntil
	e.pause(mac, 3*time.Hour)
	since := e.clock.Now().UTC()
	if err := e.pausedFor(mac, l.ID, since, 2*time.Hour, "paused", ref); err != nil {
		t.Fatalf("two hours suspended: %v", err)
	}
	ls := e.s.st.Leases[l.ID]
	if ls.State != LeaseActive {
		t.Fatalf("the held lease after two hours: %s %s", ls.State, ls.EndCode)
	}
	// The first paused renewal follows an encoding one: two hours less 30 s stood still.
	if got := ls.HoldUntil.Sub(hold); got != 2*time.Hour-30*time.Second {
		t.Fatalf("the hold moved on by %v, want the paused time", got)
	}
	// The worker claims nothing while paused.
	e.frames("j02.archive", "archive")
	if _, err := e.s.Claim(t.Context(), mac, LeaseRequest{}, 0); code(err) != "paused" {
		t.Fatalf("a paused worker's claim: %v", err)
	}
	// Past PauseHoldMax it goes back after all.
	e.pause(mac, 30*time.Hour)
	err := e.pausedFor(mac, l.ID, since, 23*time.Hour, "paused", ref)
	if ls := e.s.st.Leases[l.ID]; err == nil || ls.State != LeaseGivenBack || ls.EndCode != "paused" {
		t.Fatalf("after 25 h paused: %v %s %s", err, ls.State, ls.EndCode)
	}
}

func TestAPauseGivesAStage1LeaseBack(t *testing.T) {
	e := newEnv(t, "encoder")
	mac, ref := e.mac()
	e.frames("j01.stage1", "stage1")
	l := e.claim(mac)
	if l == nil || l.PauseHold {
		t.Fatalf("the stage 1 lease: %+v", l)
	}
	e.pause(mac, time.Hour)
	err := e.pausedFor(mac, l.ID, e.clock.Now().UTC(), 5*time.Minute, "paused", ref)
	if ls := e.s.st.Leases[l.ID]; err == nil || ls.State != LeaseGivenBack || ls.EndCode != "paused" {
		t.Fatalf("a stage 1 lease suspended for 5 min: %v %s %s", err, ls.State, ls.EndCode)
	}
}

func TestAPauseAfterTheJobLeavesItsLeaseAlone(t *testing.T) {
	e := newEnv(t, "encoder")
	mac, ref := e.mac()
	e.frames("j01.stage1", "stage1")
	l := e.claim(mac)
	e.pause(mac, time.Hour)
	if err := e.pausedFor(mac, l.ID, e.clock.Now().UTC(), 10*time.Minute, "encoding", ref); err != nil {
		t.Fatalf("a job that runs on through the pause: %v", err)
	}
	if ls := e.s.st.Leases[l.ID]; ls.State != LeaseActive {
		t.Fatalf("the lease: %s %s", ls.State, ls.EndCode)
	}
}

// A slower limit lengthens the holds (v0.2.18): a running tool held to 3 of 12 threads, and the
// efficiency cores at a tenth of the speed for archive jobs, which the ECT counts too.
func TestASlowerLimitLengthensTheHold(t *testing.T) {
	e := newEnv(t, "encoder")
	mac, _ := e.mac()
	twelve, three := 12, 3
	if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &Limits{CPUThreads: &twelve, MaxParallelJobs: 1}}); err != nil {
		t.Fatal(err)
	}
	e.frames("j01.archive", "archive")
	l := e.claim(mac)
	now := e.clock.Now()
	rest := e.s.st.Leases[l.ID].HoldUntil.Sub(now)
	if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &Limits{CPUThreads: &three, MaxParallelJobs: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := e.s.st.Leases[l.ID].HoldUntil.Sub(now); got != 4*rest {
		t.Fatalf("3 of 12 threads: hold %v, want 4 × %v", got, rest)
	}
	// Back to 12: never shorter.
	if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &Limits{CPUThreads: &twelve, MaxParallelJobs: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := e.s.st.Leases[l.ID].HoldUntil.Sub(now); got != 4*rest {
		t.Fatalf("12 again: hold %v", got)
	}
	tenth := 0.1
	if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &Limits{CPUThreads: &twelve, MaxParallelJobs: 1,
		Efficiency: EfficiencyArchive, EfficiencySpeed: &tenth}}); err != nil {
		t.Fatal(err)
	}
	if got := e.s.st.Leases[l.ID].HoldUntil.Sub(now); got != 40*rest {
		t.Fatalf("archive jobs on the efficiency cores: hold %v, want 40 × %v", got, rest)
	}
	for _, bad := range []Limits{{Efficiency: "sometimes"}, {Efficiency: EfficiencyAll, EfficiencySpeed: ptrFloat(0)}, {EfficiencySpeed: ptrFloat(2)}} {
		if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &bad}); code(err) != "invalid" {
			t.Errorf("limits %+v: %v", bad, err)
		}
	}
}

func TestTheEfficiencyCoresSlowTheECT(t *testing.T) {
	e := newEnv(t, "encoder")
	mac, _ := e.mac()
	e.frames("j01.archive", "archive")
	e.frames("j02.stage2", "stage2")
	ect := func(id string) float64 {
		e.s.mu.Lock()
		defer e.s.mu.Unlock()
		v, ok := e.s.ect(e.s.st.Workers[mac], e.s.st.Jobs[id], true, e.clock.Now())
		if !ok {
			t.Fatalf("no ECT for %s", id)
		}
		return v.RunS
	}
	archive, stage2 := ect("j01.archive"), ect("j02.stage2")
	tenth := 0.1
	if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &Limits{MaxParallelJobs: 1, Efficiency: EfficiencyArchive, EfficiencySpeed: &tenth}}); err != nil {
		t.Fatal(err)
	}
	if a, s := ect("j01.archive"), ect("j02.stage2"); a < 9.99*archive || a > 10.01*archive || s != stage2 {
		t.Fatalf("archive on the efficiency cores: archive %.0f s (was %.0f), stage 2 %.0f s (was %.0f)", a, archive, s, stage2)
	}
	if _, err := e.s.PatchMe(mac, WorkerPatch{Limits: &Limits{MaxParallelJobs: 1, Efficiency: EfficiencyAll, EfficiencySpeed: &tenth}}); err != nil {
		t.Fatal(err)
	}
	if s := ect("j02.stage2"); s < 9.99*stage2 || s > 10.01*stage2 {
		t.Fatalf("every job on the efficiency cores: stage 2 %.0f s, was %.0f", s, stage2)
	}
}

func ptrFloat(f float64) *float64 { return &f }

package queue

import (
	"testing"
	"time"
)

func (e *env) killed(wid, lid string) {
	e.t.Helper()
	if _, err := e.s.Deliver(wid, lid, ResultIn{Outcome: "failed", Code: "killed", Message: "ffmpeg: killed by SIGKILL", Signal: "SIGKILL",
		MemoryAvail: i64(300 << 20)}); err != nil {
		e.t.Fatal(err)
	}
}

// Test 6 on encoder: the OOM killer killed the only worker's ffmpeg.
func TestKilledToolIsRetriedOnTheSameWorker(t *testing.T) {
	e := newEnv(t, "encoder")
	w := e.worker("encoder", reference)
	e.rung("a.stage1", "stage1")
	l := e.claim(w)
	e.killed(w, l.ID)
	v := must(e.s.GetJob("a.stage1"))
	if v.State != StateQueued || v.Failures != 1 || len(v.ExcludedWorkers) != 0 || v.Waiting != nil {
		t.Fatalf("after one kill: %s failures %d excluded %+v waiting %+v", v.State, v.Failures, v.ExcludedWorkers, v.Waiting)
	}
	if r := v.Attempts[0].Result; r.FailureCode == nil || *r.FailureCode != "killed" || r.Signal == nil || *r.Signal != "SIGKILL" ||
		r.MemoryAvail == nil || *r.MemoryAvail != 300<<20 {
		t.Fatalf("the attempt's note %+v", r)
	}
	l = e.claim(w)
	if l == nil || l.Job.ID != "a.stage1" || l.Attempt != 2 {
		t.Fatalf("the job did not run again: %+v", l)
	}
	// The second kill on the same worker excludes it; with no other worker the job says so.
	e.killed(w, l.ID)
	v = must(e.s.GetJob("a.stage1"))
	if v.Waiting == nil || v.Waiting.Code != WaitAllExcluded || v.Waiting.RetryAt == nil ||
		!v.Waiting.RetryAt.Equal(e.clock.Now().Add(10*time.Minute)) {
		t.Fatalf("waiting %+v", v.Waiting)
	}
	if l := e.claim(w); l != nil {
		t.Fatalf("an excluded worker got the job: %+v", l)
	}
	// After the backoff the reference worker gets it again; the third failure fails the job.
	e.clock.Add(10*time.Minute + time.Second)
	e.presence(w)
	e.s.Tick()
	if v := must(e.s.GetJob("a.stage1")); v.Waiting != nil || len(v.ExcludedWorkers) != 0 {
		t.Fatalf("not let back: %+v %+v", v.Waiting, v.ExcludedWorkers)
	}
	l = e.claim(w)
	e.killed(w, l.ID)
	if st := e.state("a.stage1"); st != StateFailed {
		t.Fatalf("after three failures the job is %s", st)
	}
}

func TestWaitingSaysWhy(t *testing.T) {
	e := newEnv(t, "")
	a := e.worker("a", reference)
	b := e.worker("b", reference)
	e.rung("j.stage1", "stage1")
	if w := must(e.s.GetJob("j.stage1")).Waiting; w != nil {
		t.Fatalf("a job workers may take waits: %+v", w)
	}
	// Both results rejected: every worker excluded, and without a reference all come back.
	for _, wid := range []string{a, b} {
		l := e.claim(wid)
		if err := e.deliver(wid, l.ID, "bad "+wid); err != nil {
			t.Fatal(err)
		}
		if _, err := e.s.PutVerdict("j.stage1", l.Attempt, Verdict{Verdict: VerdictRejected, Reason: "ssim"}); err != nil {
			t.Fatal(err)
		}
	}
	w := must(e.s.GetJob("j.stage1")).Waiting
	if w == nil || w.Code != WaitAllExcluded || w.RetryAt == nil {
		t.Fatalf("every worker excluded: %+v", w)
	}
	// Offline: the job waits for a worker to come back.
	e.rung("k.stage1", "stage1")
	e.clock.Add(5 * time.Minute)
	if w := must(e.s.GetJob("k.stage1")).Waiting; w == nil || w.Code != WaitNoWorkerUp {
		t.Fatalf("offline workers: %+v", w)
	}
	// No worker runs the reference's tools.
	e2 := newEnv(t, "encoder")
	e2.worker("encoder", reference)
	odd := e2.worker("odd", []Tool{{"ffmpeg", "8.0"}, {"libx265", "4.0"}, {"libx264", "core 1"}})
	e2.s.RemoveWorker(must(e2.s.GetWorker(odd)).ID)
	e2.s.RemoveWorker(e2.s.referenceWorker().ID)
	e2.rung("m.stage1", "stage1")
	if w := must(e2.s.GetJob("m.stage1")).Waiting; w == nil || w.Code != WaitNoEligibleWorker {
		t.Fatalf("no worker at all: %+v", w)
	}
}

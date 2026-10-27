package queue

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupRemovesFinishedJobs(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("done.archive", "archive")
	l := e.claim(w)
	if err := e.deliver(w, l.ID, "file"); err != nil {
		t.Fatal(err)
	}
	e.s.PutVerdict("done.archive", 1, Verdict{Verdict: VerdictAccepted})
	resultDir := filepath.Join(e.s.cfg.ResultDir, "done.archive")
	if _, err := os.Stat(resultDir); err != nil {
		t.Fatalf("no result directory: %v", err)
	}
	e.rung("c1.archive", "archive")
	e.s.CancelJob("c1.archive")
	e.clock.Add(time.Hour)
	e.rung("c2.archive", "archive")
	e.s.CancelJob("c2.archive")
	e.rung("q.archive", "archive")
	since := e.s.ListJobs(JobFilter{}).Revision

	// Only finished states.
	for _, st := range []string{"queued", "leased", "checking", ""} {
		if _, err := e.s.CleanupJobs(Cleanup{States: []string{st}}); code(err) != "invalid" {
			t.Fatalf("state %q: %v", st, err)
		}
	}
	if _, err := e.s.CleanupJobs(Cleanup{}); code(err) != "invalid" {
		t.Fatalf("no states: %v", err)
	}
	// older_than_s: only c1 finished an hour ago.
	r := must(e.s.CleanupJobs(Cleanup{States: []string{"cancelled"}, OlderThanS: 1800}))
	if r.Removed != 1 || r.JobIDs[0] != "c1.archive" {
		t.Fatalf("older than: %+v", r)
	}
	r = must(e.s.CleanupJobs(Cleanup{States: []string{"cancelled", "done"}}))
	if r.Removed != 2 || r.JobIDs[0] != "c2.archive" || r.JobIDs[1] != "done.archive" {
		t.Fatalf("cleanup: %+v", r)
	}
	if _, err := os.Stat(resultDir); !os.IsNotExist(err) {
		t.Fatalf("the result files stayed: %v", err)
	}
	// The counts are right at once, and the queued job is untouched.
	list := e.s.ListJobs(JobFilter{})
	if len(list.Items) != 1 || list.Items[0].ID != "q.archive" || list.Counts.ByState[StateCancelled] != 0 ||
		list.Counts.ByState[StateDone] != 0 || list.Counts.ByState[StateQueued] != 1 {
		t.Fatalf("after cleanup: %d items, counts %+v", len(list.Items), list.Counts)
	}
	if st := e.s.Status("test"); st.Counts.ByState[StateCancelled] != 0 {
		t.Fatalf("status counts %+v", st.Counts)
	}
	// The pipeline sees them as forgotten.
	ch := e.s.WaitChanges(context.Background(), since, 100, 0)
	forgotten := map[string]bool{}
	for _, it := range ch.Items {
		if f, ok := it.(ForgottenView); ok {
			forgotten[f.ID] = true
		}
	}
	if !forgotten["c1.archive"] || !forgotten["c2.archive"] || !forgotten["done.archive"] || ch.Revision != r.Revision {
		t.Fatalf("changes %+v, revision %d vs %d", forgotten, ch.Revision, r.Revision)
	}
	// Nothing left to remove: 0, not an error.
	if r := must(e.s.CleanupJobs(Cleanup{States: []string{"failed"}})); r.Removed != 0 || r.JobIDs == nil {
		t.Fatalf("nothing: %+v", r)
	}
}

func TestRetentionRemovesEveryFinishedState(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("c.archive", "archive")
	e.s.CancelJob("c.archive")
	e.rung("f.archive", "archive")
	e.s.st.Jobs["f.archive"].Final = StateFailed // as countFailure leaves it
	e.s.st.Jobs["f.archive"].FinishedAt = ptrTime(e.clock.Now())
	e.rung("q.archive", "archive")
	e.clock.Add(7*24*time.Hour - time.Minute)
	e.presence(w)
	e.s.Tick()
	if n := len(e.s.ListJobs(JobFilter{}).Items); n != 3 {
		t.Fatalf("removed before the retention: %d left", n)
	}
	e.clock.Add(2 * time.Minute)
	e.s.Tick()
	list := e.s.ListJobs(JobFilter{})
	if len(list.Items) != 1 || list.Items[0].ID != "q.archive" || list.Counts.ByState[StateCancelled] != 0 || list.Counts.ByState[StateFailed] != 0 {
		t.Fatalf("after the retention: %+v", list.Counts)
	}
}

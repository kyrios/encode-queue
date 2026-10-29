package queue

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A result does not wait for its verdict forever (v0.2.26; 2026-10-29: three strip jobs sat in
// checking from 23:16 on, after the pipeline restarted). After EQ_CHECK_TIMEOUT the queue cancels
// the job with the code check_timeout, its result file goes, and the job may be submitted again.
func TestAResultWithNoVerdictExpires(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.rung("a.2160-stage1", "stage1")
	l := e.claim(w)
	e.result(w, l.ID, 100, 10)
	path := filepath.Join(e.s.cfg.ResultDir, e.s.st.Leases[l.ID].Result.Path)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the result file: %v", err)
	}
	e.clock.Add(e.s.cfg.CheckTimeout - time.Minute)
	e.s.Tick()
	if v, _ := e.s.GetJob("a.2160-stage1"); v.State != StateChecking {
		t.Fatalf("before the timeout: %s", v.State)
	}
	e.clock.Add(2 * time.Minute)
	e.s.Tick()
	v, _ := e.s.GetJob("a.2160-stage1")
	if v.State != StateCancelled || v.Cancelled == nil || v.Cancelled.Code != CancelCheckTimeout || v.Cancelled.By != "queue" ||
		v.Cancelled.Reason == nil || !strings.Contains(*v.Cancelled.Reason, "no verdict") {
		t.Fatalf("after the timeout: %s %+v", v.State, v.Cancelled)
	}
	// Its file went with it, and its room is free.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the result file after the timeout: %v", err)
	}
	if got := e.s.st.Leases[l.ID].Result.Verdict; got != VerdictSuperseded {
		t.Fatalf("the attempt's verdict: %s", got)
	}
	var b bytes.Buffer
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), "encode_queue_check_timeouts_total 1\n") {
		t.Errorf("the metric:\n%s", b.String())
	}
	// A verdict that comes late finds nothing to judge.
	if _, err := e.s.PutVerdict("a.2160-stage1", 1, Verdict{Verdict: VerdictAccepted}); code(err) != "not_checking" {
		t.Fatalf("a late verdict: %v", err)
	}
	// The submitter may send it again.
	if v := e.rung("a.2160-stage1", "stage1"); v.State != StateQueued {
		t.Fatalf("submitted again: %s", v.State)
	}
	// It survives a restart, as a counter must.
	e.reopen()
	b.Reset()
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), "encode_queue_check_timeouts_total 1\n") {
		t.Errorf("the metric after a restart:\n%s", b.String())
	}
}

// EQ_CHECK_TIMEOUT sets the wait, from the last result delivered; 2 hours without it.
func TestTheCheckTimeoutIsTheConfigured(t *testing.T) {
	var c Config
	c.Defaults()
	if c.CheckTimeout != 2*time.Hour {
		t.Fatalf("the default: %s", c.CheckTimeout)
	}
	e := newEnv(t, "")
	e.s.cfg.CheckTimeout = 10 * time.Minute
	w := e.worker("mac", reference)
	e.rung("b.1080-stage2", "stage2")
	l := e.claim(w)
	e.result(w, l.ID, 100, 10)
	e.clock.Add(11 * time.Minute)
	e.s.Tick()
	if v, _ := e.s.GetJob("b.1080-stage2"); v.State != StateCancelled {
		t.Fatalf("after 11 minutes: %s", v.State)
	}
}

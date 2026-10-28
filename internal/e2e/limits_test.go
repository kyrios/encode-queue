package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/queue"
	"github.com/kyrios/encode-queue/internal/worker"
)

// A limit Slipstream lowers while a job runs (v0.2.17; Thorsten, 2026-10-28): the queue hears it
// at once, not with the next presence, the next job starts with it, and the worker says the limit
// in effect to the app. The running fake tool sleeps, so the throttle leaves it free.
func TestALoweredLimitReachesTheQueueAndTheNextJob(t *testing.T) {
	// A presence every 20 s, so only SetLimits' own patch can bring the limit within seconds.
	h := newHarness(t, func(c *queue.Config) { c.PresenceEvery = 20 * time.Second })
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "200")
	t.Setenv("FAKE_FRAME_MS", "10")
	enc := h.pairWith("mac", func(c *worker.Config) { c.CPUThreads = 12 })
	inUse := func() int {
		var v queue.WorkerView
		h.call("GET", "/v1/workers/"+enc.id, adminToken, nil, &v)
		if v.System.ThreadsInUse == nil {
			return -1
		}
		return *v.System.ThreadsInUse
	}
	h.waitFor("the worker's limit", 30*time.Second, func() bool { return inUse() > 1 })
	h.rungJob("one_1.2160-archive", "archive", nil)
	h.waitFor("one_1.2160-archive encoding", 30*time.Second, func() bool {
		p := h.job("one_1.2160-archive").Progress
		return p != nil && p.Phase == "encoding"
	})
	enc.w.SetLimits(1, false, 1, nil)
	start := time.Now()
	h.waitFor("the queue to hear the limit", 5*time.Second, func() bool { return inUse() == 1 })
	t.Logf("the queue heard the limit after %v", time.Since(start).Round(time.Millisecond))

	a := h.waitState("one_1.2160-archive", "done", 30*time.Second)
	h.rungJob("two_2.2160-archive", "archive", nil)
	b := h.waitState("two_2.2160-archive", "done", 30*time.Second)
	if got := h.fetch(a, 1); !strings.Contains(got, "pools=12") {
		t.Fatalf("the job started before the change: %q", got[strings.LastIndex(got, "\n"):])
	}
	if got := h.fetch(b, 1); !strings.Contains(got, "pools=1 ") && !strings.HasSuffix(got, "pools=1") {
		t.Fatalf("the job started after the change: %q", got[strings.LastIndex(got, "\n"):])
	}

	h.evMu.Lock()
	defer h.evMu.Unlock()
	var limits []map[string]any
	for _, e := range h.events {
		if e["event"] == "limits" {
			limits = append(limits, e)
		}
	}
	if len(limits) < 2 || limits[len(limits)-1]["threads"] != 1 || limits[len(limits)-1]["throttled"] != 0 {
		t.Fatalf("the limits events: %v", limits)
	}
}

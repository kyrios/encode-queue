package queue

import (
	"strings"
	"testing"
)

// Thorsten, 2026-10-27: the Encoder page's CPU squares colour the busy threads; the worker says
// how many its tools keep busy (threads_busy), the queue passes it on.
func TestThreadsBusy(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.machine(w, 32*gib, 23*gib) // 6 threads
	busy := 5
	e.s.PutPresence(w, Presence{State: "working", ThreadsBusy: &busy})
	if v := must(e.s.GetWorker(w)); v.System.ThreadsBusy == nil || *v.System.ThreadsBusy != 5 {
		t.Fatalf("threads_busy %v", v.System.ThreadsBusy)
	}
	var b strings.Builder
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), `encode_queue_worker_cpu_threads_busy{worker_id="`+w+`",worker="mac"} 5`) {
		t.Fatalf("no busy series:\n%s", b.String())
	}
	// Never more than the machine has; never below 0; unknown when a presence does not say.
	busy = 40
	e.s.PutPresence(w, Presence{State: "working", ThreadsBusy: &busy})
	if v := must(e.s.GetWorker(w)); *v.System.ThreadsBusy != 6 {
		t.Fatalf("capped at %d, want 6", *v.System.ThreadsBusy)
	}
	busy = -1
	if _, err := e.s.PutPresence(w, Presence{State: "working", ThreadsBusy: &busy}); err == nil {
		t.Fatal("took a negative threads_busy")
	}
	e.s.PutPresence(w, Presence{State: "idle"})
	if v := must(e.s.GetWorker(w)); v.System.ThreadsBusy != nil {
		t.Fatalf("kept an old threads_busy: %d", *v.System.ThreadsBusy)
	}
}

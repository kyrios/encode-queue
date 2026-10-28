package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The high-priority slots (v0.2.14): Slipstream's limits line sets them only when it says them,
// the queue hears them in the limits, and a high-priority claim says its slot to both queues.
func TestSetLimitsKeepsTheHighPrioritySlotsUnlessSaid(t *testing.T) {
	w, err := New(Config{StateDir: t.TempDir(), MaxParallel: 1, HighPrioritySlots: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	high := func() (int, any) {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.highLimit, w.limits()["high_priority_slots"]
	}
	w.SetLimits(4, true, 2, nil) // Slipstream before the setting
	if n, l := high(); n != 1 || l != 1 {
		t.Fatalf("a limits line without high_slots: %d %v", n, l)
	}
	for _, c := range []struct{ set, want int }{{3, 3}, {0, 0}, {-1, 0}, {100, maxSlots}} {
		v := c.set
		w.SetLimits(4, true, 2, &v)
		if n, l := high(); n != c.want || l != c.want {
			t.Errorf("high_slots %d: %d %v, want %d", c.set, n, l, c.want)
		}
	}
	if w, _ := New(Config{StateDir: t.TempDir(), HighPrioritySlots: -3}, nil); w.highLimit != 0 {
		t.Fatalf("a negative setting gives %d", w.highLimit)
	}
}

func TestAHighPriorityClaimSaysItsSlot(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		rw.WriteHeader(204)
	}))
	defer srv.Close()
	c, err := NewClient(srv.Client(), srv.URL+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, high := range []bool{false, true} {
		if l, err := c.Claim(context.Background(), time.Second, high); err != nil || l != nil {
			t.Fatalf("claim: %+v %v", l, err)
		}
	}
	if len(bodies) != 2 || bodies[0] != "{}" || bodies[1] != `{"slot":"high","extra":true}` {
		t.Fatalf("the claims' bodies: %q", bodies)
	}
}

// The queue's word on this worker's points (v0.2.15) reaches Slipstream once per change.
func TestPointsReachSlipstreamOncePerChange(t *testing.T) {
	var events []map[string]any
	w, err := New(Config{StateDir: t.TempDir(), Events: func(e map[string]any) { events = append(events, e) }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.notePoints(nil) // a queue before 0.2.15
	p := Points{Table: "v1", Today: 1240.5, Week: 3000, Total: 9000, Rank: 1, Of: 3}
	w.notePoints(&p)
	same := p
	w.notePoints(&same)
	p.Rank = 2
	w.notePoints(&p)
	if len(events) != 2 || events[0]["event"] != "points" || events[0]["today"] != 1240.5 || events[1]["rank"] != 2 || events[1]["of"] != 3 {
		t.Fatalf("events: %v", events)
	}
	if st := w.Status(); st.Points == nil || st.Points.Rank != 2 {
		t.Fatalf("the status's points: %+v", st.Points)
	}
}

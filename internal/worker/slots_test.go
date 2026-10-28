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

// Slipstream's "letzte Antwort" follows the answers (v0.2.14): the connection event comes again
// once the last answer's minute changed, not only when the tunnel goes up or down.
func TestTheLastAnswerReachesSlipstreamEachMinute(t *testing.T) {
	events := make(chan map[string]any, 10)
	w, err := New(Config{StateDir: t.TempDir(), Events: func(e map[string]any) { events <- e }}, nil)
	if err != nil {
		t.Fatal(err)
	}
	next := func() map[string]any {
		select {
		case e := <-events:
			return e
		case <-time.After(time.Second):
			return nil
		}
	}
	w.contact(nil)
	if e := next(); e == nil || e["event"] != "connection" || e["up"] != true {
		t.Fatalf("the first answer: %v", e)
	}
	// Another answer in the same minute says nothing new.
	w.mu.Lock()
	w.saidContact = *w.lastContact
	w.mu.Unlock()
	before := time.Now().Truncate(time.Minute)
	w.contact(nil)
	if e := next(); e != nil && time.Now().Truncate(time.Minute).Equal(before) {
		t.Fatalf("an answer in the same minute: %v", e)
	}
	// One in a later minute is said again, with its time.
	w.mu.Lock()
	w.saidContact = w.lastContact.Add(-time.Minute)
	w.mu.Unlock()
	w.contact(nil)
	e := next()
	if e == nil || e["event"] != "connection" || e["last_answer_at"] == nil {
		t.Fatalf("an answer a minute later: %v", e)
	}
}

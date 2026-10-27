package queue

import (
	"testing"
	"time"
)

func TestCancelledSaysWhoAndWhy(t *testing.T) {
	e := newEnv(t, "")
	e.rung("a.archive", "archive")
	v := must(e.s.CancelJobBy("a.archive", "thorsten", "neue Datei hochgeladen"))
	if c := v.Cancelled; c == nil || c.By != "thorsten" || c.Reason == nil || *c.Reason != "neue Datei hochgeladen" || !c.At.Equal(e.clock.Now()) {
		t.Fatalf("cancelled %+v", v.Cancelled)
	}
	e.reopen()
	if c := must(e.s.GetJob("a.archive")).Cancelled; c == nil || c.By != "thorsten" {
		t.Fatalf("after a restart %+v", c)
	}
	if v := must(e.s.RetryJob("a.archive")); v.Cancelled != nil {
		t.Fatalf("a retried job keeps %+v", v.Cancelled)
	}
	if v := must(e.s.CancelJob("a.archive")); v.Cancelled == nil || v.Cancelled.By != "admin" || v.Cancelled.Reason != nil {
		t.Fatalf("without who and why %+v", v.Cancelled)
	}
	e.rung("b.archive", "archive")
	if v := must(e.s.GetJob("b.archive")); v.Cancelled != nil {
		t.Fatalf("a job never cancelled %+v", v.Cancelled)
	}
}

func TestTunnelSince(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	pub := e.s.st.Workers[w].PublicKey
	var hs time.Time
	e.s.PeerStatus = func() map[string]PeerStatus {
		if hs.IsZero() {
			return map[string]PeerStatus{}
		}
		return map[string]PeerStatus{pub: {Endpoint: "203.0.113.7:51820", LastHandshake: hs}}
	}
	since := func() *time.Time {
		e.s.Tick()
		return must(e.s.GetWorker(w)).TunnelSince
	}
	if s := since(); s != nil {
		t.Fatalf("before a handshake %v", s)
	}
	up := e.clock.Now()
	hs = up
	if s := since(); s == nil || !s.Equal(up) {
		t.Fatalf("up %v, want %v", s, up)
	}
	// Renewals every 2 minutes keep it.
	for i := 0; i < 3; i++ {
		e.clock.Add(2 * time.Minute)
		e.presence(w)
		hs = e.clock.Now()
		if s := since(); s == nil || !s.Equal(up) {
			t.Fatalf("after a renewal %v, want %v", s, up)
		}
	}
	// Down: no handshake for over 180 s.
	e.clock.Add(4 * time.Minute)
	e.presence(w)
	if s := since(); s != nil {
		t.Fatalf("down, but since %v", s)
	}
	// Up again: the new handshake.
	hs = e.clock.Now()
	if s := since(); s == nil || !s.Equal(hs) {
		t.Fatalf("up again %v, want %v", s, hs)
	}
}

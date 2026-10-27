package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// A worker of v0.2.4 (Slipstream 1.2.0 embeds one) against this queue: capabilities without a
// calibration, presence without threads_busy, claims without "extra". It gets leases, renews and
// delivers as before; the new fields stay empty for it.
func TestAWorkerOfV024(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	// What a v0.2.4 worker sends, as JSON, through the same structs the server decodes into.
	var caps Capabilities
	json.Unmarshal([]byte(`{"app_version":"0.2.4","os":"macOS 26.1","host_app":{"name":"Slipstream","version":"1.2.0"},"tools":`+string(must(json.Marshal(reference)))+`,"models":[{"name":"ggml-medium.bin","sha256":"`+modelSHA+`"}],"hardware":{"cpu_threads":12,"memory_bytes":34359738368}}`), &caps)
	if _, err := e.s.PutCapabilities(mac, caps); err != nil {
		t.Fatal(err)
	}
	var p Presence
	json.Unmarshal([]byte(`{"state":"idle","memory_used_bytes":8589934592}`), &p)
	if _, err := e.s.PutPresence(mac, p); err != nil {
		t.Fatal(err)
	}
	e.encode(ref, "r1.archive", 10)
	e.rung("a.archive", "archive")
	var req LeaseRequest
	json.Unmarshal([]byte(`{}`), &req)
	l, err := e.s.Claim(context.Background(), mac, req, 0)
	if err != nil || l == nil || e.s.st.Leases[l.ID].Extra {
		t.Fatalf("claim: %+v %v", l, err)
	}
	if _, err := e.s.Renew(mac, l.ID, Progress{Phase: "encoding", FPS: f64(3)}); err != nil {
		t.Fatal(err)
	}
	v := must(e.s.GetWorker(mac))
	if v.System.ThreadsBusy != nil || v.RelativeSpeed != nil || v.RelativeSpeedSource != nil || v.SpeedEstimate == nil || v.SpeedEstimate.Value != 0.3 {
		t.Fatalf("the new fields for an old worker: busy %v speed %v source %v estimate %+v", v.System.ThreadsBusy, v.RelativeSpeed, v.RelativeSpeedSource, v.SpeedEstimate)
	}
	// A Sofort job waits for its regular slot: an old worker never asks for the extra one.
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if x, err := e.s.Claim(context.Background(), mac, req, 0); err == nil || x != nil {
		t.Fatalf("a second lease for an old worker: %+v %v", x, err)
	}
	e.result(mac, l.ID, 1000, 50)
	if x := e.claim(mac); x == nil || x.Job.ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("after the encode: %+v", x)
	}
}

// A restart of the queue of under a minute (a new image on encoder) keeps an active lease: the
// worker goes on encoding, its next renewal is taken, nothing is preempted or ended as offline.
// The worst case: the worker's last call 30 s before the stop, 55 s down, its next call 30 s
// after the start, past the 90 s after which a worker counts as offline.
func TestARestartKeepsTheLease(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.rung("a.archive", "archive")
	l := e.claim(w)
	e.s.Renew(w, l.ID, Progress{Phase: "encoding", FPS: f64(3)})
	e.clock.Add(30 * time.Second) // nothing from the worker since its renewal
	e.reopen()
	e.clock.Add(55 * time.Second) // the queue was down
	for i := 0; i < 30; i++ {     // up again; the worker's next call is 30 s away
		e.s.Tick()
		e.clock.Add(time.Second)
	}
	if _, err := e.s.Renew(w, l.ID, Progress{Phase: "encoding", FPS: f64(3)}); err != nil {
		t.Fatalf("the renewal after the restart: %v", err)
	}
	if v := must(e.s.GetJob("a.archive")); v.State != "leased" || len(v.Attempts) != 1 {
		t.Fatalf("the job after the restart: %s %+v", v.State, v.Attempts)
	}
	// A worker that does not come back counts as offline 90 s after the start, as before.
	e.clock.Add(200 * time.Second)
	e.s.Tick()
	if v := must(e.s.GetJob("a.archive")); v.State != "queued" {
		t.Fatalf("a gone worker kept its lease: %s", v.State)
	}
}

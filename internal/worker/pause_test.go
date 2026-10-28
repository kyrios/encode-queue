package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// A pause ends a claim's long poll at once (v0.2.18): the worker takes nothing from that moment,
// not when the queue's 25 s run out.
func TestAPauseEndsTheClaimsLongPoll(t *testing.T) {
	ended := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body) // the server notices a client that leaves only once the body is read
		select {
		case <-r.Context().Done():
			close(ended)
		case <-time.After(10 * time.Second):
			rw.WriteHeader(204)
		}
	}))
	defer srv.Close()
	api, err := NewClient(srv.Client(), srv.URL+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(Config{StateDir: t.TempDir(), LongPoll: 25 * time.Second}, api)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := w.claimUnlessPaused(context.Background(), false)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	w.PauseWith(time.Now().Add(time.Hour), PauseAfterJob)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the claim during the pause: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the long poll went on through the pause")
	}
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("the queue never saw the claim end")
	}
	t.Logf("the claim ended %v after the pause", time.Since(start).Round(time.Millisecond))
	// A limits change wakes the same channel but is no pause: the claim goes on.
	w.Resume()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := w.claimUnlessPaused(ctx, false)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	w.SetLimits(2, false, 1, nil)
	select {
	case err := <-done:
		t.Fatalf("a limits change ended the claim: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	<-done
}

// The queue hears a pause to the second; its echo keeps the pause's mode, and a pause of its own
// (the Encoder page's) stops the tools.
func TestTheQueuesEchoKeepsThePauseMode(t *testing.T) {
	w, err := New(Config{StateDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Hour).Add(123 * time.Millisecond)
	w.PauseWith(until, PauseAfterJob)
	w.mu.Lock()
	w.pausePatch = false // the patch went through
	w.mu.Unlock()
	echo := until.UTC().Truncate(time.Second)
	w.setPause(&echo, true, PauseNow)
	if st := w.Status(); st.PauseMode != PauseAfterJob || w.suspends() || !w.paused() {
		t.Fatalf("after the queue's echo: %+v, suspends %v", st, w.suspends())
	}
	other := echo.Add(time.Hour)
	w.setPause(&other, true, PauseNow)
	if !w.suspends() || w.Status().PauseMode != PauseNow {
		t.Fatalf("the queue's own pause: mode %q", w.Status().PauseMode)
	}
	// "Sofort anhalten" during a pause after the job: the same time, now.
	w.PauseWith(other, PauseAfterJob)
	w.PauseWith(other, PauseNow)
	if !w.suspends() {
		t.Fatal("the switch to now")
	}
	w.Resume()
	if w.paused() || w.Status().PauseMode != "" {
		t.Fatalf("after resume: %+v", w.Status())
	}
}

// Each way to pause, on a fake long tool: after the job it runs on; now it stands still, and goes
// back after the give-back time unless the queue holds the lease; resumed, it runs again.
func TestPauseModesOnARunningTool(t *testing.T) {
	unclamped(t)
	t.Setenv("EQ_BUSY_HELPER", "1")
	for _, c := range []struct {
		name     string
		mode     string
		hold     bool
		running  bool
		givesUp  bool
		resumeOn bool
	}{
		{"after the job", PauseAfterJob, false, true, false, false},
		{"now, held", PauseNow, true, false, false, true},
		{"now, a job someone waits for", PauseNow, false, false, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			w, err := New(Config{StateDir: t.TempDir()}, nil)
			if err != nil {
				t.Fatal(err)
			}
			l := &Lease{ID: "ls_PAUSE", PauseGiveBackS: 1, PauseHold: c.hold}
			r := newRun(w, l, false)
			if err := os.MkdirAll(r.dir, 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			r.mu.Lock()
			r.cancel = cancel
			r.mu.Unlock()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				r.runTool(ctx, []string{os.Args[0], "-test.run=^TestBusyHelper$"}, "ffmpeg", nil)
			}()
			go func() { defer wg.Done(); r.pauseLoop(ctx) }()
			defer func() { cancel(); wg.Wait() }()
			var pid int
			for deadline := time.Now().Add(5 * time.Second); pid == 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				r.mu.Lock()
				if r.cmd != nil && r.cmd.Process != nil {
					pid = r.cmd.Process.Pid
				}
				r.mu.Unlock()
			}
			if pid == 0 {
				t.Fatal("the fake tool did not start")
			}
			cores := func(d time.Duration) float64 {
				c0, _ := groupCPU(pid)
				time.Sleep(d)
				c1, _ := groupCPU(pid)
				return (c1 - c0).Seconds() / d.Seconds()
			}
			stopped := func() string {
				r.mu.Lock()
				defer r.mu.Unlock()
				return r.stopCode
			}
			w.PauseWith(time.Now().Add(time.Hour), c.mode)
			time.Sleep(300 * time.Millisecond)
			if got := cores(2 * time.Second); (got > 0.3) != c.running {
				t.Fatalf("paused %s: the tool used %.2f cores", c.mode, got)
			}
			if code := stopped(); (code == "paused") != c.givesUp {
				t.Fatalf("after 2 s paused (give-back 1 s): stop code %q", code)
			}
			if !c.resumeOn {
				return
			}
			if st := r.status(); st.Progress.Phase != "paused" {
				t.Fatalf("the held job's phase: %q", st.Progress.Phase)
			}
			w.Resume()
			time.Sleep(300 * time.Millisecond)
			if got := cores(time.Second); got < 0.3 {
				t.Fatalf("resumed: the tool used %.2f cores", got)
			}
		})
	}
}

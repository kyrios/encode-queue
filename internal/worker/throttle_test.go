package worker

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The duty cycle's arithmetic: a tool within the limit runs free, one above it runs the limit
// over its rate, a lifted or higher limit frees it.
func TestThrottleStep(t *testing.T) {
	if runtime.NumCPU() < 4 {
		t.Skip("needs 4 CPUs")
	}
	period := 100 * time.Millisecond
	cores := func(n float64) time.Duration { return time.Duration(n * float64(period)) }
	var th throttle
	th.reset(1)
	for i := 0; i < 30; i++ {
		th.step(3, cores(3.3), period) // x265 at pools=3 uses a little more than 3
	}
	if th.duty != 1 {
		t.Fatalf("a tool at 3.3 cores under a limit of 3: duty %.2f, want free", th.duty)
	}
	th.reset(1)
	for i := 0; i < 30; i++ {
		ran := time.Duration(th.duty * float64(period))
		th.step(2, cores(9.8*th.duty), ran) // 9.8 cores while it runs
	}
	if want := 2 / 9.8; th.duty < want-0.01 || th.duty > want+0.01 {
		t.Fatalf("a tool at 9.8 cores under a limit of 2: duty %.3f, want %.3f", th.duty, want)
	}
	held := th
	th.step(0, cores(9.8*th.duty), time.Duration(th.duty*float64(period)))
	if th.duty != 1 {
		t.Fatalf("no limit: duty %.2f", th.duty)
	}
	th = held
	th.step(runtime.NumCPU(), cores(9.8*th.duty), time.Duration(th.duty*float64(period)))
	if th.duty != 1 {
		t.Fatalf("a limit of every core: duty %.2f", th.duty)
	}
	th = held
	for i := 0; i < 60; i++ {
		ran := time.Duration(th.duty * float64(period))
		th.step(2, cores(1.5*th.duty), ran) // the tool now needs only 1.5 cores
	}
	if th.duty != 1 {
		t.Fatalf("a tool that fell to 1.5 cores under a limit of 2: duty %.2f, want free", th.duty)
	}
	th.reset(1)
	for i := 0; i < 30; i++ {
		ran := time.Duration(th.duty * float64(period))
		th.step(1, cores(100*th.duty), ran)
	}
	if th.duty != throttleMinRun {
		t.Fatalf("a tool at 100 cores under 1: duty %.3f, want the floor %.2f", th.duty, throttleMinRun)
	}
}

// TestBusyHelper is the fake long tool: with EQ_BUSY_HELPER=N it keeps N threads busy for a
// minute, as x265 keeps its pool busy.
func TestBusyHelper(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("EQ_BUSY_HELPER"))
	if n == 0 {
		t.Skip("the fake tool for TestARunningToolKeepsTheLimit")
	}
	end := time.Now().Add(time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := 0
			for time.Now().Before(end) {
				for j := 0; j < 1e6; j++ {
					x += j
				}
			}
			_ = x
		}()
	}
	wg.Wait()
	os.Exit(0)
}

// A tool started with every core keeps a limit lowered while it runs, stands still through a
// pause, and runs free again once the limit is lifted; the limits event says it is held.
func TestARunningToolKeepsTheLimit(t *testing.T) {
	busy := min(3, runtime.NumCPU()-1)
	if busy < 2 {
		t.Skip("needs 3 CPUs")
	}
	t.Setenv("EQ_BUSY_HELPER", strconv.Itoa(busy))
	var evMu sync.Mutex
	var held []any
	w, err := New(Config{StateDir: t.TempDir(), Events: func(e map[string]any) {
		if e["event"] == "limits" {
			evMu.Lock()
			held = append(held, e["throttled"])
			evMu.Unlock()
		}
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	l := &Lease{ID: "ls_THROTTLE", PauseGiveBackS: 600}
	r := newRun(w, l, false)
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	w.runs[l.ID] = r
	w.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.runTool(ctx, []string{os.Args[0], "-test.run=^TestBusyHelper$"}, "ffmpeg", nil)
	}()
	go func() { defer wg.Done(); r.throttleLoop(ctx) }()
	go func() { defer wg.Done(); r.pauseLoop(ctx) }()
	defer func() { cancel(); wg.Wait() }()

	pid := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.cmd == nil || r.cmd.Process == nil {
			return 0
		}
		return r.cmd.Process.Pid
	}
	deadline := time.Now().Add(5 * time.Second)
	for pid() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	p := pid()
	if p == 0 {
		t.Fatal("the fake tool did not start")
	}
	// cores is the tool's CPU use over d.
	cores := func(d time.Duration) float64 {
		c0, ok0 := groupCPU(p)
		t0 := time.Now()
		time.Sleep(d)
		c1, ok1 := groupCPU(p)
		if !ok0 || !ok1 {
			t.Fatal("no CPU time for the fake tool")
		}
		return (c1 - c0).Seconds() / time.Since(t0).Seconds()
	}
	throttled := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.throttled
	}
	// within waits up to 15 s for a second in which the tool's use and state are what ok wants:
	// beside other tests (CI runs the packages side by side) a second may get fewer cores.
	within := func(what string, ok func(c float64) bool) {
		t.Helper()
		var seen []string
		for end := time.Now().Add(15 * time.Second); time.Now().Before(end); {
			c := cores(time.Second)
			seen = append(seen, strconv.FormatFloat(c, 'f', 2, 64))
			if ok(c) {
				t.Logf("%s: %.2f cores", what, c)
				return
			}
		}
		t.Fatalf("%s: never; the tool used %v cores a second, throttled %v", what, seen, throttled())
	}
	free := func(c float64) bool { return c >= float64(busy)-0.8 && !throttled() }
	toOne := func(c float64) bool { return c >= 0.3 && c <= 1.4 && throttled() && r.status().Throttled }

	within("no limit, free", free)
	w.SetLimits(1, false, 1, nil)
	within("a limit of 1 while it runs, held to it", toOne)
	w.Pause(time.Now().Add(time.Minute))
	time.Sleep(300 * time.Millisecond)
	if c := cores(time.Second); c > 0.05 {
		t.Fatalf("paused under the throttle: the tool uses %.2f cores, want none", c)
	}
	w.Resume()
	within("resumed, held again", toOne)
	w.SetLimits(0, false, 1, nil)
	within("the limit lifted, free", free)
	evMu.Lock()
	defer evMu.Unlock()
	saw := map[any]bool{}
	for _, h := range held {
		saw[h] = true
	}
	if !saw[1] || !saw[0] || held[len(held)-1] != 0 {
		t.Fatalf("the limits events' throttled: %v, want 1 while held and 0 at the end", held)
	}
}

// A lowered limit reaches a running tool only through the throttle; the next tool starts with
// it, and the limits event and the presence say it.
func TestTheLimitsEventSaysTheLimitInEffect(t *testing.T) {
	var got []map[string]any
	w, err := New(Config{StateDir: t.TempDir(), CPUThreads: 0, Events: func(e map[string]any) {
		if e["event"] == "limits" {
			got = append(got, e)
		}
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.emitLimits()
	w.SetLimits(3, true, 1, nil)
	w.SetLimits(-2, true, 1, nil)
	n := runtime.NumCPU()
	if len(got) != 3 || got[0]["threads"] != n || got[0]["cores"] != n || got[1]["threads"] != min(3, n) || got[2]["threads"] != n {
		t.Fatalf("limits events: %v", got)
	}
	if got[1]["throttled"] != 0 {
		t.Fatalf("throttled without a run: %v", got[1])
	}
	if w.threads() != 0 {
		t.Fatalf("a negative limit gives %d", w.threads())
	}
	w.SetLimits(3, true, 1, nil)
	w.mu.Lock()
	l := w.limits()
	patch := w.limitsPatch
	w.mu.Unlock()
	if l["cpu_threads"] != 3 || !patch {
		t.Fatalf("the queue's limits: %v, patch %v", l, patch)
	}
}

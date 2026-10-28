package worker

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// unclamped starts this test's tools as a realtime job's are: the throttle's and the pause's
// tests measure the cores a tool takes, which a normal job's utility clamp gives to everything
// else on a busy Mac.
func unclamped(t *testing.T) {
	saved := normalPrefix
	normalPrefix = nil
	t.Cleanup(func() { normalPrefix = saved })
}

// psNice is a process's nice value as ps reads it.
func psNice(t *testing.T, pid int) string {
	t.Helper()
	out, err := exec.Command("ps", "-o", "nice=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatalf("ps: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// A normal job's tool runs at nice 19, from whichever thread the runtime starts it, with the pid
// it was started with; a realtime job's at the worker's own nice (v0.2.19). Twenty of each,
// started from busy goroutines.
func TestNormalToolsRunAtNice19AndRealtimeOnesAtTheWorkersNice(t *testing.T) {
	if _, err := exec.LookPath("ps"); err != nil {
		t.Skip("no ps")
	}
	if len(normalPrefix) == 0 {
		t.Skip("no nice here")
	}
	mine := psNice(t, os.Getpid())
	var wg sync.WaitGroup
	errs := make(chan string, 40)
	for i := 0; i < 40; i++ {
		rt := i%2 == 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			line := launch([]string{"sh", "-c", "echo $$; ps -o nice= -p $$"}, rt)
			cmd := exec.Command(line[0], line[1:]...)
			var out strings.Builder
			cmd.Stdout = &out
			if err := start(cmd); err != nil {
				errs <- err.Error()
				return
			}
			cmd.Wait()
			got := strings.Fields(out.String())
			want := "19"
			if rt {
				want = mine
			}
			if len(got) != 2 || got[0] != strconv.Itoa(cmd.Process.Pid) || got[1] != want {
				errs <- "realtime " + strconv.FormatBool(rt) + ": " + out.String()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	// A tool that is not there keeps its own line, so it fails as tool_missing.
	if got := launch([]string{"/nonexistent/ffmpeg", "-version"}, false); got[0] != "/nonexistent/ffmpeg" {
		t.Fatalf("a missing tool: %v", got)
	}
	if runtime.GOOS == "darwin" && !strings.HasSuffix(normalPrefix[0], "taskpolicy") {
		t.Fatalf("on macOS a normal tool starts under the utility clamp: %v", normalPrefix)
	}
}

// On macOS a realtime job puts the normal jobs beside it into the background QoS, the ones that
// start meanwhile too, and gives them back when it ends; the realtime job's own tool stays out.
func TestARealtimeJobMovesTheNormalJobsIntoTheBackground(t *testing.T) {
	if runtime.GOOS != "darwin" || efficiencySpeed() == 0 {
		t.Skip("on a Mac with efficiency cores")
	}
	t.Setenv("EQ_BUSY_HELPER", "1")
	background := func(pid int) bool {
		out, err := exec.Command("ps", "-o", "pri=", "-p", strconv.Itoa(pid)).Output()
		n, _ := strconv.Atoi(strings.TrimSpace(string(out)))
		return err == nil && n <= 4
	}
	w, err := New(Config{StateDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	tool := func(prio string) (*run, *Lease, int) {
		l := &Lease{ID: "ls_RT_" + prio}
		l.Job.Priority = prio
		r := newRun(w, l, false)
		if err := os.MkdirAll(r.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		w.mu.Lock()
		w.runs[l.ID] = r
		w.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runTool(ctx, []string{os.Args[0], "-test.run=^TestBusyHelper$"}, "ffmpeg", nil)
		}()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			r.mu.Lock()
			if r.cmd != nil && r.cmd.Process != nil {
				pid := r.cmd.Process.Pid
				r.mu.Unlock()
				return r, l, pid
			}
			r.mu.Unlock()
		}
		t.Fatal("the tool did not start")
		return nil, nil, 0
	}
	archive, _, apid := tool("archive")
	if background(apid) {
		t.Fatal("the archive job in the background before any realtime job")
	}
	_, rl, rpid := tool("now")
	w.realtimeBegins(rl)
	stage1, _, spid := tool("stage1")
	if !background(apid) || !archive.status().EfficiencyCores {
		t.Error("the archive job beside the realtime job is not in the background")
	}
	if !background(spid) || !stage1.status().EfficiencyCores {
		t.Error("the stage 1 job that started beside the realtime job is not in the background")
	}
	if background(rpid) {
		t.Error("the realtime job's own tool is in the background")
	}
	w.realtimeEnds(rl)
	if background(apid) || background(spid) {
		t.Error("the normal jobs are still in the background after the realtime job ended")
	}
}

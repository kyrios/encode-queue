package worker

import (
	"context"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The efficiency cores (v0.2.18): archive jobs only, or every job, reach a running tool at once
// and go the same way; the queue hears the mode and the speed. On a Mac with efficiency cores.
func TestTheEfficiencyCoresReachARunningTool(t *testing.T) {
	if efficiencySpeed() == 0 {
		t.Skip("no efficiency cores here")
	}
	t.Setenv("EQ_BUSY_HELPER", "1")
	// ps shows a process in the background QoS at priority 4, one at nice 19 at 31.
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
	pids := map[string]int{}
	runs := map[string]*run{}
	for _, prio := range []string{"archive", "stage1"} {
		l := &Lease{ID: "ls_EFF_" + prio}
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
		for deadline := time.Now().Add(5 * time.Second); pids[prio] == 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			r.mu.Lock()
			if r.cmd != nil && r.cmd.Process != nil {
				pids[prio] = r.cmd.Process.Pid
			}
			r.mu.Unlock()
		}
		runs[prio] = r
	}
	check := func(what string, archive, stage1 bool) {
		t.Helper()
		if got := background(pids["archive"]); got != archive || runs["archive"].status().EfficiencyCores != archive {
			t.Errorf("%s: the archive job in the background %v, want %v", what, got, archive)
		}
		if got := background(pids["stage1"]); got != stage1 || runs["stage1"].status().EfficiencyCores != stage1 {
			t.Errorf("%s: the stage 1 job in the background %v, want %v", what, got, stage1)
		}
	}
	check("off", false, false)
	w.SetEfficiency(EfficiencyArchive)
	check("archive", true, false)
	w.mu.Lock()
	l := w.limits()
	w.mu.Unlock()
	if l["efficiency"] != EfficiencyArchive || math.Abs(l["efficiency_speed"].(float64)-efficiencySpeed()) > 1e-9 {
		t.Fatalf("the queue's limits: %v", l)
	}
	w.SetEfficiency(EfficiencyAll)
	check("all", true, true)
	w.SetEfficiency("sometimes")
	check("off again", false, false)
	if _, ok := l["efficiency"]; ok && w.efficiency() != EfficiencyOff {
		t.Fatalf("an unknown mode: %q", w.efficiency())
	}
}

// The estimate: the Mac Studio's 8 performance and 4 efficiency cores give the measured 10.6.
func TestEfficiencySpeedOfTheMacStudio(t *testing.T) {
	e := 4 * effPerfRatio
	if slow := (8 + e) / e; slow < 10.3 || slow > 10.8 {
		t.Fatalf("8 P and 4 E cores: %.2f times as long, measured 10.6", slow)
	}
	if normalEfficiency(EfficiencyAll) == EfficiencyAll && efficiencySpeed() == 0 {
		t.Fatal("efficiency cores where there are none")
	}
}

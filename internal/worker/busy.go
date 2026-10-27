package worker

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// **How many threads the worker's tools keep busy** (v0.2.5): the CPU time their process groups
// used since the last presence, over the time between. The Encoder page draws a square per thread
// and colours this many as busy. Only the worker's own tools count, not the rest of the machine.

// goos is runtime.GOOS; a variable so a test can take the other path.
var goos = runtime.GOOS

// clockTicks is Linux's USER_HZ, the unit of /proc/<pid>/stat's utime and stime.
const clockTicks = 100

// busySampler turns the tools' CPU time into busy threads. On Linux it compares the process
// groups' utime and stime with the last sample; macOS has no /proc, so there ps's share of a CPU
// per process counts as it is.
type busySampler struct {
	last     float64
	lastAt   time.Time
	readCPU  func(pgids []int) (float64, bool)
	readPcpu func(pgids []int) (float64, bool)
}

func newBusySampler() *busySampler {
	return &busySampler{readCPU: procCPUSeconds, readPcpu: psPercent}
}

// sample is the busy threads now, rounded, or -1 when unknown.
func (b *busySampler) sample(pgids []int, now time.Time) int {
	if len(pgids) == 0 {
		b.lastAt = time.Time{}
		return 0
	}
	if goos == "darwin" {
		p, ok := b.readPcpu(pgids)
		if !ok {
			return -1
		}
		return int(math.Round(p / 100))
	}
	cpu, ok := b.readCPU(pgids)
	if !ok {
		return -1
	}
	defer func() { b.last, b.lastAt = cpu, now }()
	if b.lastAt.IsZero() || cpu < b.last {
		return -1 // the first sample of a run has nothing to compare with
	}
	wall := now.Sub(b.lastAt).Seconds()
	if wall <= 0 {
		return -1
	}
	return int(math.Round((cpu - b.last) / wall))
}

// procCPUSeconds sums utime and stime of every process in the groups, from /proc.
func procCPUSeconds(pgids []int) (float64, bool) {
	want := map[int]bool{}
	for _, g := range pgids {
		want[g] = true
	}
	dirs, err := filepath.Glob("/proc/[0-9]*/stat")
	if err != nil || len(dirs) == 0 {
		return 0, false
	}
	var ticks int64
	for _, p := range dirs {
		data, err := os.ReadFile(p)
		if err != nil {
			continue // gone since the glob
		}
		g, t, ok := statTicks(string(data))
		if ok && want[g] {
			ticks += t
		}
	}
	return float64(ticks) / clockTicks, true
}

// statTicks reads a /proc/<pid>/stat line: its process group and utime + stime. The command name
// in parentheses may hold spaces, so the fields count from its closing parenthesis.
func statTicks(line string) (pgrp int, ticks int64, ok bool) {
	i := strings.LastIndexByte(line, ')')
	if i < 0 {
		return 0, 0, false
	}
	f := strings.Fields(line[i+1:])
	// After ")": state ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt utime stime …
	if len(f) < 13 {
		return 0, 0, false
	}
	g, err1 := strconv.Atoi(f[2])
	u, err2 := strconv.ParseInt(f[11], 10, 64)
	s, err3 := strconv.ParseInt(f[12], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, false
	}
	return g, u + s, true
}

// psPercent sums ps's %cpu over the processes of the groups (macOS).
func psPercent(pgids []int) (float64, bool) {
	gs := make([]string, len(pgids))
	for i, g := range pgids {
		gs[i] = strconv.Itoa(g)
	}
	out, err := exec.Command("ps", "-o", "pcpu=", "-g", strings.Join(gs, ",")).Output()
	if err != nil && len(out) == 0 {
		return 0, false
	}
	return sumPercent(string(out)), true
}

func sumPercent(out string) float64 {
	var sum float64
	for _, f := range strings.Fields(out) {
		if v, err := strconv.ParseFloat(strings.ReplaceAll(f, ",", "."), 64); err == nil {
			sum += v
		}
	}
	return sum
}

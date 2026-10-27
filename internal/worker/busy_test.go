package worker

import (
	"testing"
	"time"
)

func TestStatTicks(t *testing.T) {
	// A real line from /proc/<pid>/stat of an x265 encode, its name with a space.
	line := "4242 (ffmpeg main) S 4100 4242 4100 0 -1 4194304 91234 0 12 0 734512 2210 0 0 39 19 13 0 1234567 2147483648 51200 18446744073709551615"
	g, ticks, ok := statTicks(line)
	if !ok || g != 4242 || ticks != 736722 {
		t.Fatalf("pgrp %d ticks %d ok %v", g, ticks, ok)
	}
	if _, _, ok := statTicks("garbage"); ok {
		t.Fatal("read a line without a name")
	}
}

func TestBusySamplerLinux(t *testing.T) {
	cpu := 100.0
	old := goos
	goos = "linux"
	defer func() { goos = old }()
	b := &busySampler{readCPU: func([]int) (float64, bool) { return cpu, true }}
	t0 := time.Date(2026, 10, 27, 13, 50, 0, 0, time.UTC)
	if n := b.sample([]int{4242}, t0); n != -1 {
		t.Fatalf("first sample %d, want unknown", n)
	}
	cpu += 30 * 11.6 // 30 s at 11.6 threads
	if n := b.sample([]int{4242}, t0.Add(30*time.Second)); n != 12 {
		t.Fatalf("busy %d, want 12", n)
	}
	if n := b.sample(nil, t0.Add(60*time.Second)); n != 0 {
		t.Fatalf("no tool running: %d", n)
	}
}

func TestBusySamplerMac(t *testing.T) {
	old := goos
	goos = "darwin"
	defer func() { goos = old }()
	b := &busySampler{readPcpu: func([]int) (float64, bool) { return 1163, true }}
	if n := b.sample([]int{4242}, time.Now()); n != 12 {
		t.Fatalf("busy %d, want 12", n)
	}
}

func TestSumPercent(t *testing.T) {
	if s := sumPercent(" 598,3\n  2.1\n"); s < 600.3 || s > 600.5 {
		t.Fatalf("sum %v", s)
	}
}

func TestStatCPU(t *testing.T) {
	// /proc/stat of a 6-thread VM: user nice system idle iowait irq softirq steal guest guest_nice.
	busy, total, ok := statCPU("cpu  1000 50 300 8000 200 10 40 0 500 0\ncpu0 1 2 3 4 5 6 7 8 9 10\n")
	if !ok || busy != 1400 || total != 9600 {
		t.Fatalf("busy %d total %d", busy, total)
	}
	m := &machineSampler{threads: func() int { return 6 }}
	n := 0
	m.readStat = func() (uint64, uint64, bool) {
		n++
		if n == 1 {
			return 1400, 9600, true
		}
		return 1400 + 300, 9600 + 600, true // half the machine busy since
	}
	old := goos
	goos = "linux"
	defer func() { goos = old }()
	if v := m.sample(); v != -1 {
		t.Fatalf("first sample %v", v)
	}
	if v := m.sample(); v != 3 {
		t.Fatalf("busy threads %v, want 3", v)
	}
	if o := otherLoad(3, 2); o != 1 {
		t.Fatalf("other load %v", o)
	}
	if otherLoad(1, 2) != 0 || otherLoad(-1, 2) != -1 || otherLoad(3, -1) != -1 {
		t.Fatal("otherLoad's edges")
	}
}

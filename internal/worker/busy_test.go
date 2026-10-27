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

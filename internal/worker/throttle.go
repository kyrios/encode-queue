package worker

import (
	"context"
	"fmt"
	"runtime"
	"syscall"
	"time"
)

// **A running tool keeps a lowered CPU limit** (v0.2.17; Thorsten, 2026-10-28: Slipstream said
// "3 von 12 Kernen" while its archive encode, started with pools=12, ran at 978 % for hours).
// x265's pool and whisper.cpp's threads are fixed when a tool starts, and x264 keeps its own; so
// a tool that uses clearly more cores than the limit runs in a duty cycle instead, cpulimit's
// way: in each 100 ms it runs for a share and stands still for the rest (SIGSTOP and SIGCONT to
// its process group), the share being the limit over the cores it uses while it runs, measured
// from its CPU time. It then averages the limit's cores and loses no work. A higher limit, or
// none, frees it within a period. A pause's SIGSTOP wins: this never continues a paused tool.
// SIGSTOP beat macOS's own means: the background policy (taskpolicy -b) keeps a tool on the
// efficiency cores whatever the limit, and on a busy Mac all but stopped an encode (README,
// "The CPU limit", has the numbers).

const (
	throttlePeriod = 100 * time.Millisecond
	// throttleOver: a free tool is held once it uses this many cores more than the limit, so a
	// tool started at the limit runs free: x265 4K slower with pools=3 used 2.95 cores over a
	// minute and 3.4 at most in a second (ffmpeg's decoder, x265's lookahead).
	throttleOver = 0.5
	// throttleMinRun is the least share of a period a held tool runs, so it never starves.
	throttleMinRun = 0.05
	// throttleSmooth weighs each period's measurement: about the last second counts.
	throttleSmooth = 0.1
	// throttleMaxStop is the longest a held tool stands still at a time.
	throttleMaxStop = time.Second
)

// throttle is one run's duty cycle, as throttleLoop measures and steers it.
type throttle struct {
	pid   int           // the tool it measures; 0 none
	cpu   time.Duration // the tool's CPU time at the end of the last period
	known bool          // cpu holds a reading: not after a new tool or a pause
	rate  float64       // the cores it uses while it runs, smoothed; 0 not measured yet
	duty  float64       // the share of each period it runs: 1 free
}

// reset starts afresh on tool pid (0: none): free and not measured.
func (t *throttle) reset(pid int) { *t = throttle{pid: pid, duty: 1} }

// step takes one period's measurement, the CPU time the tool used and how long it was let run,
// and sets the share of the next period it runs under limit cores (0: no limit).
func (t *throttle) step(limit int, used, ran time.Duration) {
	if ran <= 0 || used < 0 {
		return
	}
	rate := used.Seconds() / ran.Seconds()
	if t.rate == 0 {
		t.rate = rate
	} else {
		t.rate += throttleSmooth * (rate - t.rate)
	}
	switch {
	case limit <= 0 || limit >= runtime.NumCPU():
		t.duty = 1
	case t.duty >= 1 && t.rate <= float64(limit)+throttleOver:
		// free, and within the limit
	case t.rate <= 0:
		t.duty = 1
	default:
		t.duty = min(1, max(throttleMinRun, float64(limit)/t.rate))
	}
}

// throttleLoop holds the run's tool to the worker's CPU limit while the run lasts.
func (r *run) throttleLoop(ctx context.Context) {
	var t throttle
	t.reset(0)
	mark := time.Now()
	defer func() {
		r.mu.Lock()
		r.release()
		r.mu.Unlock()
		r.setThrottled(false, "")
	}()
	for {
		limit := r.w.threads()
		r.mu.Lock()
		pid := 0
		if r.cmd != nil && r.cmd.Process != nil {
			pid = r.cmd.Process.Pid
		}
		paused := r.pausedSince != nil
		r.mu.Unlock()
		if pid != t.pid || paused || pid == 0 {
			t.reset(pid)
			mark = time.Now()
			r.setThrottled(false, "")
		}
		duty := t.duty
		run := throttlePeriod
		if duty < 1 {
			run = time.Duration(duty * float64(throttlePeriod))
		}
		if !sleep(ctx, run) {
			return
		}
		ran := time.Since(mark)
		if duty < 1 {
			r.mu.Lock()
			if r.cmd != nil && r.cmd.Process != nil && r.cmd.Process.Pid == pid && r.pausedSince == nil {
				r.signal(syscall.SIGSTOP)
				r.held = true
			}
			r.mu.Unlock()
			ran = time.Since(mark)
			// The stop lasts in proportion to the run as it was: the worker runs at nice 19, and
			// on a busy Mac it wakes late (an x265 held to 3 of 12 cores averaged 3.37 when the stop
			// lasted the rest of a fixed period).
			if !sleep(ctx, min(time.Duration(float64(ran)*(1-duty)/duty), throttleMaxStop)) {
				return
			}
		}
		// The CPU time while the tool stands still, before it runs on; none across a pause.
		r.mu.Lock()
		paused = paused || r.pausedSince != nil
		r.mu.Unlock()
		if cpu, ok := groupCPU(pid); ok && pid != 0 && !paused {
			if t.known {
				t.step(limit, cpu-t.cpu, ran)
			}
			t.cpu, t.known = cpu, true
		} else {
			t.known = false
		}
		r.mu.Lock()
		r.release()
		r.mu.Unlock()
		mark = time.Now()
		if t.duty < 1 {
			r.setThrottled(true, fmt.Sprintf("held to %d of %d cores: it uses %.1f while it runs, so it runs %.0f %% of each %v",
				limit, runtime.NumCPU(), t.rate, t.duty*100, throttlePeriod))
		} else {
			r.setThrottled(false, "free again: the limit allows what it uses")
		}
	}
}

// release continues the tool this loop holds, unless a pause holds it too; call with r.mu held.
func (r *run) release() {
	if r.held && r.pausedSince == nil {
		r.signal(syscall.SIGCONT)
	}
	r.held = false
}

// setThrottled notes whether the run's tool is held to the limit now. A change goes to the job's
// log, with why, unless why is empty (the tool ended, a pause), and to Slipstream's menu as the
// limits event.
func (r *run) setThrottled(on bool, why string) {
	r.mu.Lock()
	changed := r.throttled != on
	r.throttled = on
	r.mu.Unlock()
	if !changed {
		return
	}
	if why != "" {
		r.log("info", "worker", why)
	}
	r.w.emitLimits()
}

// sleep waits d, or until ctx ends: false then.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

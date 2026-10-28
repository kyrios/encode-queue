package worker

// **The efficiency cores** (v0.2.18; Thorsten, 2026-10-28: "Run archive encodes only on
// efficiency cores"). Slipstream's limits may say efficiency all (every job) or archive (jobs of
// priority archive): their tools run in macOS's background QoS (PRIO_DARWIN_BG, as taskpolicy -b),
// which keeps every thread on the efficiency cores, so the performance cores stay with the person
// at the Mac. It reaches a running tool at once, and goes the same way. A tool keeps the limit's
// threads, so one taken out of the background has every core again. The queue hears the mode and
// efficiencySpeed's estimate, for its ECT and the lease's hold.
const (
	EfficiencyOff     = "off"
	EfficiencyAll     = "all"
	EfficiencyArchive = "archive"
)

// effPerfRatio is an efficiency core's x265 throughput against a performance core's, from the Mac
// Studio (M2 Max, 8 P and 4 E cores, 2026-10-28): x265 slower 1080p with pools=12, 180 frames,
// took 297.8 s in the background against 28.2 s, 10.6 times as long (three runs each; README,
// "The efficiency cores"), and efficiencySpeed's formula solved for that gives 0.21.
const effPerfRatio = 0.21

// efficiencySpeed is the share of the machine's full speed a tool gets on its efficiency cores,
// e·r / (p + e·r) for p performance and e efficiency cores; 0 where the machine has one kind.
func efficiencySpeed() float64 {
	p, e := coreLevels()
	if p == 0 || e == 0 {
		return 0
	}
	return float64(e) * effPerfRatio / (float64(p) + float64(e)*effPerfRatio)
}

// normalEfficiency is mode as the worker keeps it: all, archive or off, off where there are no
// efficiency cores.
func normalEfficiency(mode string) string {
	if (mode != EfficiencyAll && mode != EfficiencyArchive) || efficiencySpeed() == 0 {
		return EfficiencyOff
	}
	return mode
}

// efficiency is which jobs run on the efficiency cores now.
func (w *Worker) efficiency() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cfg.Efficiency
}

// SetEfficiency changes which jobs run on the efficiency cores (Slipstream's limits line), for the
// running tools at once; the queue hears it with the next presence, which this wakes.
func (w *Worker) SetEfficiency(mode string) {
	mode = normalEfficiency(mode)
	w.mu.Lock()
	if mode == w.cfg.Efficiency {
		w.mu.Unlock()
		return
	}
	w.cfg.Efficiency = mode
	w.limitsPatch = true
	close(w.pauseCh) // wakes the presence loop
	w.pauseCh = make(chan struct{})
	runs := make([]*run, 0, len(w.runs))
	for _, r := range w.runs {
		runs = append(runs, r)
	}
	w.mu.Unlock()
	w.cfg.Log.Info("efficiency cores", "jobs", mode)
	for _, r := range runs {
		r.applyBackground()
	}
	w.emitLimits()
}

// background is whether this run's tools belong on the efficiency cores now.
func (r *run) background() bool {
	switch r.w.efficiency() {
	case EfficiencyAll:
		return true
	case EfficiencyArchive:
		return r.l != nil && r.l.Job.Priority == "archive"
	}
	return false
}

// applyBackground puts the running tool into the background QoS, or out of it, as background says.
func (r *run) applyBackground() {
	on := r.background()
	r.mu.Lock()
	if r.cmd == nil || r.cmd.Process == nil || on == r.bg {
		r.mu.Unlock()
		return
	}
	err := setBackground(r.cmd.Process.Pid, on)
	if err == nil {
		r.bg = on
	}
	r.mu.Unlock()
	switch {
	case err != nil:
		r.log("warn", "worker", "the efficiency cores: "+err.Error())
	case on:
		r.log("info", "worker", "on the efficiency cores now (background QoS)")
	default:
		r.log("info", "worker", "on every core again")
	}
}

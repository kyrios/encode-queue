package worker

import "runtime"

// **A realtime job on macOS: the normal jobs beside it go into the background QoS** (v0.2.20,
// nice.go). Their tools started under the utility clamp; while a realtime job runs on this
// worker, each normal run's tool is in PRIO_DARWIN_BG, and so is every tool a normal run starts
// meanwhile. When the last realtime run ends they come out, unless the efficiency mode keeps them
// there. On Linux nice 19 from the start does it, and nothing changes while a job runs.

// yields says whether this run's tool gives way to a realtime job now.
func (r *run) yields() bool {
	return runtime.GOOS == "darwin" && r.l != nil && !realtime(r.l.Job.Priority) && r.w.realtimeRunning()
}

func (w *Worker) realtimeRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.realtimeN > 0
}

// realtimeBegins counts a realtime run, and with the first one moves the normal runs' tools.
func (w *Worker) realtimeBegins(l *Lease) {
	w.mu.Lock()
	w.realtimeN++
	first := w.realtimeN == 1
	others := w.normalRuns()
	w.mu.Unlock()
	if first && len(others) > 0 {
		w.cfg.Log.Info("a realtime job begins; normal jobs give way", "lease", l.ID, "job", l.Job.ID, "normal_jobs", len(others))
		for _, r := range others {
			r.applyBackground()
		}
	}
}

// realtimeEnds counts a realtime run out, and with the last one gives the normal runs their
// share back.
func (w *Worker) realtimeEnds(l *Lease) {
	w.mu.Lock()
	w.realtimeN--
	last := w.realtimeN == 0
	others := w.normalRuns()
	w.mu.Unlock()
	if last && len(others) > 0 {
		w.cfg.Log.Info("the realtime jobs ended; normal jobs run as before", "lease", l.ID, "job", l.Job.ID, "normal_jobs", len(others))
		for _, r := range others {
			r.applyBackground()
		}
	}
}

// normalRuns is the runs of jobs below realtime; w.mu is held.
func (w *Worker) normalRuns() []*run {
	var out []*run
	for _, r := range w.runs {
		if r.l != nil && !realtime(r.l.Job.Priority) {
			out = append(out, r)
		}
	}
	return out
}

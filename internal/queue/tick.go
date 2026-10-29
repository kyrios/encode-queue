package queue

import (
	"fmt"
	"sort"
	"time"
)

// Run ticks the queue every second until stop is closed: leases that ran out, workers gone
// offline, pauses held too long, preemption, old finished jobs.
func (s *Store) Run(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.Tick()
		}
	}
}

// Tick runs one round of the queue's housekeeping. Tests call it with their own clock.
func (s *Store) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	changed := false
	for _, l := range s.st.Leases {
		if l.State != LeaseActive {
			continue
		}
		w := s.st.Workers[l.WorkerID]
		switch {
		case now.After(l.ExpiresAt):
			s.endLease(l, LeaseExpired, "lease_expired")
			changed = true
		case s.overdue(l, now):
			s.takeOverdue(l)
			changed = true
		case w == nil:
			s.endLease(l, LeaseCancelled, "worker_removed")
			changed = true
		case !s.connected(w, now):
			s.endLease(l, LeaseExpired, "offline")
			changed = true
		case s.pausedTooLong(w, l, now):
			s.endLease(l, LeaseGivenBack, "paused")
			changed = true
		}
	}
	s.watchTunnels(now)
	if s.liftExclusions(now) {
		changed = true
	}
	if s.expireChecks(now) {
		changed = true
	}
	if s.dropPins(now) {
		changed = true
	}
	if s.preempt(now) {
		changed = true
	}
	if s.purgeDone(now) {
		changed = true
	}
	if s.dropStaleUploads(now) {
		changed = true
	}
	if changed {
		s.touch(true)
	}
}

// pausedTooLong is a suspended lease whose worker was paused for longer than the give-back time
// and the grace on top: the worker should have given it back itself. Since v0.2.18 only a lease
// whose tool stands still counts (a worker that pauses after its jobs lets them finish), and a
// lease the pause holds (pauseHolds) goes back only after PauseHoldMax.
func (s *Store) pausedTooLong(w *Worker, l *Lease, now time.Time) bool {
	if !s.paused(w, now) || l.Progress == nil || l.Progress.Phase != "paused" {
		return false
	}
	limit := s.cfg.PauseGiveBack + s.cfg.PauseGrace
	if j := s.st.Jobs[l.JobID]; j != nil && pauseHolds(j) {
		limit = s.cfg.PauseHoldMax
	}
	since := l.CreatedAt
	if w.PausedAt != nil && w.PausedAt.After(since) {
		since = *w.PausedAt
	}
	if l.Progress != nil && l.Progress.PausedSince != nil && l.Progress.PausedSince.After(since) {
		since = *l.Progress.PausedSince
	}
	return now.Sub(since) > limit
}

// **A pause holds an archive or stage 2 lease** (v0.2.18; Thorsten, 2026-10-28). x265 cannot
// resume elsewhere: a 2160 archive encode given back after three of its five hours on the Mac
// Studio loses three hours, and nobody waits for it. Its tool stands still through the pause and
// goes on where it stood; the hold clock stops meanwhile (Renew). A now or stage 1 job, which a
// person waits for, goes back after pause_give_back_s as before, for another worker to take.
func pauseHolds(j *Job) bool { return j.Priority == "archive" || j.Priority == "stage2" }

// preempt ends the lowest lease for every new upload's job that no free worker may run
// (ADR-0007 §1, "new before old"). A `now` job preempts only where the RAM is all that keeps it out
// of a free high-priority slot (v0.2.14, slots.go): that worker's lowest lease below stage1, one at
// a time until its tool has stopped.
func (s *Store) preempt(now time.Time) bool {
	if s.draining {
		return false
	}
	changed := false
	var workers []*Worker
	for _, w := range s.st.Workers {
		workers = append(workers, w)
	}
	for _, j := range s.queued() {
		if j.Pin != nil {
			continue // it waits for its worker
		}
		p := prioIndex(j.Priority)
		if p >= prioIndex("archive") {
			break // queued() is in order: nothing further is new
		}
		served := false
		for _, w := range workers {
			if (s.freeFor(w, j, now) || s.waitsForHigh(w, now)) && s.mismatch(w, j) == "" {
				served = true
				break
			}
		}
		if served {
			continue
		}
		// The candidates: active leases of lower priority whose connected, unpaused worker may
		// run this job, but none nearly done (spared). The one with the least work done goes.
		var cands []*Lease
		for _, l := range s.st.Leases {
			if l.State != LeaseActive {
				continue
			}
			lj := s.st.Jobs[l.JobID]
			w := s.st.Workers[l.WorkerID]
			if lj == nil || w == nil || prioIndex(lj.Priority) <= p {
				continue
			}
			if !s.connected(w, now) || s.paused(w, now) || s.mismatch(w, j) != "" {
				continue
			}
			if j.Priority == PriorityNow && (highPriority(lj.Priority) || s.clearing(w, now) || !s.ramKeepsOut(w, j, now) || !s.roomWithout(w, j, lj)) {
				continue
			}
			if spared(lj, l) {
				continue
			}
			cands = append(cands, l)
		}
		if len(cands) == 0 {
			continue
		}
		sort.Slice(cands, func(a, b int) bool {
			if !cands[a].CreatedAt.Equal(cands[b].CreatedAt) {
				return cands[a].CreatedAt.After(cands[b].CreatedAt)
			}
			return cands[a].ID < cands[b].ID
		})
		victim := cands[0]
		lost := now.Sub(victim.CreatedAt)
		var frames int64
		if p := victim.Progress; p != nil && p.FramesDone != nil {
			frames = *p.FramesDone
		}
		s.cfg.Log.Info("preempt", "lease", victim.ID, "job", victim.JobID, "for", j.ID, "worker", victim.Worker,
			"lost_minutes", fmt.Sprintf("%.1f", lost.Minutes()), "lost_frames", frames)
		if w := s.st.Workers[victim.WorkerID]; w != nil {
			w.PreemptedWorkS += lost.Seconds()
			w.PreemptedFrames += frames
		}
		s.endLease(victim, LeasePreempted, "preempted")
		if j.Priority == PriorityNow {
			s.st.Workers[victim.WorkerID].ramPreemptAt = now
		}
		changed = true
		// The worker is free now; the next job in line finds it so and preempts no other.
	}
	return changed
}

// **Nearly done is never taken back** (v0.2.15; Thorsten, 2026-10-28). Queue v0.2.12 on encoder
// preempted a 2160 archive encode about 95 % done after five hours for the smoke test's stage 2
// job; it started again from zero on the Mac Studio. A lease with less than spareLeft to go by
// its progress, or more than spareDone of its frames done, is spared: the new job waits for it.
// Of the others, the one that ran the shortest goes, whatever its priority, so the least work is
// lost; the log and encode_queue_worker_preempted_work_seconds_total say how much.
const (
	spareLeft = 20 * time.Minute
	spareDone = 0.8
)

// spared says whether lease l of job j is too near its end to be taken back.
func spared(j *Job, l *Lease) bool {
	p := l.Progress
	if p == nil {
		return false
	}
	if p.Fraction != nil && *p.Fraction > spareDone {
		return true
	}
	if p.FramesDone != nil && p.FramesTotal != nil && *p.FramesTotal > 0 && float64(*p.FramesDone)/float64(*p.FramesTotal) > spareDone {
		return true
	}
	if e := eta(j, p); e != nil && time.Duration(*e)*time.Second < spareLeft {
		return true
	}
	return false
}

// dropPins ends the pins whose worker is gone, offline or paused: the job is an ordinary
// queued job again.
func (s *Store) dropPins(now time.Time) bool {
	changed := false
	for _, j := range s.st.Jobs {
		if j.Pin == nil {
			continue
		}
		w := s.st.Workers[j.Pin.WorkerID]
		if j.Final != "" || w == nil || !s.connected(w, now) || s.paused(w, now) {
			j.Pin = nil
			s.bump(j)
			changed = true
		}
	}
	return changed
}

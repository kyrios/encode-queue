package queue

import (
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

// pausedTooLong is a lease whose worker was paused for longer than the give-back time and the
// grace on top: the worker should have given it back itself.
func (s *Store) pausedTooLong(w *Worker, l *Lease, now time.Time) bool {
	if !s.paused(w, now) {
		return false
	}
	since := l.CreatedAt
	if w.PausedAt != nil && w.PausedAt.After(since) {
		since = *w.PausedAt
	}
	if l.Progress != nil && l.Progress.PausedSince != nil && l.Progress.PausedSince.After(since) {
		since = *l.Progress.PausedSince
	}
	return now.Sub(since) > s.cfg.PauseGiveBack+s.cfg.PauseGrace
}

// free is whether a worker could take one more job now.
func (s *Store) free(w *Worker, now time.Time) bool {
	return s.connected(w, now) && s.canClaim(w, now) == nil
}

// preempt ends the lowest lease for every new upload's job that no free worker may run
// (ADR-0007 §1, "new before old").
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
			if s.free(w, now) && s.mismatch(w, j) == "" {
				served = true
				break
			}
		}
		if served {
			continue
		}
		// The candidates: active leases of lower priority whose connected, unpaused worker may
		// run this job. The lowest priority goes first, then the youngest (least work lost).
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
			cands = append(cands, l)
		}
		if len(cands) == 0 {
			continue
		}
		sort.Slice(cands, func(a, b int) bool {
			pa, pb := prioIndex(s.st.Jobs[cands[a].JobID].Priority), prioIndex(s.st.Jobs[cands[b].JobID].Priority)
			if pa != pb {
				return pa > pb
			}
			return cands[a].CreatedAt.After(cands[b].CreatedAt)
		})
		victim := cands[0]
		s.cfg.Log.Info("preempt", "lease", victim.ID, "job", victim.JobID, "for", j.ID)
		s.endLease(victim, LeasePreempted, "preempted")
		changed = true
		// The worker is free now; the next job in line finds it so and preempts no other.
	}
	return changed
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

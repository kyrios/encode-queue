package queue

import "time"

// **Normal and high-priority slots** (v0.2.14; Thorsten, 2026-10-28, after a `now` whisper job
// waited over 40 minutes while every worker's one slot held a 2160 archive encode: "Slipstream
// should have a config with slots for normal jobs and slots for high-prio jobs"). A worker's
// normal slots (max_parallel_jobs) take every job. Its high-priority slots (high_priority_slots, on
// top of them) take only jobs of priority now and stage1, so no archive or stage 2 job ever holds
// one. Both are 1 by default. A worker before 0.2.14 says nothing of high_priority_slots and has
// one: v0.2.5's extra slot, which it claims with "extra": true.
//
// A claim beside a job in the other kind of slot needs the RAM for its peak (ramFits); a normal
// claim beside normal ones does not, as the worker's max_parallel_jobs says it may. Where the RAM
// is all that keeps a `now` job out of a free high-priority slot and no worker has a slot for it,
// the job preempts that worker's lowest lease below stage1 (preempt, tick.go): a high-priority job
// never waits for an archive job while a high-priority slot is free.

// The slots a lease is in.
const (
	SlotNormal = "normal"
	SlotHigh   = "high"
)

// highPriority says whether a job of priority p may take a high-priority slot.
func highPriority(p string) bool { return p == PriorityNow || p == "stage1" }

// maxJobs is the worker's normal slots: what it asked for, at least 1 and at most the queue's
// MaxParallelJobs (v0.2.9), whatever it set through PATCH /me.
func (s *Store) maxJobs(w *Worker) int {
	n := w.Limits.MaxParallelJobs
	if n < 1 {
		n = 1
	}
	if c := s.cfg.MaxParallelJobs; c > 0 && n > c {
		n = c
	}
	return n
}

// highSlots is the worker's high-priority slots: what it asked for, 1 when it says nothing (a
// worker before 0.2.14, with its extra slot), at most the queue's MaxHighPrioritySlots.
func (s *Store) highSlots(w *Worker) int {
	n := 1
	if p := w.Limits.HighPrioritySlots; p != nil {
		n = max(*p, 0)
	}
	if c := s.cfg.MaxHighPrioritySlots; c > 0 && n > c {
		n = c
	}
	return n
}

// slotsBusy is how many of the worker's normal and high-priority slots hold an active lease.
func (s *Store) slotsBusy(w *Worker) (normal, high int) {
	for _, l := range s.activeLeases(w.ID) {
		if l.High {
			high++
		} else {
			normal++
		}
	}
	return normal, high
}

// needsRAM says whether a claim in the worker's normal or high-priority slot must fit its RAM:
// beside a lease in the other kind of slot, or beside any for a high-priority one, and on a worker
// whose lease the queue just took back for the RAM until its tool has stopped (clearing).
func (s *Store) needsRAM(w *Worker, high bool, now time.Time) bool {
	normal, hi := s.slotsBusy(w)
	if s.clearing(w, now) {
		return true
	}
	if high {
		return normal+hi > 0
	}
	return hi > 0
}

// slotFor is the slot of w that takes j now: SlotNormal, SlotHigh, or "" when none is free or
// none has the RAM it needs. Whether w is connected and unpaused is the caller's.
func (s *Store) slotFor(w *Worker, j *Job, now time.Time) string {
	normal, high := s.slotsBusy(w)
	if normal < s.maxJobs(w) && (!s.needsRAM(w, false, now) || s.ramFits(w, j)) {
		return SlotNormal
	}
	if highPriority(j.Priority) && high < s.highSlots(w) && (!s.needsRAM(w, true, now) || s.ramFits(w, j)) {
		return SlotHigh
	}
	return ""
}

// freeFor says whether w could take j now: connected, unpaused, claiming allowed, and a slot for
// it.
func (s *Store) freeFor(w *Worker, j *Job, now time.Time) bool {
	return s.connected(w, now) && s.mayClaim(w, now) == nil && s.slotFor(w, j, now) != ""
}

// ramKeepsOut says whether the RAM is all that keeps j out of a free high-priority slot of w.
func (s *Store) ramKeepsOut(w *Worker, j *Job, now time.Time) bool {
	_, high := s.slotsBusy(w)
	return highPriority(j.Priority) && high < s.highSlots(w) && s.needsRAM(w, true, now) && !s.ramFits(w, j)
}

// roomWithout says whether j would fit w's RAM once the job `gone` stopped there: no lease is
// taken back for a RAM it would not free.
func (s *Store) roomWithout(w *Worker, j, gone *Job) bool {
	free, ok := s.ramFree(w)
	return ok && free+peakOn(w, gone) >= peakOn(w, j)
}

// holdsLower says whether w runs a job below stage1, which a high-priority job does not wait for.
func (s *Store) holdsLower(w *Worker) bool {
	for _, l := range s.activeLeases(w.ID) {
		if j := s.st.Jobs[l.JobID]; j != nil && !highPriority(j.Priority) {
			return true
		}
	}
	return false
}

// waitsForHigh says whether w has a free normal slot that only the RAM of its high-priority jobs
// keeps closed: a job waits for those to end rather than take another worker's lease back.
func (s *Store) waitsForHigh(w *Worker, now time.Time) bool {
	normal, high := s.slotsBusy(w)
	return s.connected(w, now) && s.mayClaim(w, now) == nil && normal < s.maxJobs(w) && high > 0 && !s.clearing(w, now)
}

// waitsBehindLower says whether j, of priority now or stage1, would wait for v to finish a job
// below stage1: v has no slot for it now and runs such a job. A high-priority job does not wait
// for an archive job, so v counts as no better worker for it.
func (s *Store) waitsBehindLower(v *Worker, j *Job, now time.Time) bool {
	return highPriority(j.Priority) && !s.freeFor(v, j, now) && s.holdsLower(v)
}

// clearing says whether the queue took a lease of w back for the RAM so recently that its tool
// may still run (the worker hears it at its next renewal) or still count in the last presence's
// memory_used_bytes. Until then every claim of w needs the RAM, and no other lease of w is taken
// back for it.
func (s *Store) clearing(w *Worker, now time.Time) bool {
	return !w.ramPreemptAt.IsZero() && now.Sub(w.ramPreemptAt) < s.cfg.RenewEvery+2*s.cfg.PresenceEvery
}

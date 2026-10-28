package queue

import (
	"math"
	"time"
)

// **Which worker gets a job: the expected completion time** (v0.2.7; Thorsten, 2026-10-27, the
// video-pipeline session's proposal, worker-scoring.md). A worker pulls work; the queue gives it
// the first job, in priority order, that it would finish about as soon as any other worker could:
//
//	ECT(j, V) = busy(V) + T_in + T_run + T_out
//
// busy is when V has a free slot for the job (0 for the asking worker), T_in the source's bytes
// over V's measured download rate, T_run the job's work over V's measured or calibrated speed,
// scaled to its share of the threads (those free of other programs, split with the jobs that run
// beside it) and corrected by how its earlier jobs of the kind went, and T_out
// the expected result's bytes over its upload rate. The asking worker takes the job when its ECT
// is at most ectSlack times the best, or when the job waited longer than maxWait for its
// priority, so no job waits forever for an ideal worker. Thorsten's example: 50 GB of
// uncompressed 4K go to encoder on Funkhaus's own host (about 13 min) rather than to the Mac
// Studio, three times faster but behind 300 Mbit/s (about 26 min).

// ectSlack keeps a small difference from parking a job.
const ectSlack = 1.25

// maxWait is how long a job of a priority waits for a better worker before any eligible one
// takes it (starting values).
var maxWait = map[string]time.Duration{
	PriorityNow: 30 * time.Second,
	"stage1":    2 * time.Minute,
	"stage2":    15 * time.Minute,
	"archive":   time.Hour,
}

// Rates the queue assumes until it has measured a worker's transfers.
const (
	defaultDownBps = 25e6
	defaultUpBps   = 12.5e6
)

// ECT is one worker's expected completion time for a job, in seconds, and its parts.
type ECT struct {
	WorkerID string  `json:"-"`
	Worker   string  `json:"worker"`
	S        float64 `json:"ect_s"`
	BusyS    float64 `json:"busy_s"`
	InS      float64 `json:"in_s"`
	RunS     float64 `json:"run_s"`
	OutS     float64 `json:"out_s"`
}

// runSeconds is the job's compute time on w from its measured or calibrated speed, before the
// threads and the correction; false when w's speed for it is not known.
func (s *Store) runSeconds(w *Worker, j *Job) (float64, bool) {
	switch {
	case j.rung != nil:
		if d := s.encodeSeconds(w, j); d > 0 {
			return d, true
		}
	case j.whisper != nil:
		_, m := modelOn(w, j)
		if v, ok := whisperSpeed(w, m); ok && v > 0 && j.Spec.Source.DurationS > 0 {
			return j.Spec.Source.DurationS / v, true
		}
	}
	return 0, false
}

// threadShare is how much slower w runs a job than calibrated: the threads it may use over the
// job's share of those free of other programs (other_load), split evenly with `beside`, the
// worker's own jobs that run beside it (v0.2.14: in either kind of slot, slots.go).
func (s *Store) threadShare(w *Worker, beside int) float64 {
	sys := s.system(w)
	if sys.ThreadsInUse == nil || *sys.ThreadsInUse <= 0 {
		return 1
	}
	allowed := float64(*sys.ThreadsInUse)
	free := allowed
	if w.OtherLoad != nil {
		free -= *w.OtherLoad
	}
	free /= float64(beside + 1)
	if free < allowed/8 {
		free = allowed / 8 // never assume a machine as good as stopped
	}
	return allowed / free
}

// correction is the learned ratio of actual to estimated compute time for w and a kind, 1 at
// first.
func correction(w *Worker, kind string) float64 {
	if c, ok := w.Corrections[kind]; ok && c > 0 {
		return c
	}
	return 1
}

// ect is w's expected completion time for j, or false when its speed for j is not known.
func (s *Store) ect(w *Worker, j *Job, asking bool, now time.Time) (ECT, bool) {
	e := ECT{WorkerID: w.ID, Worker: w.Name}
	run, ok := s.runSeconds(w, j)
	if !ok {
		return e, false
	}
	beside := len(s.activeLeases(w.ID))
	if !asking && !s.freeFor(w, j, now) {
		// A high-priority slot the RAM keeps it out of frees nothing: a normal one must.
		f := s.freeAt(w, now, "", highPriority(j.Priority) && !s.ramKeepsOut(w, j, now))
		if f != nil && !f.After(now) {
			// A slot is free but not its RAM: the job waits for one of the worker's leases to end.
			f = s.nextEnd(w, now)
		}
		if f == nil {
			return e, false // busy until it cannot tell
		}
		e.BusyS = math.Max(0, f.Sub(now).Seconds())
		if e.BusyS > 0 {
			beside-- // the job it waits for has ended then
		}
	}
	down := w.Download.AvgBps
	if w.Download.Samples == 0 || down <= 0 {
		down = defaultDownBps
	}
	up := w.Upload.AvgBps
	if w.Upload.Samples == 0 || up <= 0 {
		up = defaultUpBps
	}
	e.InS = float64(j.Spec.Source.Bytes) / down
	e.RunS = run * s.threadShare(w, beside) * correction(w, j.Spec.Kind)
	e.OutS = float64(j.Spec.Result.ExpectedBytes) / up
	e.S = e.BusyS + e.InS + e.RunS + e.OutS
	return e, true
}

// waited is how long j has waited since it was queued or its last attempt ended.
func (s *Store) waited(j *Job, now time.Time) time.Duration {
	since := j.QueuedAt
	for _, id := range j.Attempts {
		if l := s.st.Leases[id]; l != nil && l.EndedAt != nil && l.EndedAt.After(since) {
			since = *l.EndedAt
		}
	}
	return now.Sub(since)
}

// eligible says whether v could take j now as a regular job: connected, unpaused, running its
// tools and kind, with room in its regular slots or a known time until it has.
func (s *Store) eligible(v *Worker, j *Job, now time.Time) bool {
	if !s.connected(v, now) || s.paused(v, now) || v.Caps == nil || s.mismatch(v, j) != "" {
		return false
	}
	acc, _ := s.kinds(v)
	return contains(acc, j.Spec.Kind)
}

// verdict decides whether the asking worker w takes j (v0.2.7): its ECT against the best of
// the eligible workers. It returns w's ECT and the best other worker's for the lease to show.
func (s *Store) verdict(w *Worker, j *Job, now time.Time) (take bool, mine *ECT, runnerUp *ECT) {
	me, ok := s.ect(w, j, true, now)
	if ok {
		mine = &me
	}
	var best *ECT
	for _, v := range s.st.Workers {
		// Only a worker that takes work now is a better one (claiming, v0.2.10), and for a job of
		// priority now or stage1 none that would first finish a lower job (v0.2.14).
		if v == w || !s.eligible(v, j, now) || !s.claiming(v, now) || s.waitsBehindLower(v, j, now) {
			continue
		}
		if e, ok := s.ect(v, j, false, now); ok && (best == nil || e.S < best.S) {
			b := e
			best = &b
		}
	}
	runnerUp = best
	switch {
	case best == nil:
		return true, mine, nil // nobody else known: whoever asks
	case mine == nil:
		// Its own speed is unknown while another's is known: it waits, unless the job waited
		// long enough (a new worker still gets work, and learns its speed from it).
		return s.waited(j, now) > maxWait[j.Priority], mine, runnerUp
	case mine.S <= best.S*ectSlack:
		return true, mine, runnerUp
	default:
		return s.waited(j, now) > maxWait[j.Priority], mine, runnerUp
	}
}

// Why a claim had the comparison it had (v0.2.10; Thorsten, 2026-10-27, on a stage-1 job with
// ect_s and runner_up null: "Why?"). The Encoder page says it in words.
const (
	ScoringScored      = "scored"       // its ECT against the best other worker's
	ScoringWaited      = "waited"       // a better worker existed; the job waited its max_wait
	ScoringAssigned    = "assigned"     // sent to this worker on the Encoder page
	ScoringOnlyWorker  = "only_worker"  // no other worker could take it
	ScoringNotClaiming = "not_claiming" // others could, but asked for no work
	ScoringNoSpeed     = "speed_unknown"
)

// scoringNote is why the claim of j by w had the comparison it had: mine and runnerUp as the
// verdict gave them.
func (s *Store) scoringNote(w *Worker, j *Job, mine, runnerUp *ECT, now time.Time) string {
	if runnerUp != nil {
		if mine == nil || mine.S > runnerUp.S*ectSlack {
			return ScoringWaited
		}
		return ScoringScored
	}
	claiming, idle := 0, 0
	for _, v := range s.st.Workers {
		if v == w || !s.eligible(v, j, now) {
			continue
		}
		if s.claiming(v, now) {
			claiming++
		} else {
			idle++
		}
	}
	switch {
	case claiming > 0:
		return ScoringNoSpeed // others ask, but no speed for the job's class is known for them
	case idle > 0:
		return ScoringNotClaiming
	}
	return ScoringOnlyWorker
}

// learn folds a delivered rung or whisper attempt's compute time into the worker's correction
// for its kind: an EWMA of actual over estimated, kept between a quarter and four.
func (s *Store) learn(w *Worker, j *Job, l *Lease, encodeS float64) {
	if l.EstRunS <= 0 || encodeS <= 0 {
		return
	}
	r := math.Min(4, math.Max(0.25, encodeS/l.EstRunS))
	if w.Corrections == nil {
		w.Corrections = map[string]float64{}
	}
	old := correction(w, j.Spec.Kind)
	w.Corrections[j.Spec.Kind] = 0.7*old + 0.3*r
}

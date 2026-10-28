package queue

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// infraFailure is a failure code that blames the machine, not the worker's tools: a tool the
// system killed, or one that ran out of memory.
func infraFailure(code string) bool { return code == "killed" || code == "out_of_memory" }

// Waiting is why a queued job does not run, when no worker could take it now.
type Waiting struct {
	Code    string     `json:"code"`
	Detail  string     `json:"detail"`
	RetryAt *time.Time `json:"retry_at"`
}

// Why a queued job waits.
const (
	WaitNoEligibleWorker = "no_eligible_worker" // no paired worker runs what it needs
	WaitAllExcluded      = "all_excluded"       // every worker that could failed it or was rejected
	WaitNoWorkerUp       = "no_worker_available"
	// The queue's disk has no room for the result beside what running jobs may still deliver
	// (rooms, v0.2.19): a worker could take it, and none may.
	WaitNoRoom = "no_room"
)

// waiting is why a queued job has no worker that could take it now, or nil when one could
// (it waits only for its turn or a free slot).
func (s *Store) waiting(j *Job, now time.Time) *Waiting {
	if s.jobState(j) != StateQueued {
		return nil
	}
	var able, free []*Worker
	var old []string // workers that run its tools but are too old for it, "name version"
	for _, w := range s.st.Workers {
		if acc, _ := s.kinds(w); !contains(acc, j.Spec.Kind) {
			continue
		}
		if s.mismatchTools(w, j) != "" {
			if tooOld(w, j) != "" && s.mismatchRequires(w, j) == "" {
				old = append(old, w.Name+" "+w.Caps.AppVersion)
			}
			continue
		}
		able = append(able, w)
		if !contains(j.Excluded, w.ID) {
			free = append(free, w)
		}
	}
	switch {
	case len(able) == 0 && len(old) > 0:
		// The Encoder page shows the detail: which version the job waits for, and who is older.
		sort.Strings(old)
		v, what := minVersion(j)
		return &Waiting{Code: WaitNoEligibleWorker, Detail: "it waits for a worker of " + v + " or later, which " + what +
			" (paired: " + strings.Join(old, ", ") + ")"}
	case len(able) == 0:
		return &Waiting{Code: WaitNoEligibleWorker, Detail: "no worker runs the tools, model and fonts it needs"}
	case len(free) == 0:
		w := &Waiting{Code: WaitAllExcluded, Detail: "every worker that runs its tools failed it or had its result rejected"}
		if t := s.liftAt(j); t != nil {
			w.RetryAt = t
			w.Detail += "; the reference worker " + s.cfg.ReferenceWorker + " gets it again at retry_at"
			if s.cfg.ReferenceWorker == "" {
				w.Detail = "every worker that runs its tools failed it or had its result rejected; they get it again at retry_at"
			}
		}
		return w
	}
	for _, w := range free {
		if s.connected(w, now) && !s.paused(w, now) {
			if room := s.roomFor(j); j.Spec.Result.MaxBytes > room {
				return &Waiting{Code: WaitNoRoom, Detail: fmt.Sprintf("the queue's disk has room for %.1f GB of results "+
					"beside what running jobs may still deliver and its reserve; this one may take %.1f GB",
					math.Max(0, float64(room))/1e9, float64(j.Spec.Result.MaxBytes)/1e9)}
			}
			return nil
		}
	}
	return &Waiting{Code: WaitNoWorkerUp, Detail: "every worker that may run it is offline or paused"}
}

// liftAt is when an exclusion of the job ends: the reference worker's (or, without a
// reference, every worker's) after the backoff since the last failure; nil when none will.
func (s *Store) liftAt(j *Job) *time.Time {
	if j.FailedAt == nil || len(j.Excluded) == 0 {
		return nil
	}
	if ref := s.referenceWorker(); ref != nil && !contains(j.Excluded, ref.ID) {
		return nil
	} else if ref == nil && s.cfg.ReferenceWorker != "" {
		return nil
	}
	t := j.FailedAt.Add(s.cfg.ExcludeBackoff).UTC()
	return &t
}

// liftExclusions lets the reference worker (without one, every worker) back to a queued job
// that every worker able to run it is excluded from, once the backoff has passed. Failures
// still count, so a job that keeps failing ends as failed.
func (s *Store) liftExclusions(now time.Time) bool {
	changed := false
	for _, j := range s.st.Jobs {
		w := s.waiting(j, now)
		if w == nil || w.Code != WaitAllExcluded || w.RetryAt == nil || now.Before(*w.RetryAt) {
			continue
		}
		if ref := s.referenceWorker(); ref != nil {
			j.Excluded = remove(j.Excluded, ref.ID)
		} else {
			j.Excluded = nil
		}
		s.cfg.Log.Info("let back", "job", j.ID, "failures", j.Failures)
		s.bump(j)
		changed = true
	}
	return changed
}

func remove(list []string, x string) []string {
	var out []string
	for _, v := range list {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

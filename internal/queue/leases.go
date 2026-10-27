package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// LeaseRequest narrows a claim.
type LeaseRequest struct {
	Kinds          []string `json:"kinds,omitempty"`
	MaxSourceBytes int64    `json:"max_source_bytes,omitempty"`
	// Extra is the worker's extra slot (v0.2.5), on top of max_parallel_jobs: one lease at a time,
	// Sofort jobs only, and beside other work only while the worker's free RAM holds the job's
	// peak.
	Extra bool `json:"extra,omitempty"`
}

func (s *Store) activeLeases(workerID string) []*Lease {
	var out []*Lease
	for _, l := range s.st.Leases {
		if l.WorkerID == workerID && l.State == LeaseActive {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID // leases of one instant in one order, not the map's
	})
	return out
}

// room is how many bytes of results the queue can still take: free space less what active
// leases may still deliver and the reserve.
func (s *Store) room() int64 {
	free, err := s.cfg.FreeBytes(s.cfg.ResultDir)
	if err != nil {
		s.cfg.Log.Warn("free space of the result directory", "err", err)
		return 0
	}
	for _, l := range s.st.Leases {
		if l.State == LeaseActive {
			if j := s.st.Jobs[l.JobID]; j != nil {
				free -= j.Spec.Result.MaxBytes
			}
		}
	}
	// What uploads of ended leases may still write (an active lease's is counted above).
	for _, u := range s.st.Uploads {
		if l := s.st.Leases[u.LeaseID]; l == nil || l.State != LeaseActive {
			free -= u.Length - u.Offset
		}
	}
	return free - s.cfg.ResultReserve
}

// claimable is the first queued job this worker may run now, or nil, with the scoring that gave
// it (ect.go). `ramGate`: the claim is the extra slot's beside other work, so the job's peak must
// fit the worker's free RAM. A job sent to this worker by hand skips the scoring.
func (s *Store) claimable(w *Worker, req LeaseRequest, ramGate bool) (*Job, *ECT, *ECT) {
	accepted, _ := s.kinds(w)
	room := s.room()
	// The jobs the Encoder page sent to this worker first, then the rest; a job sent to
	// another worker is that worker's.
	var mine, rest []*Job
	for _, j := range s.queued() {
		switch {
		case j.Pin == nil:
			rest = append(rest, j)
		case j.Pin.WorkerID == w.ID:
			mine = append(mine, j)
		}
	}
	now := s.cfg.Now()
	for i, j := range append(mine, rest...) {
		if !contains(accepted, j.Spec.Kind) {
			continue
		}
		if len(req.Kinds) > 0 && !contains(req.Kinds, j.Spec.Kind) {
			continue
		}
		if req.MaxSourceBytes > 0 && j.Spec.Source.Bytes > req.MaxSourceBytes {
			continue
		}
		if j.Spec.Result.MaxBytes > room {
			continue
		}
		if s.mismatch(w, j) != "" {
			continue
		}
		if req.Extra && j.Priority != PriorityNow {
			continue
		}
		if ramGate && !s.ramFits(w, j) {
			continue
		}
		if i < len(mine) {
			e, _ := s.ect(w, j, true, now)
			return j, &e, nil
		}
		if s.betterModelElsewhere(w, j, now) {
			continue
		}
		take, e, runnerUp := s.verdict(w, j, now)
		if !take {
			continue // another worker would clearly finish it sooner
		}
		return j, e, runnerUp
	}
	return nil, nil, nil
}

// canClaim says why the worker may not claim now. Its regular slots count against
// max_parallel_jobs; `extra` is its extra slot, which holds one lease on top of them.
func (s *Store) canClaim(w *Worker, now time.Time, extra bool) error {
	if s.draining {
		return errf(503, "draining", "the queue hands out no new work now")
	}
	if w.Caps == nil {
		return errf(409, "no_capabilities", "say what this worker runs first (PUT /me/capabilities)")
	}
	if s.paused(w, now) {
		return errf(409, "paused", "paused until %s", w.PausedUntil.Format(time.RFC3339))
	}
	regular, extras := 0, 0
	for _, l := range s.activeLeases(w.ID) {
		if l.Extra {
			extras++
		} else {
			regular++
		}
	}
	if extra && extras > 0 {
		return errf(409, "limit_reached", "this worker's extra slot is taken")
	}
	if max := maxJobs(w); !extra && regular >= max {
		return errf(409, "limit_reached", "this worker holds %d leases, its limit", regular)
	}
	return nil
}

func maxJobs(w *Worker) int {
	if w.Limits.MaxParallelJobs < 1 {
		return 1
	}
	return w.Limits.MaxParallelJobs
}

// headroom is the RAM the extra slot leaves free beside a job's expected peak.
const headroom = 1 << 30

// peakBytes is what a job is expected to take at its peak (measured on encoder, 2026-10-27:
// a 4K `slower` encode peaks at 4.6 to 5.3 GB). Whisper medium is the large one beside an
// encode; a model the queue does not know counts as medium.
func peakBytes(j *Job) int64 {
	const gb = 1 << 30
	switch {
	case j.whisper != nil:
		if c, ok := catalogue[j.whisper.acceptable()[0]]; ok {
			return c.PeakBytes
		}
		m := strings.ToLower(j.whisper.Model)
		switch {
		case strings.Contains(m, "large"):
			return 4 * gb
		case strings.Contains(m, "tiny"), strings.Contains(m, "base"), strings.Contains(m, "small"):
			return 1 * gb
		}
		return 5 * gb / 2
	case j.rung != nil:
		h := min(j.rung.Width, j.rung.Height)
		slow := j.rung.Preset == "slower" || j.rung.Preset == "veryslow" || j.rung.Preset == "slow"
		switch {
		case h >= 2160 && slow:
			return 11 * gb / 2
		case h >= 2160:
			return 4 * gb
		case h >= 1440:
			return 3 * gb
		case h >= 1080:
			return 2 * gb
		}
		return 1 * gb
	}
	return 2 * gb
}

// ramFits says whether the worker's free RAM, by its last presence, holds the job's peak with
// the headroom. Unknown RAM never fits.
func (s *Store) ramFits(w *Worker, j *Job) bool {
	total := s.system(w).MemoryBytes
	if total == nil || w.MemoryUsed == nil {
		return false
	}
	peak := peakBytes(j)
	if _, m := modelOn(w, j); m != "" {
		if c, ok := catalogue[m]; ok {
			peak = c.PeakBytes // the model this worker would run
		}
	}
	return *total-*w.MemoryUsed-headroom >= peak
}

// Claim leases the next job this worker may run, waiting up to wait for one. It returns nil
// and no error when none came.
func (s *Store) Claim(ctx context.Context, workerID string, req LeaseRequest, wait time.Duration) (*LeaseView, error) {
	if wait > s.cfg.LongPoll {
		wait = s.cfg.LongPoll
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		s.mu.Lock()
		w, err := s.worker(workerID)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.seen(w)
		now := s.cfg.Now()
		if err := s.canClaim(w, now, req.Extra); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		ramGate := req.Extra && len(s.activeLeases(w.ID)) > 0
		if j, e, runnerUp := s.claimable(w, req, ramGate); j != nil {
			l := &Lease{ID: newLeaseID(now), JobID: j.ID, WorkerID: w.ID, Worker: w.Name,
				Attempt: len(j.Attempts) + 1, State: LeaseActive, CreatedAt: now, ExpiresAt: now.Add(s.cfg.Lease), Extra: req.Extra}
			_, l.Model = modelOn(w, j)
			l.ECT, l.RunnerUp = e, runnerUp
			if e != nil {
				l.EstRunS = e.RunS
			}
			s.st.Leases[l.ID] = l
			j.Attempts = append(j.Attempts, l.ID)
			j.Pin = nil
			s.bump(j)
			s.touch(true)
			v := s.leaseView(l)
			s.mu.Unlock()
			return &v, nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-timer.C:
			return nil, nil
		case <-ctx.Done():
			return nil, nil
		}
	}
}

// lease is a worker's own lease, or the error the helper API answers.
func (s *Store) lease(workerID, leaseID string) (*Lease, error) {
	l := s.st.Leases[leaseID]
	if l == nil {
		return nil, errf(410, "lease_cancelled", "no lease %s (its job was forgotten or cancelled)", leaseID)
	}
	if l.WorkerID != workerID {
		return nil, errf(403, "not_your_lease", "lease %s belongs to another worker", leaseID)
	}
	return l, nil
}

// goneErr is why an ended lease answers 410.
func (s *Store) goneErr(l *Lease) *Error {
	code := "lease_cancelled"
	switch {
	case l.State == LeaseExpired:
		code = "lease_expired"
	case l.State == LeasePreempted:
		code = "preempted"
	case l.EndCode == "job_done":
		code = "job_done"
	}
	return errf(410, code, "lease %s is %s (%s)", l.ID, l.State, l.EndCode)
}

// MyLeases is a worker's active leases.
func (s *Store) MyLeases(workerID string) ([]LeaseView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(workerID)
	if err != nil {
		return nil, err
	}
	s.seen(w)
	out := []LeaseView{}
	for _, l := range s.activeLeases(workerID) {
		out = append(out, s.leaseView(l))
	}
	return out, nil
}

// GetLease is one of the worker's leases.
func (s *Store) GetLease(workerID, leaseID string) (LeaseView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.worker(workerID); err != nil {
		return LeaseView{}, err
	}
	l, err := s.lease(workerID, leaseID)
	if err != nil {
		return LeaseView{}, err
	}
	if l.State != LeaseActive {
		return LeaseView{}, s.goneErr(l)
	}
	return s.leaseView(l), nil
}

// Renew stores a progress report and renews the lease.
func (s *Store) Renew(workerID, leaseID string, p Progress) (LeaseView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(workerID)
	if err != nil {
		return LeaseView{}, err
	}
	s.seen(w)
	l, err := s.lease(workerID, leaseID)
	if err != nil {
		return LeaseView{}, err
	}
	if l.State != LeaseActive {
		return LeaseView{}, s.goneErr(l)
	}
	switch p.Phase {
	case "downloading", "encoding", "uploading", "paused":
	default:
		return LeaseView{}, errf(422, "invalid", "phase %q unknown", p.Phase)
	}
	now := s.cfg.Now()
	s.noteLive(w, l.Progress, l.ExpiresAt.Add(-s.cfg.Lease), p)
	l.Progress = &p
	l.ExpiresAt = now.Add(s.cfg.Lease)
	if j := s.st.Jobs[l.JobID]; j != nil {
		s.bump(j)
	}
	s.touch(false)
	return s.leaseView(l), nil
}

// Release gives a job back.
func (s *Store) Release(workerID, leaseID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(workerID)
	if err != nil {
		return err
	}
	s.seen(w)
	switch reason {
	case "paused", "quit", "disk_full", "on_battery", "user", "too_slow", "restart":
	default:
		return errf(422, "invalid", "reason %q unknown", reason)
	}
	l, err := s.lease(workerID, leaseID)
	if err != nil {
		return err
	}
	if l.State != LeaseActive {
		return s.goneErr(l)
	}
	s.endLease(l, LeaseGivenBack, reason)
	s.touch(true)
	return nil
}

// LogLineIn is a log line from a worker.
type LogLineIn struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Source  string    `json:"source,omitempty"`
	Message string    `json:"message"`
}

// AppendLogs keeps a worker's log lines for its lease.
func (s *Store) AppendLogs(workerID, leaseID string, lines []LogLineIn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.worker(workerID); err != nil {
		return err
	}
	l, err := s.lease(workerID, leaseID)
	if err != nil {
		return err
	}
	if l.State != LeaseActive && !(l.State == LeaseDelivered || l.State == LeaseFailed) {
		return s.goneErr(l)
	}
	if len(lines) > 500 {
		return errf(413, "too_large", "at most 500 lines a batch")
	}
	r := s.logs[leaseID]
	if r == nil {
		r = &logRing{}
		s.logs[leaseID] = r
	}
	for _, in := range lines {
		level := in.Level
		switch level {
		case "debug", "info", "warning", "error":
		default:
			level = "info"
		}
		r.add(LogLine{Time: in.Time, Level: level, Source: oneLine(in.Source, 32), Message: truncate(in.Message, 2000)})
	}
	return nil
}

// Logs is a lease's lines after a sequence number.
func (s *Store) Logs(leaseID string, after int64) ([]LogLine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Leases[leaseID] == nil {
		return nil, errf(404, "not_found", "no lease %s", leaseID)
	}
	out := []LogLine{}
	if r := s.logs[leaseID]; r != nil {
		for _, l := range r.lines {
			if l.Seq > after {
				out = append(out, l)
			}
		}
	}
	return out, nil
}

// SourceFile is where a job's source or input is, for the worker that holds its lease.
func (s *Store) SourceFile(workerID, jobID, input string) (path, sha string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.worker(workerID); err != nil {
		return "", "", err
	}
	j := s.st.Jobs[jobID]
	if j == nil {
		return "", "", errf(404, "not_found", "no job %s", jobID)
	}
	// The worker's latest attempt at the job: active, or ended, which answers 410 so the
	// worker stops instead of reporting a failure.
	var mine *Lease
	for _, id := range j.Attempts {
		if l := s.st.Leases[id]; l != nil && l.WorkerID == workerID {
			mine = l
		}
	}
	if mine == nil {
		return "", "", errf(403, "not_your_lease", "this worker holds no lease on %s", jobID)
	}
	if mine.State != LeaseActive {
		return "", "", s.goneErr(mine)
	}
	f := j.Spec.Source.FileIn
	if input != "" {
		in, ok := j.Spec.Inputs[input]
		if !ok {
			return "", "", errf(404, "not_found", "%s has no input %s", jobID, input)
		}
		f = in
	}
	p, err := s.sourcePath(f.Path)
	if err != nil {
		return "", "", errf(404, "not_found", "%v", err)
	}
	return p, f.SHA256, nil
}

// deliverable is whether a lease may still deliver: an active one, and one that ran out or
// was taken back while its job is not finished (the late result).
func (s *Store) deliverable(l *Lease) error {
	j := s.st.Jobs[l.JobID]
	if j == nil {
		return errf(410, "lease_cancelled", "the job was forgotten")
	}
	switch {
	case j.Final == StateDone:
		return errf(410, "job_done", "another attempt of %s was accepted", j.ID)
	case j.Final != "":
		return errf(410, "lease_cancelled", "%s is %s", j.ID, j.Final)
	}
	if l.Result != nil {
		return errf(409, "result_exists", "lease %s delivered already", l.ID)
	}
	switch l.State {
	case LeaseActive, LeaseExpired, LeasePreempted:
		return nil
	case LeaseDelivered, LeaseFailed:
		return errf(409, "result_exists", "lease %s delivered already", l.ID)
	}
	return s.goneErr(l)
}

// ---------------------------------------------------------------- tus uploads

// CreateUpload starts a resumable upload for a lease's result.
func (s *Store) CreateUpload(workerID, leaseID string, length int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.worker(workerID); err != nil {
		return "", err
	}
	l, err := s.lease(workerID, leaseID)
	if err != nil {
		return "", err
	}
	if err := s.deliverable(l); err != nil {
		return "", err
	}
	if l.State != LeaseActive {
		return "", s.goneErr(l) // an upload starts while the lease is held; it may finish late
	}
	j := s.st.Jobs[l.JobID]
	if length < 1 || length > j.Spec.Result.MaxBytes {
		return "", errf(413, "too_large", "the result may have at most %d bytes", j.Spec.Result.MaxBytes)
	}
	// One upload per lease: a new one replaces the old.
	for uid, u := range s.st.Uploads {
		if u.LeaseID == leaseID && !u.busy {
			os.Remove(s.uploadPath(uid))
			delete(s.st.Uploads, uid)
		}
	}
	u := &Upload{ID: randomID("f_", 16), LeaseID: leaseID, Length: length, CreatedAt: s.cfg.Now()}
	f, err := os.Create(s.uploadPath(u.ID))
	if err != nil {
		return "", err
	}
	f.Close()
	s.st.Uploads[u.ID] = u
	s.touch(true)
	return u.ID, nil
}

func (s *Store) upload(workerID, id string) (*Upload, *Lease, error) {
	u := s.st.Uploads[id]
	if u == nil {
		return nil, nil, errf(404, "not_found", "no upload %s", id)
	}
	l := s.st.Leases[u.LeaseID]
	if l == nil {
		return nil, nil, errf(404, "not_found", "no upload %s", id)
	}
	if l.WorkerID != workerID {
		return nil, nil, errf(403, "not_your_lease", "upload %s belongs to another worker", id)
	}
	return u, l, nil
}

// UploadOffset is how much of an upload arrived.
func (s *Store) UploadOffset(workerID, id string) (offset, length int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, _, err := s.upload(workerID, id)
	if err != nil {
		return 0, 0, err
	}
	return u.Offset, u.Length, nil
}

// AppendUpload writes the next chunk at offset. The file is written outside the lock.
func (s *Store) AppendUpload(workerID, id string, offset int64, body io.Reader) (int64, error) {
	s.mu.Lock()
	u, l, err := s.upload(workerID, id)
	if err == nil {
		err = s.deliverable(l)
	}
	if err == nil && u.busy {
		err = errf(409, "offset_mismatch", "another chunk of %s is arriving", id)
	}
	if err == nil && offset != u.Offset {
		err = errf(409, "offset_mismatch", "the upload is at %d, not %d", u.Offset, offset)
	}
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	u.busy = true
	remaining := u.Length - u.Offset
	path := s.uploadPath(id)
	s.mu.Unlock()

	start := time.Now()
	f, ferr := os.OpenFile(path, os.O_WRONLY, 0)
	var n int64
	if ferr == nil {
		if _, ferr = f.Seek(offset, io.SeekStart); ferr == nil {
			n, ferr = io.Copy(f, io.LimitReader(body, remaining+1))
		}
		if cerr := f.Close(); ferr == nil {
			ferr = cerr
		}
	}
	tooLong := n > remaining
	s.mu.Lock()
	defer s.mu.Unlock()
	u.busy = false
	if tooLong {
		os.Truncate(path, u.Length)
		u.Offset = u.Length
		return 0, errf(413, "too_large", "more bytes than Upload-Length")
	}
	u.Offset += n
	u.LastWrite = s.cfg.Now()
	s.noteTransfer(workerID, DirUpload, n, time.Since(start))
	s.touch(false)
	if ferr != nil && n == 0 {
		return u.Offset, ferr
	}
	return u.Offset, nil
}

// DeleteUpload drops an unfinished upload.
func (s *Store) DeleteUpload(workerID, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, _, err := s.upload(workerID, id)
	if err != nil {
		return err
	}
	os.Remove(s.uploadPath(u.ID))
	delete(s.st.Uploads, u.ID)
	s.touch(true)
	return nil
}

// ---------------------------------------------------------------- results

// ResultIn is what a worker delivers.
type ResultIn struct {
	Outcome string  `json:"outcome"`
	File    string  `json:"file,omitempty"`
	SHA256  string  `json:"sha256,omitempty"`
	Frames  int64   `json:"frames,omitempty"`
	EncodeS float64 `json:"encode_s,omitempty"`
	Tools   []Tool  `json:"tools,omitempty"`
	Code    string  `json:"code,omitempty"`
	Message string  `json:"message,omitempty"`
	// How the tool ended, for a failure.
	Signal      string `json:"signal,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	MemoryAvail *int64 `json:"memory_available_bytes,omitempty"`
}

// Deliver takes a lease's result. A file's sha256 is checked outside the lock.
func (s *Store) Deliver(workerID, leaseID string, in ResultIn) (ResultView, error) {
	s.mu.Lock()
	w, err := s.worker(workerID)
	if err != nil {
		s.mu.Unlock()
		return ResultView{}, err
	}
	s.seen(w)
	l, err := s.lease(workerID, leaseID)
	if err == nil {
		err = s.deliverable(l)
	}
	if err != nil {
		s.mu.Unlock()
		return ResultView{}, err
	}
	now := s.cfg.Now()
	j := s.st.Jobs[l.JobID]
	if in.Outcome == "failed" {
		if in.Code == "" || in.Message == "" {
			s.mu.Unlock()
			return ResultView{}, errf(422, "invalid", "a failure needs code and message")
		}
		// Only a lease the worker still held counts as its failure: one that ran out or was
		// preempted failed for that reason, not for the worker's tools.
		wasActive := l.State == LeaseActive
		l.Result = &Result{Outcome: "failed", Verdict: VerdictFailed, FailureCode: in.Code, Message: oneLine(in.Message, 500),
			Signal: oneLine(in.Signal, 16), ExitCode: in.ExitCode, MemoryAvail: in.MemoryAvail, DeliveredAt: now, DecidedAt: ptrTime(now)}
		if wasActive {
			l.State = LeaseFailed
			l.EndedAt = ptrTime(now)
			w.Counts.Failed++
			// A tool the system killed (the OOM killer, mostly) says nothing about the worker's
			// results: it counts as a failure, but excludes the worker only the second time.
			exclude := true
			if infraFailure(in.Code) {
				if j.Killed == nil {
					j.Killed = map[string]int{}
				}
				j.Killed[w.ID]++
				exclude = j.Killed[w.ID] > 1
			}
			s.countFailure(j, w.ID, exclude)
		}
		s.bump(j)
		s.touch(true)
		v := s.resultView(l)
		s.mu.Unlock()
		return v, nil
	}
	if in.Outcome != "file" {
		s.mu.Unlock()
		return ResultView{}, errf(422, "invalid", "outcome is file or failed")
	}
	fid := in.File[strings.LastIndex(in.File, "/")+1:]
	u, ul, err := s.upload(workerID, fid)
	if err == nil && ul != l {
		err = errf(422, "invalid", "upload %s belongs to another lease", fid)
	}
	if err == nil && (u.busy || u.Offset != u.Length) {
		err = errf(422, "upload_incomplete", "upload %s has %d of %d bytes", fid, u.Offset, u.Length)
	}
	if err == nil && !sha256Re.MatchString(in.SHA256) {
		err = errf(422, "invalid", "sha256 must be 64 lowercase hex digits")
	}
	if err != nil {
		s.mu.Unlock()
		return ResultView{}, err
	}
	u.busy = true
	src := s.uploadPath(u.ID)
	s.mu.Unlock()

	sum, herr := fileSHA256(src)

	s.mu.Lock()
	defer s.mu.Unlock()
	u.busy = false
	if herr != nil {
		return ResultView{}, herr
	}
	if sum != in.SHA256 {
		return ResultView{}, errf(422, "checksum_mismatch", "the upload's sha256 is %s, not %s", sum, in.SHA256)
	}
	if err := s.deliverable(l); err != nil { // the job may have finished meanwhile
		return ResultView{}, err
	}
	rel := filepath.Join(j.ID, strconv.Itoa(l.Attempt), j.Spec.Result.Name)
	dst := filepath.Join(s.cfg.ResultDir, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return ResultView{}, err
	}
	if err := os.Rename(src, dst); err != nil {
		return ResultView{}, err
	}
	delete(s.st.Uploads, u.ID)
	if l.State == LeaseActive {
		l.State = LeaseDelivered
		l.EndedAt = ptrTime(now)
	} else {
		// A late result: the attempt ended already, and is delivered now.
		l.State = LeaseDelivered
	}
	l.Result = &Result{Outcome: "file", Verdict: VerdictChecking, SHA256: sum, Bytes: u.Length, Frames: in.Frames,
		EncodeS: in.EncodeS, Tools: in.Tools, Path: rel, DeliveredAt: s.cfg.Now()}
	s.learn(w, j, l, in.EncodeS)
	s.noteResult(w, j, l, in.Frames, in.EncodeS)
	s.bump(j)
	s.touch(true)
	return s.resultView(l), nil
}

// GetResult is a lease's result and its verdict.
func (s *Store) GetResult(workerID, leaseID string) (ResultView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.worker(workerID); err != nil {
		return ResultView{}, err
	}
	l := s.st.Leases[leaseID]
	if l == nil || l.Result == nil {
		return ResultView{}, errf(404, "not_found", "no result for %s", leaseID)
	}
	if l.WorkerID != workerID {
		return ResultView{}, errf(403, "not_your_lease", "lease %s belongs to another worker", leaseID)
	}
	return s.resultView(l), nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// oneLine is s on one line, at most n bytes.
func oneLine(s string, n int) string {
	return truncate(strings.Join(strings.Fields(s), " "), n)
}

// truncate cuts s to at most n bytes, at a character's start.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// dropStaleUploads removes the uploads of leases that ended without a result once nothing
// arrived for a lease's length, and every upload of a finished job.
func (s *Store) dropStaleUploads(now time.Time) bool {
	changed := false
	for uid, u := range s.st.Uploads {
		if u.busy {
			continue
		}
		l := s.st.Leases[u.LeaseID]
		var j *Job
		if l != nil {
			j = s.st.Jobs[l.JobID]
		}
		last := u.CreatedAt
		if u.LastWrite.After(last) {
			last = u.LastWrite
		}
		if l != nil && l.EndedAt != nil && l.EndedAt.After(last) {
			last = *l.EndedAt
		}
		stale := l == nil || j == nil || j.Final != "" || l.Result != nil ||
			(l.State != LeaseActive && (l.State == LeaseGivenBack || l.State == LeaseCancelled || now.Sub(last) > s.cfg.Lease))
		if stale {
			os.Remove(s.uploadPath(uid))
			delete(s.st.Uploads, uid)
			changed = true
		}
	}
	return changed
}

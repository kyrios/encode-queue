package queue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/kyrios/encode-queue/internal/aligner"
)

var (
	jobIDRe      = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}\.[a-z0-9-]{1,32}$`)
	resultNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	inputNameRe  = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
	sha256Re     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	presets      = []string{"ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"}
)

// state is the job's state, derived from its leases unless it is finished.
func (s *Store) jobState(j *Job) string {
	if j.Final != "" {
		return j.Final
	}
	leased := false
	for _, id := range j.Attempts {
		l := s.st.Leases[id]
		if l == nil {
			continue
		}
		if l.Result != nil && l.Result.Verdict == VerdictChecking {
			return StateChecking
		}
		if l.State == LeaseActive {
			leased = true
		}
	}
	if leased {
		return StateLeased
	}
	return StateQueued
}

func (s *Store) activeLease(j *Job) *Lease {
	for i := len(j.Attempts) - 1; i >= 0; i-- {
		if l := s.st.Leases[j.Attempts[i]]; l != nil && l.State == LeaseActive {
			return l
		}
	}
	return nil
}

func lessJob(a, b *Job) bool {
	if pa, pb := prioIndex(a.Priority), prioIndex(b.Priority); pa != pb {
		return pa < pb
	}
	if a.Rank != b.Rank {
		return a.Rank < b.Rank
	}
	if !a.QueuedAt.Equal(b.QueuedAt) {
		return a.QueuedAt.Before(b.QueuedAt)
	}
	return a.ID < b.ID
}

// queued is every queued job in the order the queue hands them out.
func (s *Store) queued() []*Job {
	var out []*Job
	for _, j := range s.st.Jobs {
		if s.jobState(j) == StateQueued {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, k int) bool { return lessJob(out[i], out[k]) })
	return out
}

func (s *Store) lastRank(prio string) float64 {
	r := 0.0
	for _, j := range s.st.Jobs {
		if j.Priority == prio && j.Rank > r {
			r = j.Rank
		}
	}
	return r
}

// reference is the reference worker's tools for a kind, and whether there is a reference at all.
func (s *Store) reference(kind string, needFFmpeg bool) ([]Tool, bool, bool) {
	if s.cfg.ReferenceWorker == "" {
		return nil, false, true
	}
	var ref *Worker
	for _, w := range s.st.Workers {
		if w.Name == s.cfg.ReferenceWorker {
			ref = w
			break
		}
	}
	if ref == nil || ref.Caps == nil {
		return nil, true, false
	}
	names := toolsByKind[kind]
	if needFFmpeg && !contains(names, "ffmpeg") {
		names = append([]string{"ffmpeg"}, names...)
	}
	var out []Tool
	for _, n := range names {
		if v, ok := ref.Caps.tool(n); ok {
			out = append(out, Tool{Name: n, Version: v})
		}
	}
	return out, true, true
}

// requires is what a worker must run for the job: the job's own requirement or the
// reference's tools, and the model and font its parameters name.
func (s *Store) requires(j *Job) (req Requires, onlyReference bool) {
	if j.Spec.Requires != nil {
		req = *j.Spec.Requires
	} else {
		tools, configured, known := s.reference(j.Spec.Kind, (j.whisper != nil || j.align != nil) && j.Spec.Source.Container != "wav")
		if configured && !known {
			onlyReference = true
		}
		req.Tools = tools
	}

	if j.rung != nil && j.rung.Captions != nil {
		if !contains(req.Fonts, j.rung.Captions.Font) {
			req.Fonts = append(append([]string(nil), req.Fonts...), j.rung.Captions.Font)
		}
		// Burnt-in captions need the reference's libass exactly, so every worker that takes one
		// renders the same subtitles (v0.2.7; toolchain 3 builds it on both platforms).
		if j.Spec.Requires == nil {
			if ref := s.referenceWorker(); ref != nil && ref.Caps != nil {
				if v, ok := ref.Caps.tool("libass"); ok {
					req.Tools = append(append([]Tool(nil), req.Tools...), Tool{Name: "libass", Version: v})
				}
			}
		}
	}
	return req, onlyReference
}

// mismatch says why a worker may not run a job, or "" when it may.
func (s *Store) mismatch(w *Worker, j *Job) string {
	if w.Caps != nil && contains(j.Excluded, w.ID) {
		return "its result for this job failed or was rejected"
	}
	return s.mismatchTools(w, j)
}

// mismatchTools is mismatch without the exclusions: whether the worker could run the job.
func (s *Store) mismatchTools(w *Worker, j *Job) string {
	if w.Caps == nil {
		return "no capabilities"
	}
	if why := tooOld(w, j); why != "" {
		return why
	}
	return s.mismatchRequires(w, j)
}

// tooOld says why the worker's version cannot make the job as asked, or "".
func tooOld(w *Worker, j *Job) string {
	v, _ := minVersion(j)
	switch {
	case v == "" || !versionLess(w.Caps.AppVersion, v):
		return ""
	case v == "0.2.8":
		return "worker " + w.Caps.AppVersion + " runs no cut, remux or check jobs and no check step (0.2.8)"
	case v == "0.2.16":
		return "worker " + w.Caps.AppVersion + " runs no align jobs (0.2.16)"
	case v == "0.2.22":
		return "worker " + w.Caps.AppVersion + " runs no peaks or strip jobs (0.2.22)"
	case v == "0.2.24":
		return "worker " + w.Caps.AppVersion + " runs no loudness jobs (0.2.24)"
	}
	// v0.2.13: an older worker would not refuse it, it would make the file without them.
	return "worker " + w.Caps.AppVersion + " would make the file without " + strings.Join(clipFields(j), ", ") + " (0.2.13)"
}

// mismatchRequires is mismatchTools without the version: the tools, model and fonts.
func (s *Store) mismatchRequires(w *Worker, j *Job) string {
	req, onlyRef := s.requires(j)
	if onlyRef && w.Name != s.cfg.ReferenceWorker {
		return "the reference worker has not said what it runs yet"
	}
	for _, t := range req.Tools {
		v, ok := w.Caps.tool(t.Name)
		if !ok {
			return fmt.Sprintf("%s missing", t.Name)
		}
		if v != t.Version {
			return fmt.Sprintf("%s differs: worker %s, required %s", t.Name, v, t.Version)
		}
	}
	for _, m := range req.Models {
		found := false
		for _, have := range w.Caps.Models {
			if have.SHA256 == m {
				found = true
			}
		}
		if !found {
			return "model " + m[:12] + " missing"
		}
	}
	for _, f := range req.Fonts {
		if !contains(w.Caps.Fonts, f) {
			return "font " + f + " missing"
		}
	}
	if j.whisper != nil {
		if _, m := modelOn(w, j); m == "" {
			return "model " + j.whisper.acceptable()[0][:12] + " missing"
		}
	}
	if j.align != nil {
		if alignEngine(w) == "" {
			return "no aligner engine (" + strings.Join(aligner.Engines, " or ") + ")"
		}
		if _, m := modelOn(w, j); m == "" {
			return "model " + modelLabel(j.align.ModelSHA256) + " missing"
		}
	}
	return ""
}

// alignEngine is the best aligner engine the worker runs (v0.2.16), or "".
func alignEngine(w *Worker) string {
	if w.Caps == nil {
		return ""
	}
	for _, e := range aligner.Engines {
		if _, ok := w.Caps.tool(e); ok {
			return e
		}
	}
	return ""
}

// modelOn is the best of the job's acceptable models the worker holds: its place in the list
// (0 is the best) and its sha256, or -1 and "". An align job has one model, and its place is
// the worker's engine's (v0.2.16): a CPU worker leaves the job to a Mac with MLX as a worker with
// a worse whisper model leaves a whisper job to one with a better (betterModelElsewhere).
func modelOn(w *Worker, j *Job) (int, string) {
	if w.Caps != nil && j.align != nil {
		rank := aligner.Rank(alignEngine(w))
		for _, have := range w.Caps.Models {
			if have.SHA256 == j.align.ModelSHA256 && rank >= 0 {
				return rank, have.SHA256
			}
		}
		return -1, ""
	}
	if w.Caps == nil || j.whisper == nil {
		return -1, ""
	}
	for i, m := range j.whisper.acceptable() {
		for _, have := range w.Caps.Models {
			if have.SHA256 == m {
				return i, m
			}
		}
	}
	return -1, ""
}

// betterModelElsewhere says whether another connected, unpaused worker that may run the job
// holds a better one of its models than w: then w leaves the job to it. A worse model runs
// only when no worker with a better one is there, or, for a job of priority now or stage1, when
// that one would first finish a lower job (waitsBehindLower).
func (s *Store) betterModelElsewhere(w *Worker, j *Job, now time.Time) bool {
	mine, _ := modelOn(w, j)
	if mine <= 0 {
		return false
	}
	for _, v := range s.st.Workers {
		if v == w || !s.claiming(v, now) || s.mismatch(v, j) != "" || s.waitsBehindLower(v, j, now) {
			continue
		}
		if i, _ := modelOn(v, j); i >= 0 && i < mine {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- submit

func (s *Store) validate(id string, spec *JobSubmit) error {
	if !jobIDRe.MatchString(id) {
		return errf(422, "invalid", "job id %q does not match %s", id, jobIDRe)
	}
	defaultMediaPriority(spec)
	if prioIndex(spec.Priority) == len(Priorities) {
		return errf(422, "invalid", "priority %q is not one of %v", spec.Priority, Priorities)
	}
	if spec.Stage != nil && *spec.Stage != 1 && *spec.Stage != 2 {
		return errf(422, "invalid", "stage is 1, 2 or null")
	}
	if !resultNameRe.MatchString(spec.Result.Name) || spec.Result.MaxBytes < 1 {
		return errf(422, "invalid", "result needs a name (%s) and max_bytes", resultNameRe)
	}
	if spec.MaxFailures < 0 {
		return errf(422, "invalid", "max_failures below 1")
	}
	j := &Job{Spec: *spec}
	if err := j.parse(); err != nil {
		return errf(422, "invalid", "params: %v", err)
	}
	if p := j.rung; p != nil {
		if p.Codec != "libx265" && p.Codec != "libx264" {
			return errf(422, "invalid", "codec is libx265 or libx264")
		}
		if !contains(presets, p.Preset) {
			return errf(422, "invalid", "preset %q unknown", p.Preset)
		}
		if p.Width < 2 || p.Height < 2 || p.Width%2 != 0 || p.Height%2 != 0 {
			return errf(422, "invalid", "width and height must be even and at least 2")
		}
		if p.Audio != nil && p.Audio.Mode != "copy" && p.Audio.Mode != "aac" && p.Audio.Mode != "none" {
			return errf(422, "invalid", "audio.mode is copy, aac or none")
		}
		if p.Audio != nil && p.Audio.Mode == "aac" && p.Audio.BitrateKbps < 1 {
			return errf(422, "invalid", "audio.mode aac needs bitrate_kbps")
		}
		if p.Captions != nil {
			if p.Codec != "libx264" {
				return errf(422, "invalid", "captions are burnt in with libx264 only")
			}
			if _, ok := spec.Inputs[p.Captions.Input]; !ok {
				return errf(422, "invalid", "captions.input %q is not among the inputs", p.Captions.Input)
			}
			if p.Captions.Font == "" {
				return errf(422, "invalid", "captions.font is empty")
			}
		}
		if err := validateClip(p); err != nil {
			return errf(422, "invalid", "params: %v", err)
		}
	}
	if err := validateKind(j, spec); err != nil {
		return errf(422, "invalid", "params: %v", err)
	}
	if p := j.whisper; p != nil {
		if p.Language == "" || (p.ModelSHA256 == "" && len(p.Models) == 0) {
			return errf(422, "invalid", "whisper needs model_sha256 or models, and language")
		}
		if p.ModelSHA256 != "" && !sha256Re.MatchString(p.ModelSHA256) {
			return errf(422, "invalid", "model_sha256 is not a sha256")
		}
		if len(p.Models) > 8 {
			return errf(422, "invalid", "at most 8 models")
		}
		for _, m := range p.Models {
			if !sha256Re.MatchString(m) {
				return errf(422, "invalid", "models: %q is not a sha256", m)
			}
		}
	}
	if p := j.align; p != nil {
		if !sha256Re.MatchString(p.ModelSHA256) {
			return errf(422, "invalid", "align needs model_sha256")
		}
		if _, ok := aligner.Languages[p.Language]; !ok {
			return errf(422, "invalid", "align: language %q is not one of the aligner's (%s)", p.Language, languages())
		}
		in, ok := spec.Inputs[p.input()]
		if !ok {
			return errf(422, "invalid", "align: the words, input %q, are not among the inputs", p.input())
		}
		if in.Bytes > aligner.MaxWordsBytes {
			return errf(422, "invalid", "align: the words weigh %d bytes, more than %d", in.Bytes, aligner.MaxWordsBytes)
		}
	}
	if err := s.checkFile("source", spec.Source.FileIn); err != nil {
		return err
	}
	for name, f := range spec.Inputs {
		if !inputNameRe.MatchString(name) {
			return errf(422, "invalid", "input name %q does not match %s", name, inputNameRe)
		}
		if err := s.checkFile("input "+name, f); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) checkFile(what string, f FileIn) error {
	if !sha256Re.MatchString(f.SHA256) {
		return errf(422, "invalid", "%s: sha256 must be 64 lowercase hex digits", what)
	}
	p, err := s.sourcePath(f.Path)
	if err != nil {
		return errf(422, "invalid", "%s: %v", what, err)
	}
	fi, err := os.Stat(p)
	if err != nil || !fi.Mode().IsRegular() {
		return errf(422, "source_missing", "%s: %s is not a file under the source directory", what, f.Path)
	}
	if fi.Size() != f.Bytes {
		return errf(422, "source_size", "%s: %s has %d bytes, not %d", what, f.Path, fi.Size(), f.Bytes)
	}
	return nil
}

func (s *Store) sourcePath(rel string) (string, error) {
	if rel == "" || filepath.IsAbs(rel) || strings.HasPrefix(filepath.ToSlash(rel), "../") || strings.Contains(filepath.ToSlash(rel), "/../") || rel == ".." {
		return "", errors.New("the path must be relative, inside the source directory")
	}
	return filepath.Join(s.cfg.SourceDir, filepath.Clean(rel)), nil
}

// languages are the aligner's language codes, sorted.
func languages() string {
	var out []string
	for c := range aligner.Languages {
		out = append(out, c)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// Submit puts a job. created is false when the same job was there already.
func (s *Store) Submit(id string, spec JobSubmit) (view JobView, created bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validate(id, &spec); err != nil {
		return view, false, err
	}
	h := hashSpec(spec)
	now := s.cfg.Now()
	if old := s.st.Jobs[id]; old != nil {
		if old.SpecHash == h && old.Final != StateCancelled && old.Final != StateFailed {
			return s.jobView(old), false, nil
		}
		if old.Final == "" {
			return view, false, errf(409, "job_exists", "%s is %s with other settings; cancel it first", id, s.jobState(old))
		}
		s.dropJob(old)
	}
	j := &Job{ID: id, Spec: spec, SpecHash: h, Priority: spec.Priority, QueuedAt: now, Rank: s.lastRank(spec.Priority) + 1}
	if spec.Order != nil {
		j.Rank = *spec.Order
	}
	j.parse()
	s.st.Jobs[id] = j
	s.bump(j)
	s.touch(true)
	return s.jobView(j), true, nil
}

// dropJob removes a job, its leases, uploads and files.
func (s *Store) dropJob(j *Job) {
	for _, lid := range j.Attempts {
		for uid, u := range s.st.Uploads {
			if u.LeaseID == lid {
				os.Remove(s.uploadPath(uid))
				delete(s.st.Uploads, uid)
			}
		}
		delete(s.st.Leases, lid)
		delete(s.logs, lid)
	}
	os.RemoveAll(filepath.Join(s.cfg.ResultDir, j.ID))
	delete(s.st.Jobs, j.ID)
	s.st.Revision++
	s.st.Forgotten = append(s.st.Forgotten, forgotten{JobID: j.ID, Revision: s.st.Revision})
	if n := len(s.st.Forgotten); n > 5000 {
		s.st.Forgotten = append([]forgotten(nil), s.st.Forgotten[n-5000:]...)
	}
}

// GetJob is one job.
func (s *Store) GetJob(id string) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	return s.jobView(j), nil
}

// Forget removes a finished job and its files.
func (s *Store) Forget(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return errf(404, "not_found", "no job %s", id)
	}
	if j.Final == "" {
		return errf(409, "not_finished", "%s is %s; cancel it first", id, s.jobState(j))
	}
	s.dropJob(j)
	s.touch(true)
	return nil
}

// Move is a reorder from the Encoder page.
type Move struct {
	Before   string `json:"before,omitempty"`
	After    string `json:"after,omitempty"`
	To       string `json:"to,omitempty"`
	Priority string `json:"priority,omitempty"`
}

// MoveJob puts a queued job in another place.
func (s *Store) MoveJob(id string, m Move) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	n := 0
	for _, v := range []string{m.Before, m.After, m.To} {
		if v != "" {
			n++
		}
	}
	if n != 1 {
		return JobView{}, errf(422, "invalid", "give exactly one of before, after, to")
	}
	if m.Priority != "" && prioIndex(m.Priority) == len(Priorities) {
		return JobView{}, errf(422, "invalid", "priority %q unknown", m.Priority)
	}
	if s.jobState(j) != StateQueued {
		return JobView{}, errf(409, "not_queued", "%s is %s; only queued jobs move", id, s.jobState(j))
	}
	prio := j.Priority
	var anchor *Job
	if m.Before != "" || m.After != "" {
		aid := m.Before + m.After
		anchor = s.st.Jobs[aid]
		if anchor == nil || s.jobState(anchor) != StateQueued {
			return JobView{}, errf(422, "invalid", "%s is not a queued job", aid)
		}
		if anchor == j {
			return s.jobView(j), nil
		}
		prio = anchor.Priority
	} else if m.Priority != "" {
		prio = m.Priority
	}
	// The queued jobs of that priority without j, in order; j goes in; ranks are 1, 2, 3...
	var list []*Job
	for _, q := range s.queued() {
		if q.Priority == prio && q != j {
			list = append(list, q)
		}
	}
	pos := len(list)
	switch {
	case m.To == "top":
		pos = 0
	case m.To == "bottom":
		pos = len(list)
	case m.To != "":
		return JobView{}, errf(422, "invalid", "to is top or bottom")
	default:
		for i, q := range list {
			if q == anchor {
				pos = i
				if m.After != "" {
					pos = i + 1
				}
			}
		}
	}
	list = append(list[:pos], append([]*Job{j}, list[pos:]...)...)
	// Jobs of this priority that are not queued keep ranks below and above, so number after
	// the highest rank any of them holds: only the queued order matters.
	base := 0.0
	for _, q := range s.st.Jobs {
		if q.Priority == prio && s.jobState(q) != StateQueued && q.Rank > base {
			base = q.Rank
		}
	}
	for i, q := range list {
		q.Rank = base + float64(i+1)
	}
	j.Priority = prio
	s.bump(j)
	s.touch(true)
	return s.jobView(j), nil
}

// Assign is a move to one worker from the Encoder page.
type Assign struct {
	WorkerID string `json:"worker_id"`
}

// AssignJob sends a queued or leased job to one worker: an attempt on another worker is
// preempted, and the job is pinned to the target until the target leases it.
func (s *Store) AssignJob(id string, a Assign) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	if a.WorkerID == "" {
		return JobView{}, errf(422, "invalid", "worker_id is required")
	}
	w := s.st.Workers[a.WorkerID]
	if w == nil {
		return JobView{}, errf(404, "not_found", "no worker %s", a.WorkerID)
	}
	st := s.jobState(j)
	if st != StateQueued && st != StateLeased {
		return JobView{}, errf(409, "not_movable", "%s is %s; only queued or leased jobs move to a worker", id, st)
	}
	active := s.activeLease(j)
	if active != nil && active.WorkerID == w.ID {
		return s.jobView(j), nil
	}
	if err := s.refusal(w, j, s.cfg.Now()); err != nil {
		if err.Code == "worker_mismatch" {
			err.Detail = fmt.Sprintf("%s may not run %s: %s", w.Name, id, err.Detail)
		}
		return JobView{}, err
	}
	if active != nil {
		s.cfg.Log.Info("assign", "lease", active.ID, "job", j.ID, "from", active.Worker, "to", w.Name)
		s.endLease(active, LeasePreempted, "assigned")
	}
	j.Pin = &Pin{WorkerID: w.ID, At: s.cfg.Now().UTC()}
	s.bump(j)
	s.touch(true)
	return s.jobView(j), nil
}

// UnassignJob ends a job's pin.
func (s *Store) UnassignJob(id string) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	if j.Pin != nil {
		j.Pin = nil
		s.bump(j)
		s.touch(true)
	}
	return s.jobView(j), nil
}

// Candidate is a worker as a target for a job (the move menu).
type Candidate struct {
	Worker     WorkerRef `json:"worker"`
	State      string    `json:"state"`
	Available  bool      `json:"available"`
	ReasonCode *string   `json:"reason_code"`
	Reason     *string   `json:"reason"`
	Current    bool      `json:"current"`
	Pinned     bool      `json:"pinned"`
	// Claiming: it asked for work lately or works (v0.2.10); one that does not counts in no
	// scoring, so the page says "rechnet nicht".
	Claiming      bool       `json:"claiming"`
	FreeAt        *time.Time `json:"free_at"`
	RelativeSpeed *float64   `json:"relative_speed"`
	EncodeS       *float64   `json:"encode_s"`
	FinishAt      *time.Time `json:"finish_at"`
}

// Candidates is the answer of GET /jobs/{id}/candidates.
type Candidates struct {
	ServerTime time.Time   `json:"server_time"`
	Items      []Candidate `json:"items"`
}

// refusal is why a worker may not take a job now, as AssignJob refuses it.
func (s *Store) refusal(w *Worker, j *Job, now time.Time) *Error {
	if !s.connected(w, now) {
		return errf(409, "worker_offline", "%s is offline", w.Name)
	}
	if s.paused(w, now) {
		return errf(409, "worker_paused", "%s is paused until %s", w.Name, w.PausedUntil.Format(time.RFC3339))
	}
	accepted, refused := s.kinds(w)
	if !contains(accepted, j.Spec.Kind) {
		reason := "it takes no " + j.Spec.Kind + " jobs"
		for _, r := range refused {
			if r.Kind == j.Spec.Kind {
				reason = r.Reason
			}
		}
		return errf(409, "worker_mismatch", "%s", reason)
	}
	if why := s.mismatch(w, j); why != "" {
		return errf(409, "worker_mismatch", "%s", why)
	}
	return nil
}

// JobCandidates is every worker as a target for a job.
func (s *Store) JobCandidates(id string) (Candidates, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return Candidates{}, errf(404, "not_found", "no job %s", id)
	}
	now := s.cfg.Now()
	out := Candidates{ServerTime: now.UTC(), Items: []Candidate{}}
	var ws []*Worker
	for _, w := range s.st.Workers {
		ws = append(ws, w)
	}
	sort.Slice(ws, func(a, b int) bool { return ws[a].Name < ws[b].Name })
	active := s.activeLease(j)
	st := s.jobState(j)
	movable := st == StateQueued || st == StateLeased
	ut := func(t time.Time) *time.Time {
		t = t.UTC().Truncate(time.Second)
		return &t
	}
	for _, w := range ws {
		c := Candidate{Worker: WorkerRef{ID: w.ID, Name: w.Name}, State: s.workerState(w, now),
			Pinned: j.Pin != nil && j.Pin.WorkerID == w.ID, Claiming: s.claiming(w, now)}
		if j.rung != nil {
			c.RelativeSpeed = s.classRelative(w, rungClass(j.rung))
		}
		if c.RelativeSpeed == nil {
			c.RelativeSpeed = s.relativeSpeed(w)
		}
		if active != nil && active.WorkerID == w.ID {
			c.Current, c.Available = true, true
			if e := eta(j, active.Progress); e != nil {
				c.FinishAt = ut(now.Add(time.Duration(*e) * time.Second))
			}
			c.FreeAt = ut(now)
			out.Items = append(out.Items, c)
			continue
		}
		// Every candidate that is not available says why.
		if !movable {
			code, why := "not_movable", fmt.Sprintf("%s is %s; only queued or leased jobs move to a worker", j.ID, st)
			c.ReasonCode, c.Reason = &code, &why
			out.Items = append(out.Items, c)
			continue
		}
		if err := s.refusal(w, j, now); err != nil {
			c.ReasonCode, c.Reason = &err.Code, &err.Detail
			out.Items = append(out.Items, c)
			continue
		}
		c.Available = true
		if f := s.freeAt(w, now, j.ID, highPriority(j.Priority)); f != nil {
			c.FreeAt = ut(*f)
			if d := s.encodeSeconds(w, j); d > 0 {
				c.EncodeS = &d
				c.FinishAt = ut(f.Add(time.Duration(d * float64(time.Second))))
			}
		}
		out.Items = append(out.Items, c)
	}
	return out, nil
}

// endLease ends an active lease without a result and counts it against its worker.
func (s *Store) endLease(l *Lease, state, code string) {
	now := s.cfg.Now()
	l.State = state
	l.EndCode = code
	l.EndedAt = ptrTime(now)
	if w := s.st.Workers[l.WorkerID]; w != nil {
		switch state {
		case LeaseGivenBack:
			w.Counts.GivenBack++
		case LeaseExpired:
			w.Counts.Expired++
		case LeasePreempted:
			w.Counts.Preempted++
		case LeaseCancelled:
			w.Counts.Cancelled++
		}
	}
	if j := s.st.Jobs[l.JobID]; j != nil {
		s.bump(j)
	}
}

// supersede drops a waiting result: its file goes, its verdict is superseded.
func (s *Store) supersede(l *Lease) {
	now := s.cfg.Now()
	s.removeResultFile(l)
	l.Result.Verdict = VerdictSuperseded
	l.Result.DecidedAt = ptrTime(now)
	if w := s.st.Workers[l.WorkerID]; w != nil {
		w.Counts.Superseded++
	}
}

func (s *Store) removeResultFile(l *Lease) {
	if l.Result != nil && l.Result.Path != "" {
		os.Remove(filepath.Join(s.cfg.ResultDir, l.Result.Path))
		os.Remove(filepath.Dir(filepath.Join(s.cfg.ResultDir, l.Result.Path)))
		l.Result.Path = ""
	}
}

// CancelJob cancels a job that is not finished.
func (s *Store) CancelJob(id string) (JobView, error) { return s.CancelJobBy(id, "admin", "") }

// Cancelled is who cancelled a job, why and when; Code says the queue did, and for what
// (check_timeout, v0.2.26).
type Cancelled struct {
	By     string    `json:"by"`
	Reason *string   `json:"reason"`
	At     time.Time `json:"at"`
	Code   string    `json:"code,omitempty"`
}

// Why the queue itself cancels a job.
const CancelCheckTimeout = "check_timeout"

// CancelJobBy cancels a job that is not finished and notes who did and why.
func (s *Store) CancelJobBy(id, by, reason string) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	if j.Final != "" {
		return JobView{}, errf(409, "finished", "%s is %s already", id, j.Final)
	}
	s.cancel(j, &Cancelled{By: oneLine(by, 64), Reason: strp(oneLine(reason, 200)), At: s.cfg.Now().UTC()})
	return s.jobView(j), nil
}

// cancel ends a job: its active leases, and its results waiting for a verdict, whose files go.
func (s *Store) cancel(j *Job, c *Cancelled) {
	for _, lid := range j.Attempts {
		l := s.st.Leases[lid]
		if l == nil {
			continue
		}
		if l.State == LeaseActive {
			s.endLease(l, LeaseCancelled, "job_cancelled")
		}
		if l.Result != nil && l.Result.Verdict == VerdictChecking {
			s.supersede(l)
		}
	}
	j.Final = StateCancelled
	j.FinishedAt = ptrTime(s.cfg.Now())
	j.Pin = nil
	j.Cancelled = c
	s.bump(j)
	s.touch(true)
}

// **A result does not wait for its verdict forever** (v0.2.26; 2026-10-29: three strip jobs sat
// in checking from 23:16 on, after the pipeline restarted and lost the threads that waited for
// them). A job whose last result has waited CheckTimeout for its verdict is cancelled by the
// queue with the code check_timeout ("verfallen" on Funkhaus, under Abgebrochen): its result files
// go, so their room is free, and the submitter may submit it again under its id.
func (s *Store) expireChecks(now time.Time) bool {
	changed := false
	for _, j := range s.st.Jobs {
		if s.jobState(j) != StateChecking {
			continue
		}
		var last time.Time
		for _, lid := range j.Attempts {
			if l := s.st.Leases[lid]; l != nil && l.Result != nil && l.Result.Verdict == VerdictChecking && l.Result.DeliveredAt.After(last) {
				last = l.Result.DeliveredAt
			}
		}
		if last.IsZero() || now.Sub(last) < s.cfg.CheckTimeout {
			continue
		}
		reason := fmt.Sprintf("no verdict from the submitter within %s of the result", s.cfg.CheckTimeout)
		s.cfg.Log.Info("check timeout", "job", j.ID, "delivered_at", last.UTC().Format(time.RFC3339), "waited", now.Sub(last).Round(time.Second).String())
		s.cancel(j, &Cancelled{By: "queue", Reason: &reason, At: now.UTC(), Code: CancelCheckTimeout})
		s.st.CheckTimeouts++
		changed = true
	}
	return changed
}

// RetryJob queues a failed or cancelled job again.
func (s *Store) RetryJob(id string) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	if j.Final != StateFailed && j.Final != StateCancelled {
		return JobView{}, errf(409, "not_finished", "%s is %s; only failed or cancelled jobs are retried", id, s.jobState(j))
	}
	j.Final = ""
	j.FinishedAt = nil
	j.Cancelled = nil
	j.Failures = 0
	j.Excluded = nil
	j.Killed = nil
	j.QueuedAt = s.cfg.Now()
	s.bump(j)
	s.touch(true)
	return s.jobView(j), nil
}

// Verdict is the pipeline's judgement of a result.
type Verdict struct {
	Verdict string  `json:"verdict"`
	Reason  string  `json:"reason,omitempty"`
	Checks  []Check `json:"checks,omitempty"`
}

// PutVerdict accepts or rejects an attempt's result.
func (s *Store) PutVerdict(id string, attempt int, v Verdict) (JobView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil {
		return JobView{}, errf(404, "not_found", "no job %s", id)
	}
	if v.Verdict != VerdictAccepted && v.Verdict != VerdictRejected {
		return JobView{}, errf(422, "invalid", "verdict is accepted or rejected")
	}
	if attempt < 1 || attempt > len(j.Attempts) {
		return JobView{}, errf(404, "not_found", "%s has no attempt %d", id, attempt)
	}
	l := s.st.Leases[j.Attempts[attempt-1]]
	if l == nil || l.Result == nil || l.Result.Verdict != VerdictChecking {
		return JobView{}, errf(409, "not_checking", "attempt %d of %s has no result waiting", attempt, id)
	}
	now := s.cfg.Now()
	w := s.st.Workers[l.WorkerID]
	if j.Final == StateDone {
		s.supersede(l)
		s.bump(j)
		s.touch(true)
		return JobView{}, errf(409, "job_done", "attempt %d of %s was accepted already", j.Accepted, id)
	}
	l.Result.Checks = v.Checks
	l.Result.DecidedAt = ptrTime(now)
	if v.Verdict == VerdictAccepted {
		l.Result.Verdict = VerdictAccepted
		l.Result.Message = v.Reason
		if w != nil {
			w.Counts.Accepted++
		}
		s.award(w, j, l)
		j.Final = StateDone
		j.Accepted = attempt
		j.FinishedAt = ptrTime(now)
		for _, oid := range j.Attempts {
			o := s.st.Leases[oid]
			if o == nil || o == l {
				continue
			}
			if o.State == LeaseActive {
				s.endLease(o, LeaseCancelled, "job_done")
			}
			if o.Result != nil && o.Result.Verdict == VerdictChecking {
				s.supersede(o)
			}
		}
	} else {
		s.removeResultFile(l)
		l.Result.Verdict = VerdictRejected
		l.Result.Message = v.Reason
		if w != nil {
			w.Counts.Rejected++
		}
		s.countFailure(j, l.WorkerID, true)
	}
	s.bump(j)
	s.touch(true)
	return s.jobView(j), nil
}

// countFailure counts a rejection or failure: the worker gets the job no more, and the job
// fails once it reached its limit and nothing else is under way.
func (s *Store) countFailure(j *Job, workerID string, exclude bool) {
	j.Failures++
	j.FailedAt = ptrTime(s.cfg.Now())
	if exclude && !contains(j.Excluded, workerID) {
		j.Excluded = append(j.Excluded, workerID)
	}
	max := j.Spec.MaxFailures
	if max == 0 {
		max = s.cfg.MaxFailures
	}
	if j.Failures >= max {
		if st := s.jobState(j); st == StateQueued {
			j.Final = StateFailed
			j.FinishedAt = ptrTime(s.cfg.Now())
		}
	}
}

// ResultFile is where an attempt's file is, and its sha256.
func (s *Store) ResultFile(id string, attempt int) (path, sha string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.Jobs[id]
	if j == nil || attempt < 1 || attempt > len(j.Attempts) {
		return "", "", errf(404, "not_found", "no attempt %d of %s", attempt, id)
	}
	l := s.st.Leases[j.Attempts[attempt-1]]
	if l == nil || l.Result == nil || l.Result.Path == "" {
		return "", "", errf(404, "not_found", "attempt %d of %s has no file", attempt, id)
	}
	return filepath.Join(s.cfg.ResultDir, l.Result.Path), l.Result.SHA256, nil
}

// Cleanup is a removal of finished jobs from the Encoder page.
type Cleanup struct {
	States     []string `json:"states"`
	OlderThanS int64    `json:"older_than_s,omitempty"`
}

// CleanupResult is what a cleanup removed.
type CleanupResult struct {
	Removed  int      `json:"removed"`
	JobIDs   []string `json:"job_ids"`
	Revision int64    `json:"revision"`
}

// CleanupJobs removes the finished jobs in the given states, as Forget does each.
func (s *Store) CleanupJobs(c Cleanup) (CleanupResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(c.States) == 0 {
		return CleanupResult{}, errf(422, "invalid", "states is empty")
	}
	for _, st := range c.States {
		if st != StateDone && st != StateCancelled && st != StateFailed {
			return CleanupResult{}, errf(422, "invalid", "state %q: only done, cancelled and failed jobs are removed", st)
		}
	}
	if c.OlderThanS < 0 {
		return CleanupResult{}, errf(422, "invalid", "older_than_s below 0")
	}
	now := s.cfg.Now()
	out := CleanupResult{JobIDs: []string{}}
	for _, j := range s.st.Jobs {
		if !contains(c.States, j.Final) {
			continue
		}
		if c.OlderThanS > 0 && (j.FinishedAt == nil || now.Sub(*j.FinishedAt) < time.Duration(c.OlderThanS)*time.Second) {
			continue
		}
		out.JobIDs = append(out.JobIDs, j.ID)
	}
	sort.Strings(out.JobIDs)
	for _, id := range out.JobIDs {
		s.dropJob(s.st.Jobs[id])
	}
	out.Removed = len(out.JobIDs)
	if out.Removed > 0 {
		s.cfg.Log.Info("cleanup", "states", c.States, "removed", out.Removed)
		s.touch(true)
	}
	out.Revision = s.st.Revision
	return out, nil
}

// purgeDone forgets finished jobs older than DoneKeep (EQ_JOB_RETENTION).
func (s *Store) purgeDone(now time.Time) bool {
	changed := false
	for _, j := range s.st.Jobs {
		if j.Final != "" && j.FinishedAt != nil && now.Sub(*j.FinishedAt) > s.cfg.DoneKeep {
			s.dropJob(j)
			changed = true
		}
	}
	return changed
}

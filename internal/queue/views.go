package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// WorkerRef names a worker.
type WorkerRef struct {
	ID   string `json:"worker_id"`
	Name string `json:"name"`
}

// AttemptResult is what an attempt delivered, as the pipeline sees it.
type AttemptResult struct {
	Outcome     string     `json:"outcome"`
	Verdict     string     `json:"verdict"`
	SHA256      string     `json:"sha256,omitempty"`
	Bytes       int64      `json:"bytes,omitempty"`
	Frames      int64      `json:"frames,omitempty"`
	EncodeS     float64    `json:"encode_s,omitempty"`
	Tools       []Tool     `json:"tools,omitempty"`
	Path        *string    `json:"path"`
	FileURL     *string    `json:"file_url"`
	FailureCode *string    `json:"failure_code"`
	Message     *string    `json:"message"`
	Checks      []Check    `json:"checks,omitempty"`
	DeliveredAt time.Time  `json:"delivered_at"`
	DecidedAt   *time.Time `json:"decided_at"`
}

// AttemptView is one attempt.
type AttemptView struct {
	Attempt   int            `json:"attempt"`
	LeaseID   string         `json:"lease_id"`
	Worker    WorkerRef      `json:"worker"`
	State     string         `json:"state"`
	StartedAt time.Time      `json:"started_at"`
	ExpiresAt *time.Time     `json:"expires_at"`
	EndedAt   *time.Time     `json:"ended_at"`
	EndCode   *string        `json:"end_code"`
	Progress  *Progress      `json:"progress"`
	Result    *AttemptResult `json:"result"`
}

// JobView is a job as pipeline-admin.yaml shows it.
type JobView struct {
	ID              string            `json:"job_id"`
	UploadID        *string           `json:"upload_id"`
	Title           *string           `json:"title"`
	Kind            string            `json:"kind"`
	Stage           *int              `json:"stage"`
	Priority        string            `json:"priority"`
	Position        *int              `json:"position"`
	State           string            `json:"state"`
	Rung            *string           `json:"rung"`
	Resolution      *string           `json:"resolution"`
	Quality         *string           `json:"quality"`
	Worker          *WorkerRef        `json:"worker"`
	Progress        *Progress         `json:"progress"`
	Params          json.RawMessage   `json:"params"`
	Source          SourceIn          `json:"source"`
	Inputs          map[string]FileIn `json:"inputs,omitempty"`
	ResultSpec      ResultSpec        `json:"result_spec"`
	Requires        Requires          `json:"requires"`
	Failures        int               `json:"failures"`
	MaxFailures     int               `json:"max_failures"`
	ExcludedWorkers []WorkerRef       `json:"excluded_workers"`
	AcceptedAttempt *int              `json:"accepted_attempt"`
	Attempts        []AttemptView     `json:"attempts"`
	QueuedAt        time.Time         `json:"queued_at"`
	StartedAt       *time.Time        `json:"started_at"`
	FinishedAt      *time.Time        `json:"finished_at"`
	ElapsedS        *float64          `json:"elapsed_s"`
	Revision        int64             `json:"revision"`
}

func strp(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (s *Store) workerRef(id, fallback string) WorkerRef {
	if w := s.st.Workers[id]; w != nil {
		return WorkerRef{ID: id, Name: w.Name}
	}
	return WorkerRef{ID: id, Name: fallback}
}

func (s *Store) attemptView(j *Job, l *Lease) AttemptView {
	a := AttemptView{Attempt: l.Attempt, LeaseID: l.ID, Worker: s.workerRef(l.WorkerID, l.Worker), State: l.State,
		StartedAt: l.CreatedAt, EndedAt: l.EndedAt, EndCode: strp(l.EndCode)}
	if l.State == LeaseActive {
		e := l.ExpiresAt
		a.ExpiresAt = &e
		a.Progress = l.Progress
	}
	if r := l.Result; r != nil {
		ar := &AttemptResult{Outcome: r.Outcome, Verdict: r.Verdict, SHA256: r.SHA256, Bytes: r.Bytes, Frames: r.Frames,
			EncodeS: r.EncodeS, Tools: r.Tools, FailureCode: strp(r.FailureCode), Message: strp(r.Message),
			Checks: r.Checks, DeliveredAt: r.DeliveredAt, DecidedAt: r.DecidedAt}
		if r.Path != "" {
			p := r.Path
			u := fmt.Sprintf("/v1/jobs/%s/attempts/%d/file", j.ID, l.Attempt)
			ar.Path, ar.FileURL = &p, &u
		}
		a.Result = ar
	}
	return a
}

func (s *Store) jobView(j *Job) JobView {
	st := s.jobState(j)
	v := JobView{ID: j.ID, UploadID: strp(j.Spec.UploadID), Title: strp(j.Spec.Title), Kind: j.Spec.Kind, Stage: j.Spec.Stage,
		Priority: j.Priority, State: st, Params: j.Spec.Params, Source: j.Spec.Source, Inputs: j.Spec.Inputs,
		ResultSpec: j.Spec.Result, Failures: j.Failures, QueuedAt: j.QueuedAt, FinishedAt: j.FinishedAt,
		Revision: j.Revision, Attempts: []AttemptView{}, ExcludedWorkers: []WorkerRef{}}
	v.Requires, _ = s.requires(j)
	v.MaxFailures = j.Spec.MaxFailures
	if v.MaxFailures == 0 {
		v.MaxFailures = s.cfg.MaxFailures
	}
	if r := j.rung; r != nil {
		v.Rung = strp(r.Rung)
		v.Resolution = strp(fmt.Sprintf("%dx%d", r.Width, r.Height))
		v.Quality = strp(fmt.Sprintf("%s CRF %s", r.Preset, strconv.FormatFloat(r.CRF, 'f', -1, 64)))
	}
	if w := j.whisper; w != nil {
		q := w.Model
		if q == "" {
			q = w.ModelSHA256[:12]
		}
		v.Quality = strp(q)
	}
	for _, id := range j.Excluded {
		v.ExcludedWorkers = append(v.ExcludedWorkers, s.workerRef(id, ""))
	}
	if j.Accepted > 0 {
		a := j.Accepted
		v.AcceptedAttempt = &a
	}
	var shown *Lease
	for _, id := range j.Attempts {
		l := s.st.Leases[id]
		if l == nil {
			continue
		}
		v.Attempts = append(v.Attempts, s.attemptView(j, l))
		if l.State == LeaseActive || (j.Accepted > 0 && l.Attempt == j.Accepted) {
			shown = l
		}
	}
	if shown != nil {
		ref := s.workerRef(shown.WorkerID, shown.Worker)
		v.Worker = &ref
		start := shown.CreatedAt
		v.StartedAt = &start
		end := s.cfg.Now()
		if shown.State == LeaseActive {
			v.Progress = shown.Progress
		} else if shown.EndedAt != nil {
			end = *shown.EndedAt
		}
		e := end.Sub(start).Seconds()
		v.ElapsedS = &e
	}
	return v
}

// LeaseJob is the job as its worker needs it.
type LeaseJob struct {
	ID       string             `json:"job_id"`
	UploadID string             `json:"upload_id,omitempty"`
	Title    string             `json:"title,omitempty"`
	Kind     string             `json:"kind"`
	Priority string             `json:"priority"`
	Source   LeaseSource        `json:"source"`
	Inputs   map[string]FileRef `json:"inputs,omitempty"`
}

// FileRef is a file the worker fetches.
type FileRef struct {
	URL    string `json:"url"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// LeaseSource is the source as its worker sees it.
type LeaseSource struct {
	FileRef
	Container string  `json:"container,omitempty"`
	DurationS float64 `json:"duration_s,omitempty"`
	Frames    int64   `json:"frames,omitempty"`
	Width     int     `json:"width,omitempty"`
	Height    int     `json:"height,omitempty"`
}

// LeaseView is a lease as helper.yaml shows it.
type LeaseView struct {
	ID             string            `json:"lease_id"`
	Attempt        int               `json:"attempt"`
	Job            LeaseJob          `json:"job"`
	State          string            `json:"state"`
	CreatedAt      time.Time         `json:"created_at"`
	ExpiresAt      time.Time         `json:"expires_at"`
	RenewEveryS    int               `json:"renew_every_s"`
	PauseGiveBackS int               `json:"pause_give_back_s"`
	PausedUntil    *time.Time        `json:"paused_until"`
	Params         json.RawMessage   `json:"params"`
	Result         ResultSpec        `json:"result"`
	Progress       *Progress         `json:"progress"`
	Links          map[string]string `json:"links"`
}

func (s *Store) leaseView(l *Lease) LeaseView {
	j := s.st.Jobs[l.JobID]
	v := LeaseView{ID: l.ID, Attempt: l.Attempt, State: l.State, CreatedAt: l.CreatedAt, ExpiresAt: l.ExpiresAt,
		RenewEveryS: int(s.cfg.RenewEvery / time.Second), PauseGiveBackS: int(s.cfg.PauseGiveBack / time.Second),
		Progress: l.Progress}
	if w := s.st.Workers[l.WorkerID]; w != nil && s.paused(w, s.cfg.Now()) {
		v.PausedUntil = w.PausedUntil
	}
	if j != nil {
		src := j.Spec.Source
		v.Job = LeaseJob{ID: j.ID, UploadID: j.Spec.UploadID, Title: j.Spec.Title, Kind: j.Spec.Kind, Priority: j.Priority,
			Source: LeaseSource{FileRef: FileRef{URL: "/jobs/" + j.ID + "/source", Bytes: src.Bytes, SHA256: src.SHA256},
				Container: src.Container, DurationS: src.DurationS, Frames: src.Frames, Width: src.Width, Height: src.Height}}
		for name, in := range j.Spec.Inputs {
			if v.Job.Inputs == nil {
				v.Job.Inputs = map[string]FileRef{}
			}
			v.Job.Inputs[name] = FileRef{URL: "/jobs/" + j.ID + "/inputs/" + name, Bytes: in.Bytes, SHA256: in.SHA256}
		}
		v.Params = j.Spec.Params
		v.Result = j.Spec.Result
	}
	v.Links = map[string]string{
		"self":   "/leases/" + l.ID,
		"source": v.Job.Source.URL,
		"logs":   "/leases/" + l.ID + "/logs",
		"result": "/leases/" + l.ID + "/result",
	}
	return v
}

// ResultView is a result as helper.yaml shows it.
type ResultView struct {
	LeaseID   string     `json:"lease_id"`
	Attempt   int        `json:"attempt"`
	Outcome   string     `json:"outcome"`
	Verdict   string     `json:"verdict"`
	Reason    *string    `json:"reason"`
	Checks    []Check    `json:"checks,omitempty"`
	DecidedAt *time.Time `json:"decided_at"`
}

func (s *Store) resultView(l *Lease) ResultView {
	r := l.Result
	return ResultView{LeaseID: l.ID, Attempt: l.Attempt, Outcome: r.Outcome, Verdict: r.Verdict, Reason: strp(r.Message),
		Checks: r.Checks, DecidedAt: r.DecidedAt}
}

// MeView is the calling worker.
type MeView struct {
	ID          string     `json:"worker_id"`
	Name        string     `json:"name"`
	Owner       string     `json:"owner,omitempty"`
	State       string     `json:"state"`
	PausedUntil *time.Time `json:"paused_until"`
	Limits      Limits     `json:"limits"`
	Address     string     `json:"tunnel_address"`
	AppVersion  *string    `json:"app_version"`
	LastSeen    *time.Time `json:"last_seen"`
	Leases      []string   `json:"leases"`
	Counts      Counts     `json:"counts"`
}

func (s *Store) workerState(w *Worker, now time.Time) string {
	switch {
	case !s.connected(w, now):
		return "offline"
	case s.paused(w, now):
		return "paused"
	case len(s.activeLeases(w.ID)) > 0:
		return "working"
	}
	return "idle"
}

func (s *Store) meView(w *Worker) MeView {
	now := s.cfg.Now()
	v := MeView{ID: w.ID, Name: w.Name, Owner: w.Owner, State: s.workerState(w, now), Limits: w.Limits,
		Address: w.Address, LastSeen: w.LastSeen, Leases: []string{}, Counts: w.Counts}
	if s.paused(w, now) {
		v.PausedUntil = w.PausedUntil
	}
	if w.Caps != nil {
		v.AppVersion = &w.Caps.AppVersion
	}
	for _, l := range s.activeLeases(w.ID) {
		v.Leases = append(v.Leases, l.ID)
	}
	return v
}

// WorkerJob is a worker's active attempt, for the Encoder table.
type WorkerJob struct {
	JobID    string   `json:"job_id"`
	Title    *string  `json:"title"`
	Rung     *string  `json:"rung"`
	LeaseID  string   `json:"lease_id"`
	Attempt  int      `json:"attempt"`
	Phase    *string  `json:"phase"`
	Fraction *float64 `json:"fraction"`
	FPS      *float64 `json:"fps"`
	Speed    *float64 `json:"speed"`
	ETAS     *int64   `json:"eta_s"`
}

// WorkerView is a worker as pipeline-admin.yaml shows it.
type WorkerView struct {
	ID            string          `json:"worker_id"`
	Name          string          `json:"name"`
	Owner         *string         `json:"owner"`
	State         string          `json:"state"`
	PausedUntil   *time.Time      `json:"paused_until"`
	PublicKey     string          `json:"public_key"`
	Address       string          `json:"tunnel_address"`
	AppVersion    *string         `json:"app_version"`
	OS            *string         `json:"os"`
	Tools         []Tool          `json:"tools"`
	Models        []Model         `json:"models"`
	Fonts         []string        `json:"fonts"`
	Hardware      json.RawMessage `json:"hardware"`
	Limits        Limits          `json:"limits"`
	AcceptedKinds []string        `json:"accepted_kinds"`
	Refused       []Refusal       `json:"refused"`
	Jobs          []WorkerJob     `json:"jobs"`
	LastSeen      *time.Time      `json:"last_seen"`
	AddedAt       time.Time       `json:"added_at"`
	Counts        Counts          `json:"counts"`
}

func (s *Store) workerView(w *Worker) WorkerView {
	now := s.cfg.Now()
	v := WorkerView{ID: w.ID, Name: w.Name, Owner: strp(w.Owner), State: s.workerState(w, now), PublicKey: w.PublicKey,
		Address: w.Address, Limits: w.Limits, LastSeen: w.LastSeen, AddedAt: w.AddedAt, Counts: w.Counts,
		Tools: []Tool{}, Models: []Model{}, Fonts: []string{}, Jobs: []WorkerJob{}, Refused: []Refusal{}, Hardware: json.RawMessage("null")}
	if s.paused(w, now) {
		v.PausedUntil = w.PausedUntil
	}
	if c := w.Caps; c != nil {
		v.AppVersion, v.OS = strp(c.AppVersion), strp(c.OS)
		v.Tools = append(v.Tools, c.Tools...)
		v.Models = append(v.Models, c.Models...)
		v.Fonts = append(v.Fonts, c.Fonts...)
		if len(c.Hardware) > 0 {
			v.Hardware = c.Hardware
		}
	}
	acc, ref := s.kinds(w)
	v.AcceptedKinds = acc
	v.Refused = append(v.Refused, ref...)
	for _, l := range s.activeLeases(w.ID) {
		wj := WorkerJob{JobID: l.JobID, LeaseID: l.ID, Attempt: l.Attempt}
		if j := s.st.Jobs[l.JobID]; j != nil {
			wj.Title = strp(j.Spec.Title)
			if j.rung != nil {
				wj.Rung = strp(j.rung.Rung)
			}
		}
		if p := l.Progress; p != nil {
			wj.Phase, wj.Fraction, wj.FPS, wj.Speed, wj.ETAS = &p.Phase, p.Fraction, p.FPS, p.Speed, p.ETAS
		}
		v.Jobs = append(v.Jobs, wj)
	}
	return v
}

// ---------------------------------------------------------------- lists

// JobFilter narrows a job list.
type JobFilter struct {
	States     []string
	Priorities []string
	Kinds      []string
	UploadID   string
	Limit      int
	Cursor     int // an offset into the ordered list
}

// Counts of jobs.
type JobCounts struct {
	ByState    map[string]int `json:"by_state"`
	ByPriority map[string]int `json:"by_priority"`
}

// JobList is one page of the queue.
type JobList struct {
	ServerTime time.Time `json:"server_time"`
	Revision   int64     `json:"revision"`
	Counts     JobCounts `json:"counts"`
	Items      []JobView `json:"items"`
	Next       *string   `json:"next"`
}

func (s *Store) counts() JobCounts {
	c := JobCounts{ByState: map[string]int{}, ByPriority: map[string]int{}}
	for _, j := range s.st.Jobs {
		st := s.jobState(j)
		c.ByState[st]++
		if j.Final == "" {
			c.ByPriority[j.Priority]++
		}
	}
	return c
}

// ListJobs is the queue in the order the Encoder page shows it.
func (s *Store) ListJobs(f JobFilter) JobList {
	s.mu.Lock()
	defer s.mu.Unlock()
	group := func(st string) int {
		switch st {
		case StateLeased, StateChecking:
			return 0
		case StateQueued:
			return 1
		}
		return 2
	}
	type row struct {
		j  *Job
		st string
	}
	var rows []row
	for _, j := range s.st.Jobs {
		st := s.jobState(j)
		if len(f.States) > 0 && !contains(f.States, st) {
			continue
		}
		if len(f.Priorities) > 0 && !contains(f.Priorities, j.Priority) {
			continue
		}
		if len(f.Kinds) > 0 && !contains(f.Kinds, j.Spec.Kind) {
			continue
		}
		if f.UploadID != "" && j.Spec.UploadID != f.UploadID {
			continue
		}
		rows = append(rows, row{j, st})
	}
	sort.Slice(rows, func(a, b int) bool {
		ga, gb := group(rows[a].st), group(rows[b].st)
		if ga != gb {
			return ga < gb
		}
		if ga == 2 {
			fa, fb := rows[a].j.FinishedAt, rows[b].j.FinishedAt
			if fa != nil && fb != nil && !fa.Equal(*fb) {
				return fa.After(*fb)
			}
			return rows[a].j.ID < rows[b].j.ID
		}
		return lessJob(rows[a].j, rows[b].j)
	})
	positions := map[string]int{}
	for i, j := range s.queued() {
		positions[j.ID] = i + 1
	}
	if f.Limit <= 0 {
		f.Limit = 100
	}
	out := JobList{ServerTime: s.cfg.Now().UTC(), Revision: s.st.Revision, Counts: s.counts(), Items: []JobView{}}
	for i := f.Cursor; i < len(rows) && len(out.Items) < f.Limit; i++ {
		v := s.jobView(rows[i].j)
		if p, ok := positions[v.ID]; ok {
			p := p
			v.Position = &p
		}
		out.Items = append(out.Items, v)
	}
	if end := f.Cursor + len(out.Items); end < len(rows) {
		n := strconv.Itoa(end)
		out.Next = &n
	}
	return out
}

// Changes is the answer of the changes feed; Items holds JobView or Forgotten values.
type Changes struct {
	Revision int64 `json:"revision"`
	Reset    bool  `json:"reset,omitempty"`
	Items    []any `json:"items"`
}

// ForgottenView is a forgotten job in the changes feed.
type ForgottenView struct {
	ID        string `json:"job_id"`
	Forgotten bool   `json:"forgotten"`
	Revision  int64  `json:"revision"`
}

// WaitChanges answers the jobs changed since a revision, waiting up to wait for one.
func (s *Store) WaitChanges(ctx context.Context, since int64, limit int, wait time.Duration) Changes {
	if wait > s.cfg.LongPoll {
		wait = s.cfg.LongPoll
	}
	if limit <= 0 {
		limit = 500
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		s.mu.Lock()
		c := s.changesLocked(since, limit)
		ch := s.changed
		s.mu.Unlock()
		if len(c.Items) > 0 || c.Reset {
			return c
		}
		select {
		case <-ch:
		case <-timer.C:
			return c
		case <-ctx.Done():
			return c
		}
	}
}

func (s *Store) changesLocked(since int64, limit int) Changes {
	c := Changes{Revision: s.st.Revision, Items: []any{}}
	if since > s.st.Revision {
		c.Reset = true
		since = 0
	}
	type item struct {
		rev int64
		v   any
	}
	var items []item
	for _, j := range s.st.Jobs {
		if j.Revision > since {
			items = append(items, item{j.Revision, j})
		}
	}
	for _, f := range s.st.Forgotten {
		if f.Revision > since && !c.Reset {
			items = append(items, item{f.Revision, ForgottenView{ID: f.JobID, Forgotten: true, Revision: f.Revision}})
		}
	}
	sort.Slice(items, func(a, b int) bool { return items[a].rev < items[b].rev })
	if len(items) > limit {
		items = items[:limit]
		c.Revision = items[len(items)-1].rev
	}
	for _, it := range items {
		if j, ok := it.v.(*Job); ok {
			c.Items = append(c.Items, s.jobView(j))
		} else {
			c.Items = append(c.Items, it.v)
		}
	}
	return c
}

// Status is the queue itself.
type Status struct {
	Version         string            `json:"version"`
	ServerTime      time.Time         `json:"server_time"`
	Revision        int64             `json:"revision"`
	Draining        bool              `json:"draining"`
	ReferenceWorker *string           `json:"reference_worker"`
	ReferenceTools  map[string][]Tool `json:"reference_tools"`
	Tunnel          map[string]any    `json:"tunnel"`
	Counts          JobCounts         `json:"counts"`
	Timings         map[string]int    `json:"timings"`
}

// Status describes the queue; version is the program's.
func (s *Store) Status(version string) Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Status{Version: version, ServerTime: s.cfg.Now().UTC(), Revision: s.st.Revision, Draining: s.draining,
		ReferenceWorker: strp(s.cfg.ReferenceWorker), ReferenceTools: map[string][]Tool{}, Counts: s.counts(),
		Tunnel: map[string]any{"public_key": s.cfg.ServerPublicKey, "endpoint": s.cfg.Endpoint,
			"address": s.cfg.TunnelAddress.String(), "subnet": s.cfg.Subnet.String(), "peers": len(s.st.Workers)},
		Timings: map[string]int{"lease_s": secs(s.cfg.Lease), "renew_every_s": secs(s.cfg.RenewEvery),
			"long_poll_s": secs(s.cfg.LongPoll), "presence_every_s": secs(s.cfg.PresenceEvery),
			"pause_give_back_s": secs(s.cfg.PauseGiveBack)}}
	for _, k := range []string{KindRung, KindWhisper} {
		tools, _, _ := s.reference(k, false)
		if tools == nil {
			tools = []Tool{}
		}
		st.ReferenceTools[k] = tools
	}
	return st
}

func secs(d time.Duration) int { return int(d / time.Second) }

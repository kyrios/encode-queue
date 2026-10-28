package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/kyrios/encode-queue/internal/aligner"
)

// WorkerRef names a worker.
type WorkerRef struct {
	ID   string `json:"worker_id"`
	Name string `json:"name"`
}

// AttemptResult is what an attempt delivered, as the pipeline sees it.
type AttemptResult struct {
	Outcome     string  `json:"outcome"`
	Verdict     string  `json:"verdict"`
	SHA256      string  `json:"sha256,omitempty"`
	Bytes       int64   `json:"bytes,omitempty"`
	Frames      int64   `json:"frames,omitempty"`
	EncodeS     float64 `json:"encode_s,omitempty"`
	Tools       []Tool  `json:"tools,omitempty"`
	Path        *string `json:"path"`
	FileURL     *string `json:"file_url"`
	FailureCode *string `json:"failure_code"`
	Message     *string `json:"message"`
	Signal      *string `json:"signal"`
	ExitCode    *int    `json:"exit_code"`
	MemoryAvail *int64  `json:"memory_available_bytes"`
	Checks      []Check `json:"checks,omitempty"`
	// Check: the figures the worker's check found (v0.2.8), for the pipeline's floors.
	Check       json.RawMessage `json:"check,omitempty"`
	DeliveredAt time.Time       `json:"delivered_at"`
	DecidedAt   *time.Time      `json:"decided_at"`
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
	// ModelSHA256: the whisper model this attempt ran (v0.2.7).
	ModelSHA256 *string `json:"model_sha256"`
	// ECTS and RunnerUp: the scoring at the claim (v0.2.7, ect.go): this worker's expected
	// completion time in seconds, and the best other worker's.
	ECTS     *float64 `json:"ect_s"`
	RunnerUp *ECT     `json:"runner_up"`
	// Scoring: why the claim had that comparison (v0.2.10): scored, waited, assigned,
	// only_worker, not_claiming, speed_unknown; null for a lease from before.
	Scoring *string `json:"scoring"`
	// Points: what it earned once accepted (v0.2.15), by Punkte-Tabelle v1; 0 otherwise.
	Points float64 `json:"points"`
}

// JobView is a job as pipeline-admin.yaml shows it.
type JobView struct {
	ID       string  `json:"job_id"`
	UploadID *string `json:"upload_id"`
	Title    *string `json:"title"`
	Kind     string  `json:"kind"`
	// Type and Detail: what the job makes, as Funkhaus's Encoder page names it (v0.2.20,
	// jobtype.go): "Video" and "1080p · slower CRF 29", "Untertitel" and "turbo".
	Type            string            `json:"type"`
	Detail          string            `json:"detail"`
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
	ETAS            *int64            `json:"eta_s"`
	FinishAt        *time.Time        `json:"finish_at"`
	Waiting         *Waiting          `json:"waiting"`
	Cancelled       *Cancelled        `json:"cancelled"`
	PinnedTo        *PinView          `json:"pinned_to"`
	Revision        int64             `json:"revision"`
}

// PinView is the worker a job was sent to.
type PinView struct {
	WorkerID string    `json:"worker_id"`
	Name     string    `json:"name"`
	PinnedAt time.Time `json:"pinned_at"`
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
		StartedAt: l.CreatedAt, EndedAt: l.EndedAt, EndCode: strp(l.EndCode), ModelSHA256: strp(l.Model), RunnerUp: l.RunnerUp, Scoring: strp(l.Scoring),
		Points: l.Points}
	if l.ECT != nil {
		v := l.ECT.S
		a.ECTS = &v
	}
	if l.State == LeaseActive {
		e := l.ExpiresAt
		a.ExpiresAt = &e
		a.Progress = l.Progress
	}
	if r := l.Result; r != nil {
		ar := &AttemptResult{Outcome: r.Outcome, Verdict: r.Verdict, SHA256: r.SHA256, Bytes: r.Bytes, Frames: r.Frames,
			EncodeS: r.EncodeS, Tools: r.Tools, FailureCode: strp(r.FailureCode), Message: strp(r.Message),
			Signal: strp(r.Signal), ExitCode: r.ExitCode, MemoryAvail: r.MemoryAvail,
			Checks: r.Checks, Check: r.Figures, DeliveredAt: r.DeliveredAt, DecidedAt: r.DecidedAt}
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
	return s.jobViewWith(j, s.forecast(s.cfg.Now()))
}

func (s *Store) jobViewWith(j *Job, fc map[string]time.Time) JobView {
	st := s.jobState(j)
	v := JobView{ID: j.ID, UploadID: strp(j.Spec.UploadID), Title: strp(j.Spec.Title), Kind: j.Spec.Kind, Stage: j.Spec.Stage,
		Priority: j.Priority, State: st, Params: j.Spec.Params, Source: j.Spec.Source, Inputs: j.Spec.Inputs,
		ResultSpec: j.Spec.Result, Failures: j.Failures, QueuedAt: j.QueuedAt, FinishedAt: j.FinishedAt,
		Revision: j.Revision, Attempts: []AttemptView{}, ExcludedWorkers: []WorkerRef{}}
	v.Type, v.Detail = jobType(j)
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
			q = modelLabel(w.acceptable()[0])
		}
		if len(w.Models) > 1 {
			q += fmt.Sprintf(" (+%d)", len(w.Models)-1)
		}
		v.Quality = strp(q)
	}
	if a := j.align; a != nil {
		q := a.Model
		if q == "" {
			q = modelLabel(a.ModelSHA256)
		}
		v.Quality = strp(q + " " + a.Language)
	}
	v.Waiting = s.waiting(j, s.cfg.Now())
	v.Cancelled = j.Cancelled
	if j.Final != "" {
		v.FinishAt = j.FinishedAt
	} else if t, ok := fc[j.ID]; ok {
		t = t.UTC().Truncate(time.Second)
		v.FinishAt = &t
	}
	if p := j.Pin; p != nil {
		v.PinnedTo = &PinView{WorkerID: p.WorkerID, Name: s.workerRef(p.WorkerID, "").Name, PinnedAt: p.At}
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
			v.ETAS = eta(j, shown.Progress)
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
	HoldUntil      *time.Time        `json:"hold_until,omitempty"` // v0.2.9: no renewal past it
	RenewEveryS    int               `json:"renew_every_s"`
	PauseGiveBackS int               `json:"pause_give_back_s"`
	PauseHold      bool              `json:"pause_hold"` // v0.2.18: a pause keeps the lease (pauseHolds)
	PausedUntil    *time.Time        `json:"paused_until"`
	Params         json.RawMessage   `json:"params"`
	Result         ResultSpec        `json:"result"`
	Progress       *Progress         `json:"progress"`
	Links          map[string]string `json:"links"`
}

// modelLabel is a model's catalogue name, or its sha256's first 12 characters.
func modelLabel(sha string) string {
	if m, ok := catalogue[sha]; ok {
		return m.Name
	}
	if sha == aligner.SHA256 {
		return aligner.Name
	}
	if len(sha) >= 12 {
		return sha[:12]
	}
	return sha
}

// modelName is what the worker calls a model it holds ("ggml-medium.bin"), or "".
func (s *Store) modelName(workerID, sha string) string {
	if w := s.st.Workers[workerID]; w != nil && w.Caps != nil {
		for _, m := range w.Caps.Models {
			if m.SHA256 == sha {
				return m.Name
			}
		}
	}
	return ""
}

func (s *Store) leaseView(l *Lease) LeaseView {
	j := s.st.Jobs[l.JobID]
	v := LeaseView{ID: l.ID, Attempt: l.Attempt, State: l.State, CreatedAt: l.CreatedAt, ExpiresAt: l.ExpiresAt,
		RenewEveryS: int(s.cfg.RenewEvery / time.Second), PauseGiveBackS: int(s.cfg.PauseGiveBack / time.Second),
		Progress: l.Progress}
	if !l.HoldUntil.IsZero() {
		v.HoldUntil = ptrTime(l.HoldUntil)
	}
	if w := s.st.Workers[l.WorkerID]; w != nil && s.paused(w, s.cfg.Now()) {
		v.PausedUntil = w.PausedUntil
	}
	if j != nil {
		src := j.Spec.Source
		v.PauseHold = pauseHolds(j)
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
		if p := j.whisper; p != nil && l.Model != "" {
			// The chosen model, alone, so a worker of v0.2.6 or before runs exactly it.
			q := WhisperParams{ModelSHA256: l.Model, Language: p.Language, Threads: p.Threads, Model: s.modelName(l.WorkerID, l.Model)}
			v.Params, _ = json.Marshal(q)
		}
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
	// Slot: the slot it runs in, normal or high (v0.2.14). Extra is true in a high-priority slot,
	// v0.2.5's name for it.
	Slot  string `json:"slot"`
	Extra bool   `json:"extra"`
}

// WorkerSlots is a worker's slots of each kind and how many hold a lease (v0.2.14): the
// normal ones take every job, the high-priority ones jobs of priority now and stage1 only.
type WorkerSlots struct {
	Normal SlotCount `json:"normal"`
	High   SlotCount `json:"high"`
}

// SlotCount is a worker's slots of one kind: as many as the queue gives it (what it asked for,
// capped), and those that hold an active lease.
type SlotCount struct {
	Slots int `json:"slots"`
	Busy  int `json:"busy"`
}

// WorkerView is a worker as pipeline-admin.yaml shows it.
type WorkerView struct {
	ID    string  `json:"worker_id"`
	Name  string  `json:"name"`
	Owner *string `json:"owner"`
	State string  `json:"state"`
	// Claiming: it asked for work within two presence intervals, or works (v0.2.10); one that
	// does not counts in no scoring.
	Claiming    bool            `json:"claiming"`
	PausedUntil *time.Time      `json:"paused_until"`
	PublicKey   string          `json:"public_key"`
	Address     string          `json:"tunnel_address"`
	AppVersion  *string         `json:"app_version"`
	HostApp     *HostApp        `json:"host_app"`
	OS          *string         `json:"os"`
	Tools       []Tool          `json:"tools"`
	Models      []Model         `json:"models"`
	Fonts       []string        `json:"fonts"`
	Hardware    json.RawMessage `json:"hardware"`
	Limits      Limits          `json:"limits"`
	Slots       WorkerSlots     `json:"slots"`
	// Points: what its accepted results earned today, this week and in all (v0.2.15). House: it is
	// encoder's own worker, the reference, which counts with the others under a "Haus" badge.
	Points        PointsView   `json:"points"`
	House         bool         `json:"house"`
	AcceptedKinds []string     `json:"accepted_kinds"`
	Refused       []Refusal    `json:"refused"`
	Jobs          []WorkerJob  `json:"jobs"`
	LastSeen      *time.Time   `json:"last_seen"`
	AddedAt       time.Time    `json:"added_at"`
	Counts        Counts       `json:"counts"`
	Endpoint      *string      `json:"endpoint"`
	LastHandshake *time.Time   `json:"last_handshake"`
	TunnelState   *string      `json:"tunnel_state"`
	TunnelSince   *time.Time   `json:"tunnel_since"`
	System        WorkerSystem `json:"system"`
	Transfer      Transfer     `json:"transfer"`
	RelativeSpeed *float64     `json:"relative_speed"`
	// RelativeSpeedSource: "measured" from delivered jobs, "calibrated" from the calibration
	// clip until there are some (v0.2.5); null without a relative speed.
	RelativeSpeedSource *string        `json:"relative_speed_source"`
	SpeedEstimate       *SpeedEstimate `json:"speed_estimate"`
	// Corrections: actual over estimated compute time by job kind, learned from its results
	// (v0.2.7); 1 is as estimated.
	Corrections  map[string]float64 `json:"corrections"`
	SpeedClasses []SpeedClass       `json:"speed_classes"`
	FreeAt       *time.Time         `json:"free_at"`
}

// WorkerSystem is the worker's machine, from its capabilities' hardware.
type WorkerSystem struct {
	CPUModel     *string `json:"cpu_model"`
	CPUCores     *int    `json:"cpu_cores"`
	CPUThreads   *int    `json:"cpu_threads"`
	ThreadsInUse *int    `json:"threads_in_use"`
	ThreadsBusy  *int    `json:"threads_busy"`
	// OtherLoad: threads other programs keep busy, from the last presence (v0.2.7).
	OtherLoad   *float64 `json:"other_load"`
	MemoryBytes *int64   `json:"memory_bytes"`
	MemoryUsed  *int64   `json:"memory_used_bytes"`
	OSName      *string  `json:"os_name"`
	OSVersion   *string  `json:"os_version"`
	Arch        *string  `json:"arch"`
	Kernel      *string  `json:"kernel"`
}

// hardware is what a worker's capabilities may say about its machine (helper.yaml).
type hardware struct {
	Chip        string `json:"chip"`
	CPUs        int    `json:"cpus"`
	CPUCores    int    `json:"cpu_cores"`
	CPUThreads  int    `json:"cpu_threads"`
	MemoryBytes int64  `json:"memory_bytes"`
	OSName      string `json:"os_name"`
	OSVersion   string `json:"os_version"`
	Arch        string `json:"arch"`
	Kernel      string `json:"kernel"`
}

func intp(n int) *int {
	if n <= 0 {
		return nil
	}
	return &n
}

func (s *Store) system(w *Worker) WorkerSystem {
	var hw hardware
	if w.Caps != nil && len(w.Caps.Hardware) > 0 {
		json.Unmarshal(w.Caps.Hardware, &hw) // a field of another type stays empty
	}
	threads := hw.CPUThreads
	if threads == 0 {
		threads = hw.CPUs
	}
	v := WorkerSystem{CPUModel: strp(hw.Chip), CPUCores: intp(hw.CPUCores), CPUThreads: intp(threads),
		OSName: strp(hw.OSName), OSVersion: strp(hw.OSVersion), Arch: strp(hw.Arch), Kernel: strp(hw.Kernel)}
	if hw.MemoryBytes > 0 {
		m := hw.MemoryBytes
		v.MemoryBytes = &m
	}
	v.MemoryUsed = w.MemoryUsed
	v.OtherLoad = w.OtherLoad
	if b := w.ThreadsBusy; b != nil {
		n := *b
		if v.CPUThreads != nil && n > *v.CPUThreads {
			n = *v.CPUThreads
		}
		v.ThreadsBusy = &n
	}
	v.ThreadsInUse = v.CPUThreads
	if t := w.Limits.CPUThreads; t != nil {
		n := *t
		if v.CPUThreads != nil && n > *v.CPUThreads {
			n = *v.CPUThreads
		}
		v.ThreadsInUse = &n
	}
	return v
}

// Transfer is how fast a worker moves files through the tunnel.
type Transfer struct {
	Download TransferRate `json:"download"`
	Upload   TransferRate `json:"upload"`
}

// TransferRate is one direction.
type TransferRate struct {
	CurrentBps float64    `json:"current_bps"`
	LastBps    *float64   `json:"last_bps"`
	AvgBps     *float64   `json:"avg_bps"`
	LastAt     *time.Time `json:"last_at"`
	Samples    int64      `json:"samples"`
}

func (s *Store) rate(t TransferStats, now time.Time) TransferRate {
	r := TransferRate{CurrentBps: t.current(now, 2*s.cfg.RenewEvery+5*time.Second), LastAt: t.LastAt, Samples: t.Samples}
	if t.Samples > 0 {
		last, avg := t.LastBps, t.AvgBps
		r.LastBps, r.AvgBps = &last, &avg
	}
	return r
}

// SpeedEstimate is "Tempo" before a measured relative speed exists: the running job's live fps
// over the reference's median on its rung class (v0.2.5).
type SpeedEstimate struct {
	Value        float64 `json:"value"`
	Class        string  `json:"class"`
	FPS          float64 `json:"fps"`
	ReferenceFPS float64 `json:"reference_fps"`
}

// SpeedClass is a worker's average fps on one rung class, beside the reference's.
type SpeedClass struct {
	Class         string    `json:"class"`
	Codec         string    `json:"codec"`
	Width         int       `json:"width"`
	Height        int       `json:"height"`
	Preset        string    `json:"preset"`
	FPS           float64   `json:"fps"`
	Samples       int       `json:"samples"`
	ReferenceFPS  *float64  `json:"reference_fps"`
	RelativeSpeed *float64  `json:"relative_speed"`
	UpdatedAt     time.Time `json:"updated_at"`
	// Source: "measured" (delivered jobs, Samples of them) or "calibrated" (the clip, v0.2.5).
	Source string `json:"source"`
}

func (s *Store) peerStatus() map[string]PeerStatus {
	if s.PeerStatus == nil {
		return nil
	}
	return s.PeerStatus()
}

func (s *Store) workerView(w *Worker) WorkerView {
	return s.workerViewWith(w, s.peerStatus())
}

func (s *Store) workerViewWith(w *Worker, peers map[string]PeerStatus) WorkerView {
	now := s.cfg.Now()
	v := WorkerView{ID: w.ID, Name: w.Name, Owner: strp(w.Owner), State: s.workerState(w, now), Claiming: s.claiming(w, now), PublicKey: w.PublicKey,
		Address: w.Address, Limits: w.Limits, LastSeen: w.LastSeen, AddedAt: w.AddedAt, Counts: w.Counts,
		Tools: []Tool{}, Models: []Model{}, Fonts: []string{}, Jobs: []WorkerJob{}, Refused: []Refusal{}, Hardware: json.RawMessage("null")}
	if s.paused(w, now) {
		v.PausedUntil = w.PausedUntil
	}
	if c := w.Caps; c != nil {
		v.AppVersion, v.OS, v.HostApp = strp(c.AppVersion), strp(c.OS), c.HostApp
		v.Tools = append(v.Tools, c.Tools...)
		v.Models = append(v.Models, c.Models...)
		v.Fonts = append(v.Fonts, c.Fonts...)
		if len(c.Hardware) > 0 {
			v.Hardware = c.Hardware
		}
	}
	if p, ok := peers[w.PublicKey]; ok {
		v.Endpoint = strp(p.Endpoint)
		if !p.LastHandshake.IsZero() {
			t := p.LastHandshake.UTC()
			v.LastHandshake = &t
			ts := "up"
			if now.Sub(t) > tunnelUp {
				ts = "down"
			} else {
				since := t // before the next tick has seen it come up
				if w.tunnelSince != nil {
					since = w.tunnelSince.UTC()
				}
				v.TunnelSince = &since
			}
			v.TunnelState = &ts
		}
	}
	v.System = s.system(w)
	v.Transfer = Transfer{Download: s.rate(w.Download, now), Upload: s.rate(w.Upload, now)}
	rel, src := s.relativeSpeedSource(w)
	v.RelativeSpeed = rel
	if refW := s.referenceWorker(); refW == w {
		v.RelativeSpeed, src = nil, ""
		if len(w.Speeds) > 0 || len(calibratedClasses(w)) > 0 {
			one := 1.0
			v.RelativeSpeed, src = &one, SpeedCalibrated
			if len(w.Speeds) > 0 {
				src = SpeedMeasured
			}
		}
	}
	if src != "" {
		v.RelativeSpeedSource = &src
	}
	v.SpeedClasses = []SpeedClass{}
	refW := s.referenceWorker()
	for _, h := range w.Speeds {
		sc := SpeedClass{Class: h.Class, Codec: h.Codec, Width: h.Width, Height: h.Height, Preset: h.Preset,
			FPS: h.median(), Samples: len(h.FPS), UpdatedAt: h.UpdatedAt, RelativeSpeed: s.classRelative(w, h.Class), Source: SpeedMeasured}
		if refW != nil {
			if rh := refW.speed(h.Class); rh != nil {
				f := rh.median()
				sc.ReferenceFPS = &f
			}
		}
		v.SpeedClasses = append(v.SpeedClasses, sc)
	}
	// The calibrated classes it has no measured result on yet.
	if w.Caps != nil && w.Caps.Calibration != nil {
		for _, cc := range w.Caps.Calibration.Classes {
			if w.speed(cc.Class) != nil {
				continue
			}
			codec, width, height, preset := parseClass(cc.Class)
			sc := SpeedClass{Class: cc.Class, Codec: codec, Width: width, Height: height, Preset: preset,
				FPS: cc.FPS, UpdatedAt: w.Caps.Calibration.At, RelativeSpeed: s.calibratedRelative(w, cc.Class), Source: SpeedCalibrated}
			if refW != nil {
				if f, clip := calibratedFPS(refW, cc.Class); f > 0 && clip == w.Caps.Calibration.ClipSHA256 {
					sc.ReferenceFPS = &f
				}
			}
			v.SpeedClasses = append(v.SpeedClasses, sc)
		}
	}
	v.FreeAt = s.freeAt(w, now, "", false)
	normal, high := s.slotsBusy(w)
	v.Slots = WorkerSlots{Normal: SlotCount{s.maxJobs(w), normal}, High: SlotCount{s.highSlots(w), high}}
	v.Points = s.pointsView(w)
	v.House = s.cfg.ReferenceWorker != "" && w.Name == s.cfg.ReferenceWorker
	v.Corrections = map[string]float64{}
	for k, c := range w.Corrections {
		v.Corrections[k] = c
	}
	acc, ref := s.kinds(w)
	v.AcceptedKinds = acc
	v.Refused = append(v.Refused, ref...)
	for _, l := range s.activeLeases(w.ID) {
		wj := WorkerJob{JobID: l.JobID, LeaseID: l.ID, Attempt: l.Attempt, Slot: SlotNormal, Extra: l.High}
		if l.High {
			wj.Slot = SlotHigh
		}
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
	if v.RelativeSpeed == nil {
		v.SpeedEstimate = s.speedEstimate(w)
	}
	return v
}

// speedEstimate is the worker's live fps on the rung job it encodes against the reference's
// median on that class, or nil: a new worker shows a tempo while its first job runs, before any
// result has been measured.
func (s *Store) speedEstimate(w *Worker) *SpeedEstimate {
	ref := s.referenceWorker()
	if ref == nil || ref == w {
		return nil
	}
	for _, l := range s.activeLeases(w.ID) {
		j := s.st.Jobs[l.JobID]
		p := l.Progress
		if j == nil || j.rung == nil || p == nil || p.Phase != "encoding" || p.FPS == nil || *p.FPS <= 0 {
			continue
		}
		class := rungClass(j.rung)
		rh := ref.speed(class)
		if rh == nil || rh.median() <= 0 {
			continue
		}
		return &SpeedEstimate{Value: *p.FPS / rh.median(), Class: class, FPS: *p.FPS, ReferenceFPS: rh.median()}
	}
	return nil
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
	fc := s.forecast(s.cfg.Now())
	positions := map[string]int{}
	for i, j := range s.queued() {
		positions[j.ID] = i + 1
	}
	if f.Limit <= 0 {
		f.Limit = 100
	}
	out := JobList{ServerTime: s.cfg.Now().UTC(), Revision: s.st.Revision, Counts: s.counts(), Items: []JobView{}}
	for i := f.Cursor; i < len(rows) && len(out.Items) < f.Limit; i++ {
		v := s.jobViewWith(rows[i].j, fc)
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
	var fc map[string]time.Time
	for _, it := range items {
		if j, ok := it.v.(*Job); ok {
			if fc == nil {
				fc = s.forecast(s.cfg.Now())
			}
			c.Items = append(c.Items, s.jobViewWith(j, fc))
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

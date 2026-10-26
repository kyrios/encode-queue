// Package worker takes jobs from the queue through its tunnel and runs them: ffmpeg with
// libx265 or libx264, or whisper.cpp, always at nice 19.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Config is the worker's settings.
type Config struct {
	Version        string
	StateDir       string
	WorkDir        string
	FFmpeg         string
	WhisperCLI     string
	WhisperVersion string
	ModelsDir      string
	Fonts          []string
	CPUThreads     int // 0: the tools' own
	GPU            bool
	MaxParallel    int
	ChunkBytes     int64
	Log            *slog.Logger

	// Timings the queue does not send; tests shorten them.
	LongPoll      time.Duration
	LogEvery      time.Duration
	RetryAfter    time.Duration
	ProgressEvery time.Duration // how often the runner samples ffmpeg's progress for the report

	// Events receives the worker's events (Slipstream's JSON lines on stdout); nil for none.
	Events func(map[string]any)
}

func (c *Config) defaults() {
	if c.FFmpeg == "" {
		c.FFmpeg = "ffmpeg"
	}
	if c.WhisperCLI == "" {
		c.WhisperCLI = "whisper-cli"
	}
	if c.WorkDir == "" {
		c.WorkDir = filepath.Join(c.StateDir, "work")
	}
	if c.MaxParallel < 1 {
		c.MaxParallel = 1
	}
	if c.ChunkBytes == 0 {
		c.ChunkBytes = 16 << 20
	}
	if c.LongPoll == 0 {
		c.LongPoll = 25 * time.Second
	}
	if c.LogEvery == 0 {
		c.LogEvery = 5 * time.Second
	}
	if c.RetryAfter == 0 {
		c.RetryAfter = 5 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// JobStatus is one running job, for the control socket and the menu bar.
type JobStatus struct {
	LeaseID  string    `json:"lease_id"`
	JobID    string    `json:"job_id"`
	Title    string    `json:"title,omitempty"`
	Kind     string    `json:"kind"`
	Rung     string    `json:"rung,omitempty"`
	Attempt  int       `json:"attempt"`
	Started  time.Time `json:"started_at"`
	Progress Progress  `json:"progress"`
	Log      []string  `json:"log,omitempty"`
}

// Finished is a job this worker ended.
type Finished struct {
	LeaseID string    `json:"lease_id"`
	JobID   string    `json:"job_id"`
	Title   string    `json:"title,omitempty"`
	Outcome string    `json:"outcome"` // accepted, rejected, superseded, checking, failed, given_back, stopped
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at"`
}

// Status is the worker's state for the control socket.
type Status struct {
	State         string      `json:"state"` // idle, working, paused, offline, unpaired
	Name          string      `json:"name,omitempty"`
	PausedUntil   *time.Time  `json:"paused_until"`
	Connected     bool        `json:"connected"`
	LastContact   *time.Time  `json:"last_contact"`
	LastError     string      `json:"last_error,omitempty"`
	Refused       []string    `json:"refused,omitempty"`
	Tools         []Tool      `json:"tools"`
	Jobs          []JobStatus `json:"jobs"`
	Recent        []Finished  `json:"recent"`
	FinishedToday int         `json:"finished_today"`
}

// Worker runs jobs.
type Worker struct {
	cfg    Config
	api    *Client
	caps   Capabilities
	models map[string]string // sha256 -> path

	mu          sync.Mutex
	name        string
	pausedUntil *time.Time
	pausePatch  bool // the pause changed here and the queue does not know yet
	lastContact *time.Time
	lastErr     string
	refused     []string
	refusedKind []map[string]string
	up          bool
	healthAt    time.Time
	limitsPatch bool
	jobLimit    int
	limitCh     chan struct{} // closed and replaced when the job limit changes
	runs        map[string]*run
	recent      []Finished
	today       int
	todayDate   string
	pauseCh     chan struct{} // closed and replaced when the pause changes
	quit        chan struct{}
	quitOnce    sync.Once
}

// New is a worker that talks to the queue through api.
func New(cfg Config, api *Client) (*Worker, error) {
	cfg.defaults()
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		return nil, err
	}
	return &Worker{cfg: cfg, api: api, runs: map[string]*run{}, pauseCh: make(chan struct{}), quit: make(chan struct{}),
		jobLimit: cfg.MaxParallel, limitCh: make(chan struct{})}, nil
}

// emit sends an event to cfg.Events.
func (w *Worker) emit(event string, fields map[string]any) {
	if w.cfg.Events == nil {
		return
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"] = event
	w.cfg.Events(fields)
}

// healthFile is touched on every answer from the queue; `encode-worker healthcheck` reads it.
func (w *Worker) healthFile() string { return filepath.Join(w.cfg.StateDir, "health") }

func (w *Worker) contact(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err == nil {
		now := time.Now()
		w.lastContact = &now
		w.lastErr = ""
		if now.Sub(w.healthAt) > 10*time.Second {
			w.healthAt = now
			os.WriteFile(w.healthFile(), []byte(now.UTC().Format(time.RFC3339)+"\n"), 0o600)
		}
	} else {
		var ae *APIError
		if !errors.As(err, &ae) {
			w.lastErr = err.Error()
		}
	}
	up := w.lastContact != nil && time.Since(*w.lastContact) < 90*time.Second
	if up != w.up {
		w.up = up
		w.emitConnectionLocked()
	}
}

func (w *Worker) emitConnectionLocked() {
	refused := []map[string]string{}
	refused = append(refused, w.refusedKind...)
	f := map[string]any{"up": w.up, "refused": refused, "last_answer_at": nil}
	if w.lastContact != nil {
		f["last_answer_at"] = w.lastContact.UTC().Format(time.RFC3339)
	}
	go w.emit("connection", f)
}

// SetLimits changes the threads, the GPU and the jobs at once (Slipstream's settings); the
// queue hears it with the next presence.
func (w *Worker) SetLimits(threads int, gpu bool, jobs int) {
	if jobs < 1 {
		jobs = 1
	}
	if jobs > maxSlots {
		jobs = maxSlots
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg.CPUThreads, w.cfg.GPU, w.cfg.MaxParallel = threads, gpu, jobs
	w.jobLimit = jobs
	w.limitsPatch = true
	close(w.limitCh)
	w.limitCh = make(chan struct{})
	close(w.pauseCh) // wakes the presence loop
	w.pauseCh = make(chan struct{})
}

func (w *Worker) limits() map[string]any {
	l := map[string]any{"gpu": w.cfg.GPU, "max_parallel_jobs": w.cfg.MaxParallel}
	if w.cfg.CPUThreads > 0 {
		l["cpu_threads"] = w.cfg.CPUThreads
	}
	return l
}

// maxSlots is the most jobs at once a worker runs.
const maxSlots = 16

func ptrTime(t time.Time) *time.Time { return &t }

// paused is whether the worker is paused now.
func (w *Worker) paused() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pausedUntil != nil && w.pausedUntil.After(time.Now())
}

// setPause changes the pause; fromQueue is a pause the queue reported.
func (w *Worker) setPause(until *time.Time, fromQueue bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if fromQueue && w.pausePatch {
		return // ours is newer and on its way
	}
	if until != nil && !until.After(time.Now()) {
		until = nil
	}
	same := (until == nil && w.pausedUntil == nil) || (until != nil && w.pausedUntil != nil && until.Equal(*w.pausedUntil))
	if same {
		return
	}
	w.pausedUntil = until
	if !fromQueue {
		w.pausePatch = true
	}
	close(w.pauseCh)
	w.pauseCh = make(chan struct{})
	w.cfg.Log.Info("pause", "until", until)
	var u any
	if until != nil {
		u = until.UTC().Format(time.RFC3339)
	}
	go w.emit("paused", map[string]any{"until": u})
}

// Pause pauses until the given time (from the control socket or the menu bar).
func (w *Worker) Pause(until time.Time) { w.setPause(&until, false) }

// Resume ends the pause.
func (w *Worker) Resume() { w.setPause(nil, false) }

// Quit gives every job back and stops the worker.
func (w *Worker) Quit() { w.quitOnce.Do(func() { close(w.quit) }) }

// Status is the worker's state now.
func (w *Worker) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := Status{Name: w.name, PausedUntil: w.pausedUntil, LastContact: w.lastContact, LastError: w.lastErr,
		Refused: w.refused, Tools: w.caps.Tools, Jobs: []JobStatus{}, Recent: append([]Finished(nil), w.recent...), FinishedToday: w.today}
	st.Connected = w.lastContact != nil && time.Since(*w.lastContact) < 90*time.Second
	for _, r := range w.runs {
		st.Jobs = append(st.Jobs, r.status())
	}
	switch {
	case !st.Connected:
		st.State = "offline"
	case w.pausedUntil != nil && w.pausedUntil.After(time.Now()):
		st.State = "paused"
	case len(w.runs) > 0:
		st.State = "working"
	default:
		st.State = "idle"
	}
	return st
}

func (w *Worker) finished(f Finished) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f.At = time.Now()
	for i := range w.recent {
		if w.recent[i].LeaseID == f.LeaseID {
			w.recent[i] = f
			w.emitResult(f)
			return
		}
	}
	day := f.At.Format("2006-01-02")
	if day != w.todayDate {
		w.todayDate, w.today = day, 0
	}
	if f.Outcome == "accepted" || f.Outcome == "checking" {
		w.today++
	}
	w.recent = append([]Finished{f}, w.recent...)
	if len(w.recent) > 3 {
		w.recent = w.recent[:3]
	}
	w.emitResult(f)
}

// emitResult tells the app how a job ended: accepted, rejected (a failure too), or given back
// (a pause, a quit, preemption, a job done or cancelled elsewhere). A result still checking
// waits for its verdict.
func (w *Worker) emitResult(f Finished) {
	verdict := ""
	switch f.Outcome {
	case "accepted":
		verdict = "accepted"
	case "rejected", "failed":
		verdict = "rejected"
	case "given_back", "stopped", "superseded":
		verdict = "given_back"
	default:
		return
	}
	go w.emit("result", map[string]any{"lease_id": f.LeaseID, "title": f.Title, "verdict": verdict, "check": f.Reason})
}

// waitUnpaused blocks while the worker is paused, until ctx ends.
func (w *Worker) waitUnpaused(ctx context.Context) error {
	for {
		w.mu.Lock()
		p := w.pausedUntil != nil && w.pausedUntil.After(time.Now())
		ch := w.pauseCh
		var until time.Duration
		if p {
			until = time.Until(*w.pausedUntil)
		}
		w.mu.Unlock()
		if !p {
			return nil
		}
		select {
		case <-ch:
		case <-time.After(until):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Run pairs with the queue and takes jobs until ctx ends or Quit is called. On the way out it
// gives every job back.
func (w *Worker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-w.quit:
			cancel()
		case <-ctx.Done():
		}
	}()
	caps, problems := DetectCapabilities(ctx, w.cfg)
	for _, p := range problems {
		w.cfg.Log.Warn("a tool", "problem", p)
	}
	w.caps = caps
	w.emit("tools", map[string]any{"tools": caps.Tools})
	w.emit("idle", nil)
	w.models = map[string]string{}
	for _, m := range caps.Models {
		w.models[m.SHA256] = m.path
	}
	// Until the queue answers, keep asking: the tunnel may come up after the worker.
	for {
		if err := w.hello(ctx); err == nil {
			break
		} else {
			w.contact(err)
			w.cfg.Log.Warn("the queue does not answer yet", "err", err)
			var ae *APIError
			if errors.As(err, &ae) && ae.Code == "app_too_old" {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.cfg.RetryAfter):
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.presenceLoop(ctx) }()
	for i := 0; i < maxSlots; i++ {
		wg.Add(1)
		go func(slot int) { defer wg.Done(); w.claimLoop(ctx, slot) }(i)
	}
	wg.Wait()
	return nil
}

// hello says who this worker is: its limits, its capabilities, and gives back leases a
// former run of it left (x265 cannot resume).
func (w *Worker) hello(ctx context.Context) error {
	w.mu.Lock()
	limits := w.limits()
	w.mu.Unlock()
	me, err := w.api.PatchMe(ctx, map[string]any{"limits": limits})
	if err != nil {
		return err
	}
	rep, err := w.api.PutCapabilities(ctx, w.caps)
	if err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Code == "app_too_old" {
			latest := ae.Detail
			if i := strings.LastIndex(latest, " "); i >= 0 {
				latest = latest[i+1:]
			}
			w.emit("update", map[string]any{"latest": latest})
		}
		return err
	}
	w.mu.Lock()
	w.name = me.Name
	w.refused = nil
	w.refusedKind = nil
	for _, r := range rep.Refused {
		w.refused = append(w.refused, r.Kind+": "+r.Reason)
		w.refusedKind = append(w.refusedKind, map[string]string{"kind": r.Kind, "reason": r.Reason})
	}
	w.emitConnectionLocked()
	w.mu.Unlock()
	w.setPause(me.PausedUntil, true)
	w.contact(nil)
	w.cfg.Log.Info("paired", "name", me.Name, "address", me.Address, "accepted", rep.Accepted, "refused", w.refused)
	leases, err := w.api.MyLeases(ctx)
	if err != nil {
		return err
	}
	for _, l := range leases {
		w.cfg.Log.Info("give back a lease of a former run", "lease", l.ID, "job", l.Job.ID)
		w.api.Release(ctx, l.ID, "restart")
	}
	return nil
}

func (w *Worker) presenceLoop(ctx context.Context) {
	every := 30 * time.Second
	for {
		w.mu.Lock()
		if w.limitsPatch {
			w.limitsPatch = false
			l := w.limits()
			w.mu.Unlock()
			if _, err := w.api.PatchMe(ctx, map[string]any{"limits": l}); err != nil {
				w.contact(err)
				w.mu.Lock()
				w.limitsPatch = true
				w.mu.Unlock()
			}
			w.mu.Lock()
		}
		patch, until := w.pausePatch, w.pausedUntil
		state := "idle"
		if len(w.runs) > 0 {
			state = "working"
		}
		w.mu.Unlock()
		if patch {
			var v any
			if until != nil {
				v = until.UTC().Format(time.RFC3339)
			}
			_, err := w.api.PatchMe(ctx, map[string]any{"paused_until": v})
			w.contact(err)
			if err == nil {
				w.mu.Lock()
				if w.pausedUntil == until {
					w.pausePatch = false
				}
				w.mu.Unlock()
			}
		}
		if w.paused() {
			state = "paused"
		}
		var free int64
		var st unix.Statfs_t
		if unix.Statfs(w.cfg.WorkDir, &st) == nil {
			free = int64(st.Bavail) * int64(st.Bsize)
		}
		ack, err := w.api.PutPresence(ctx, state, free)
		w.contact(err)
		if err == nil {
			w.setPause(ack.PausedUntil, true)
			if ack.PresenceEveryS > 0 {
				every = time.Duration(ack.PresenceEveryS) * time.Second
			}
		}
		w.mu.Lock()
		ch := w.pauseCh
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		case <-ch: // a pause or resume reaches the queue at once
		}
	}
}

// claimLoop is one job slot; slots at or above the job limit wait.
func (w *Worker) claimLoop(ctx context.Context, slot int) {
	for ctx.Err() == nil {
		w.mu.Lock()
		limit, ch := w.jobLimit, w.limitCh
		w.mu.Unlock()
		if slot >= limit {
			select {
			case <-ctx.Done():
				return
			case <-ch:
			}
			continue
		}
		if err := w.waitUnpaused(ctx); err != nil {
			return
		}
		l, err := w.api.Claim(ctx, w.cfg.LongPoll)
		w.contact(err)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var ae *APIError
			wait := w.cfg.RetryAfter
			if errors.As(err, &ae) {
				switch ae.Code {
				case "paused":
					// The queue knows a pause this worker has not heard of; presence brings it.
				case "no_capabilities":
					w.hello(ctx)
				case "unknown_peer":
					w.cfg.Log.Error("the queue does not know this worker; pair it again")
					wait = time.Minute
				}
			}
			w.cfg.Log.Debug("claim", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		if l == nil {
			continue
		}
		w.runLease(ctx, l)
	}
}

func (w *Worker) runLease(ctx context.Context, l *Lease) {
	r := newRun(w, l)
	w.mu.Lock()
	w.runs[l.ID] = r
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.runs, l.ID)
		idle := len(w.runs) == 0
		w.mu.Unlock()
		os.RemoveAll(r.dir)
		if idle {
			w.emit("idle", nil)
		}
	}()
	w.cfg.Log.Info("job", "lease", l.ID, "job", l.Job.ID, "attempt", l.Attempt, "kind", l.Job.Kind)
	f := r.execute(ctx)
	f.LeaseID, f.JobID, f.Title = l.ID, l.Job.ID, l.Job.Title
	w.finished(f)
	w.cfg.Log.Info("job ended", "lease", l.ID, "job", l.Job.ID, "outcome", f.Outcome, "reason", f.Reason)
	if f.Outcome == "checking" {
		go w.watchVerdict(ctx, f)
	}
}

// watchVerdict asks for the verdict until the pipeline decided, for the menu bar.
func (w *Worker) watchVerdict(ctx context.Context, f Finished) {
	for i := 0; i < 120; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
		res, err := w.api.GetResult(ctx, f.LeaseID)
		if err != nil || res.Verdict == "checking" {
			continue
		}
		f.Outcome = res.Verdict
		if res.Reason != nil {
			f.Reason = *res.Reason
		}
		w.finished(f)
		return
	}
}

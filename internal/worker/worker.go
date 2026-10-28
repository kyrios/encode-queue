// Package worker takes jobs from the queue through its tunnel and runs them: ffmpeg with
// libx265 or libx264, or whisper.cpp, always at nice 19.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Config is the worker's settings.
type Config struct {
	Version        string
	HostAppName    string // the app the worker runs inside (Slipstream), with its version
	HostAppVersion string
	StateDir       string
	WorkDir        string
	FFmpeg         string
	WhisperCLI     string
	WhisperVersion string
	// ToolchainVersions is the toolchain's versions.txt when it does not lie beside the
	// binaries (EQ_TOOLCHAIN_VERSIONS, v0.2.8): every tool version but ffmpeg's, x265's and
	// x264's, which the tools print, is read from it. WhisperVersion still wins for whisper.cpp.
	ToolchainVersions string
	ModelsDir         string
	Fonts             []string
	CPUThreads        int // 0: the tools' own
	GPU               bool
	MaxParallel       int // normal slots, which take every job
	// HighPrioritySlots: slots on top of MaxParallel for jobs of priority now and stage1 only
	// (v0.2.14; 0 none). The command's default is 1.
	HighPrioritySlots int
	Calibrate         bool // the calibration clip at pairing and on a tools change (calibrate.go)
	// FetchClient fetches the whisper models the queue wants (fetch.go); nil: the default client.
	FetchClient *http.Client
	ChunkBytes  int64
	Log         *slog.Logger

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
	c.HighPrioritySlots = min(max(c.HighPrioritySlots, 0), maxSlots)
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
	Slot     string    `json:"slot"` // normal or high (v0.2.14)
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
	Points        *Points     `json:"points,omitempty"` // v0.2.15, from the queue
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
	highLimit   int
	limitCh     chan struct{} // closed and replaced when a job limit changes
	runs        map[string]*run
	recent      []Finished
	today       int
	todayDate   string
	pauseCh     chan struct{} // closed and replaced when the pause changes
	quit        chan struct{}
	quitOnce    sync.Once
	unavailable map[string]bool // wanted models a read-only models folder cannot take (fetch.go)
	points      *Points         // the queue's last word on them (notePoints)
	busy        *busySampler    // used by the presence loop only
	machine     *machineSampler // likewise
}

// New is a worker that talks to the queue through api.
func New(cfg Config, api *Client) (*Worker, error) {
	cfg.defaults()
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		return nil, err
	}
	return &Worker{cfg: cfg, api: api, runs: map[string]*run{}, pauseCh: make(chan struct{}), quit: make(chan struct{}),
		jobLimit: cfg.MaxParallel, highLimit: cfg.HighPrioritySlots, limitCh: make(chan struct{}), busy: newBusySampler(), machine: newMachineSampler()}, nil
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

// SetLimits changes the threads, the GPU, the normal slots and, unless high is nil, the
// high-priority slots at once (Slipstream's settings); the queue hears it with the next presence.
func (w *Worker) SetLimits(threads int, gpu bool, jobs int, high *int) {
	jobs = min(max(jobs, 1), maxSlots)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg.CPUThreads, w.cfg.GPU, w.cfg.MaxParallel = threads, gpu, jobs
	w.jobLimit = jobs
	if high != nil {
		w.cfg.HighPrioritySlots = min(max(*high, 0), maxSlots)
		w.highLimit = w.cfg.HighPrioritySlots
	}
	w.limitsPatch = true
	close(w.limitCh)
	w.limitCh = make(chan struct{})
	close(w.pauseCh) // wakes the presence loop
	w.pauseCh = make(chan struct{})
}

func (w *Worker) limits() map[string]any {
	l := map[string]any{"gpu": w.cfg.GPU, "max_parallel_jobs": w.cfg.MaxParallel, "high_priority_slots": w.cfg.HighPrioritySlots}
	if w.cfg.CPUThreads > 0 {
		l["cpu_threads"] = w.cfg.CPUThreads
	}
	return l
}

// maxSlots is the most slots of each kind a worker runs.
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
		Refused: w.refused, Tools: w.caps.Tools, Jobs: []JobStatus{}, Recent: append([]Finished(nil), w.recent...), FinishedToday: w.today, Points: w.points}
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
	// hello gave back every lease a former run left, so no lease folder is in use: what is
	// there is a crash's leftover (v0.2.8). Before the presence loop, which reports the room.
	w.sweepWork()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.presenceLoop(ctx) }()
	// The calibration beside the claims, not before them (v0.2.11; Thorsten's live session,
	// 2026-10-27: both Macs sat "bereit" and claimed nothing while Slipstream 1.3.1's new
	// toolchain made them calibrate again). A calibration beside a job measures low; the first
	// measured result overrides it. A failure leaves the worker uncalibrated, never unusable.
	if w.cfg.Calibrate {
		wg.Add(1)
		go func() { defer wg.Done(); w.calibrateAll(ctx, &wg) }()
	}
	// The normal slots and, beside them, the high-priority ones (v0.2.14), which the queue gives
	// only jobs of priority now and stage1, beside other work only while this machine's RAM allows.
	for i := 0; i < maxSlots; i++ {
		wg.Add(2)
		go func(slot int) { defer wg.Done(); w.claimLoop(ctx, slot, false) }(i)
		go func(slot int) { defer wg.Done(); w.claimLoop(ctx, slot, true) }(i)
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
	w.mu.Lock()
	caps := w.caps // the calibration beside it may change it
	w.mu.Unlock()
	rep, err := w.api.PutCapabilities(ctx, caps)
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
		own := w.busy.sample(w.toolGroups(), time.Now())
		ack, err := w.api.PutPresence(ctx, state, free, memoryUsed(), own, otherLoad(w.machine.sample(), own))
		w.contact(err)
		if err == nil {
			w.setPause(ack.PausedUntil, true)
			if ack.PresenceEveryS > 0 {
				every = time.Duration(ack.PresenceEveryS) * time.Second
			}
			w.notePoints(ack.Points)
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

// notePoints tells Slipstream this worker's points when they or its place changed (v0.2.15).
func (w *Worker) notePoints(p *Points) {
	if p == nil {
		return
	}
	c := *p
	w.mu.Lock()
	same := w.points != nil && *w.points == c
	w.points = &c
	w.mu.Unlock()
	if !same {
		w.emit("points", map[string]any{"table": p.Table, "today": p.Today, "week": p.Week, "total": p.Total, "rank": p.Rank, "of": p.Of})
	}
}

// calibrateAll makes the calibration, times the held models, sends both, and fetches the models
// the queue wants then, all while the claim loops run.
func (w *Worker) calibrateAll(ctx context.Context, wg *sync.WaitGroup) {
	c, err := w.calibrate(ctx)
	if err != nil {
		w.cfg.Log.Warn("calibration failed", "err", err)
		return
	}
	if err := w.calibrateWhisper(ctx, c); err != nil {
		w.cfg.Log.Warn("whisper calibration failed", "err", err)
	}
	w.saveCalibration(c)
	w.mu.Lock()
	w.caps.Calibration = c
	caps := w.caps
	w.mu.Unlock()
	rep, err := w.api.PutCapabilities(ctx, caps)
	if err != nil {
		w.cfg.Log.Warn("calibration not sent", "err", err)
		return
	}
	if len(rep.WantedModels) > 0 {
		// The models the queue wants, in the background too: a 1.6 GB download must not hold
		// back the jobs this worker can run now.
		wg.Add(1)
		go func() { defer wg.Done(); w.modelLoop(ctx, c, rep.WantedModels) }()
	}
}

// modelLoop fetches the models the queue wants, times each new one on the speech clip, and tells
// the queue, which may then want another (a model that measured too slow is dropped from the
// wanted list). At most three rounds.
func (w *Worker) modelLoop(ctx context.Context, c *Calibration, wanted []WantedModel) {
	for round := 0; round < 3 && len(wanted) > 0 && ctx.Err() == nil; round++ {
		if !w.fetchModels(ctx, wanted) {
			return
		}
		held := w.heldModels()
		w.mu.Lock()
		w.caps.Models = w.caps.Models[:0]
		for sha, path := range held {
			w.caps.Models = append(w.caps.Models, Model{Name: filepath.Base(path), SHA256: sha})
		}
		sort.Slice(w.caps.Models, func(i, j int) bool { return w.caps.Models[i].Name < w.caps.Models[j].Name })
		w.mu.Unlock()
		if err := w.calibrateWhisper(ctx, c); err != nil {
			w.cfg.Log.Warn("whisper calibration failed", "err", err)
		}
		w.saveCalibration(c)
		w.mu.Lock()
		w.caps.Calibration = c
		caps := w.caps
		w.mu.Unlock()
		rep, err := w.api.PutCapabilities(ctx, caps)
		if err != nil {
			w.cfg.Log.Warn("capabilities not sent", "err", err)
			return
		}
		wanted = rep.WantedModels
	}
}

// toolGroups are the process groups of the tools running now, one per run.
func (w *Worker) toolGroups() []int {
	w.mu.Lock()
	runs := make([]*run, 0, len(w.runs))
	for _, r := range w.runs {
		runs = append(runs, r)
	}
	w.mu.Unlock()
	var out []int
	for _, r := range runs {
		r.mu.Lock()
		if r.cmd != nil && r.cmd.Process != nil {
			out = append(out, r.cmd.Process.Pid)
		}
		r.mu.Unlock()
	}
	return out
}

// claimLoop is one slot, normal or high-priority; slots at or above their kind's limit wait.
func (w *Worker) claimLoop(ctx context.Context, slot int, high bool) {
	for ctx.Err() == nil {
		w.mu.Lock()
		limit, ch := w.jobLimit, w.limitCh
		if high {
			limit = w.highLimit
		}
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
		l, err := w.api.Claim(ctx, w.cfg.LongPoll, high)
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
		w.runLease(ctx, l, high)
	}
}

// sweepWork deletes the lease folders (`ls_…`) in the work directory that no run of this
// worker holds: what a crash or a kill left behind. Each lease's folder is deleted when it ends.
func (w *Worker) sweepWork() {
	ents, err := os.ReadDir(w.cfg.WorkDir)
	if err != nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "ls_") {
			continue
		}
		if _, held := w.runs[e.Name()]; held {
			continue
		}
		if err := os.RemoveAll(filepath.Join(w.cfg.WorkDir, e.Name())); err == nil {
			w.cfg.Log.Info("deleted a lease folder left behind", "folder", e.Name())
		}
	}
}

func (w *Worker) runLease(ctx context.Context, l *Lease, high bool) {
	r := newRun(w, l, high)
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

package queue

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Config is the queue's behaviour. Addresses and ports are not here: the server owns those.
type Config struct {
	StateDir  string
	SourceDir string
	ResultDir string

	Lease          time.Duration // 120 s
	RenewEvery     time.Duration // 30 s
	LongPoll       time.Duration // 25 s, the most a long poll waits
	PresenceEvery  time.Duration // 30 s; three missed and a worker is offline
	PauseGiveBack  time.Duration // 60 s
	PauseGrace     time.Duration // 30 s more before the queue takes a paused lease back itself
	DoneKeep       time.Duration // 7 days
	MaxFailures    int           // 3
	ExcludeBackoff time.Duration // 10 min: then a job no worker may run lets the reference worker back
	ResultReserve  int64         // bytes the result directory keeps free beyond every lease's max_bytes

	ReferenceWorker string // the worker whose tools jobs require by default; "" requires none

	// **A rogue worker cannot hold everything** (v0.2.9; security review M2, 2026-10-27): a
	// worker sets its own max_parallel_jobs and reports its own calibrated speed, and a lease
	// renewed every 30 s never ran out. MaxParallelJobs caps the normal slots of every worker (2),
	// MaxHighPrioritySlots its high-priority slots on top of them (2, v0.2.14). A lease may be held
	// HoldFactor times its expected completion time (3), at least HoldMin (30 min), and HoldMax
	// (12 h) when nothing is expected; then the queue takes the job back and keeps it from that
	// worker. A calibrated speed counts at most as the reference's until measured results confirm
	// it (calibration.go).
	MaxParallelJobs      int
	MaxHighPrioritySlots int
	HoldFactor           float64
	HoldMin              time.Duration
	HoldMax              time.Duration
	// WhisperMinSpeed: how many times real time a worker must transcribe with a model for the
	// queue to want it there (wantedModels); 0 is 1.
	WhisperMinSpeed float64
	MinAppVersion   string

	// The tunnel, for the configurations the queue hands out.
	Subnet          netip.Prefix
	TunnelAddress   netip.Addr
	ServerPublicKey string
	Endpoint        string
	API             string
	Keepalive       int

	Now       func() time.Time
	FreeBytes func(dir string) (int64, error)
	Log       *slog.Logger
}

// Defaults fills what is unset with ADR-0007's timings.
func (c *Config) Defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.Lease, 120*time.Second)
	set(&c.RenewEvery, 30*time.Second)
	set(&c.LongPoll, 25*time.Second)
	set(&c.PresenceEvery, 30*time.Second)
	set(&c.PauseGiveBack, 60*time.Second)
	set(&c.PauseGrace, 30*time.Second)
	set(&c.DoneKeep, 7*24*time.Hour)
	set(&c.ExcludeBackoff, 10*time.Minute)
	set(&c.HoldMin, 30*time.Minute)
	set(&c.HoldMax, 12*time.Hour)
	if c.MaxParallelJobs == 0 {
		c.MaxParallelJobs = 2
	}
	if c.MaxHighPrioritySlots == 0 {
		c.MaxHighPrioritySlots = 2
	}
	if c.HoldFactor == 0 {
		c.HoldFactor = 3
	}
	if c.MaxFailures == 0 {
		c.MaxFailures = 3
	}
	if c.Keepalive == 0 {
		c.Keepalive = 25
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.FreeBytes == nil {
		c.FreeBytes = freeBytes
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.ResultDir == "" {
		c.ResultDir = filepath.Join(c.StateDir, "results")
	}
}

// Job is a job as the queue keeps it.
type Job struct {
	ID         string         `json:"id"`
	Spec       JobSubmit      `json:"spec"`
	SpecHash   string         `json:"spec_hash"`
	Priority   string         `json:"priority"` // the spec's, until the Encoder page moves the job
	Final      string         `json:"final,omitempty"`
	Rank       float64        `json:"rank"`
	Failures   int            `json:"failures"`
	Excluded   []string       `json:"excluded,omitempty"`
	Attempts   []string       `json:"attempts,omitempty"`
	Accepted   int            `json:"accepted,omitempty"`
	QueuedAt   time.Time      `json:"queued_at"`
	FinishedAt *time.Time     `json:"finished_at,omitempty"`
	Revision   int64          `json:"revision"`
	Pin        *Pin           `json:"pin,omitempty"`
	Cancelled  *Cancelled     `json:"cancelled,omitempty"`
	Killed     map[string]int `json:"killed,omitempty"`    // tool kills per worker, which exclude only the second time
	FailedAt   *time.Time     `json:"failed_at,omitempty"` // the last failure or rejection

	rung    *RungParams
	whisper *WhisperParams
	cut     *CutParams   // v0.2.8
	remux   *RemuxParams // v0.2.8
	check   *CheckParams // v0.2.8
}

// Lease is one attempt at a job.
type Lease struct {
	ID        string    `json:"id"`
	JobID     string    `json:"job_id"`
	WorkerID  string    `json:"worker_id"`
	Worker    string    `json:"worker"` // the name when it was made, for a worker since removed
	Attempt   int       `json:"attempt"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	// HoldUntil: the latest a renewal reaches (v0.2.9); zero for a lease from before it, which
	// gets HoldMax from its start.
	HoldUntil time.Time  `json:"hold_until,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	EndCode   string     `json:"end_code,omitempty"`
	Progress  *Progress  `json:"progress,omitempty"`
	Result    *Result    `json:"result,omitempty"`
	// High: in one of the worker's high-priority slots (v0.2.14), beside its max_parallel_jobs; the
	// state keeps v0.2.5's name for its extra slot.
	High bool `json:"extra,omitempty"`
	// Model: the whisper model the queue chose for this attempt (v0.2.7), the best of the job's
	// models the worker holds; the lease's params carry it as model_sha256.
	Model string `json:"model,omitempty"`
	// The scoring at the claim (v0.2.7): this worker's expected completion time and the best
	// other worker's, and the compute time it expected, to learn from the result.
	ECT      *ECT    `json:"ect,omitempty"`
	RunnerUp *ECT    `json:"runner_up,omitempty"`
	EstRunS  float64 `json:"est_run_s,omitempty"`
	// Scoring: why the claim had the comparison it had (v0.2.10, scoringNote).
	Scoring string `json:"scoring,omitempty"`
	// Points: what its accepted result earned (v0.2.15, points.go).
	Points float64 `json:"points,omitempty"`
}

// Result is what an attempt delivered.
type Result struct {
	Outcome     string  `json:"outcome"`
	Verdict     string  `json:"verdict"`
	SHA256      string  `json:"sha256,omitempty"`
	Bytes       int64   `json:"bytes,omitempty"`
	Frames      int64   `json:"frames,omitempty"`
	EncodeS     float64 `json:"encode_s,omitempty"`
	Tools       []Tool  `json:"tools,omitempty"`
	Path        string  `json:"path,omitempty"` // relative to the result directory, while the file exists
	FailureCode string  `json:"failure_code,omitempty"`
	Message     string  `json:"message,omitempty"`
	Signal      string  `json:"signal,omitempty"`
	ExitCode    *int    `json:"exit_code,omitempty"`
	MemoryAvail *int64  `json:"memory_available_bytes,omitempty"`
	Checks      []Check `json:"checks,omitempty"`
	// Figures: what the worker's check found (v0.2.8), as the worker wrote it; the pipeline
	// judges them.
	Figures     json.RawMessage `json:"figures,omitempty"`
	DeliveredAt time.Time       `json:"delivered_at"`
	DecidedAt   *time.Time      `json:"decided_at,omitempty"`
}

// Worker is a paired worker.
type Worker struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Owner     string     `json:"owner,omitempty"`
	PublicKey string     `json:"public_key"`
	Address   string     `json:"address"`
	AddedAt   time.Time  `json:"added_at"`
	LastSeen  *time.Time `json:"last_seen,omitempty"`
	// AskedAt: when it last asked for work (v0.2.10), what makes it a competitor in the scoring.
	AskedAt     *time.Time    `json:"asked_at,omitempty"`
	PausedUntil *time.Time    `json:"paused_until,omitempty"`
	PausedAt    *time.Time    `json:"paused_at,omitempty"`
	Limits      Limits        `json:"limits"`
	Caps        *Capabilities `json:"capabilities,omitempty"`
	Presence    string        `json:"presence,omitempty"`
	DiskFree    *int64        `json:"disk_free_bytes,omitempty"`
	Counts      Counts        `json:"counts"`
	Download    TransferStats `json:"download"`
	Upload      TransferStats `json:"upload"`
	Speeds      []*SpeedHist  `json:"speeds,omitempty"` // most recent first
	MemoryUsed  *int64        `json:"memory_used_bytes,omitempty"`
	ThreadsBusy *int          `json:"threads_busy,omitempty"`
	// OtherLoad: threads other programs keep busy, from the last presence (v0.2.7).
	OtherLoad *float64 `json:"other_load,omitempty"`
	// Corrections: actual over estimated compute time by job kind, learned (v0.2.7, ect.go).
	Corrections map[string]float64 `json:"corrections,omitempty"`
	// Points: what its accepted results earned (v0.2.15, points.go).
	Points *PointsLedger `json:"points,omitempty"`

	tunnelSince *time.Time // when the tunnel last came up; not kept over a restart
	// memoryAt: when a presence last said memory_used_bytes. A lease claimed since is not in that
	// figure yet (ramFits); zero after a restart, so every active lease counts until the next.
	memoryAt time.Time
	// ramPreemptAt: when the queue last took a lease of this worker back for a `now` job's RAM
	// (clearing, slots.go); not kept over a restart.
	ramPreemptAt time.Time
}

// Pin is the worker the Encoder page sent a job to.
type Pin struct {
	WorkerID string    `json:"worker_id"`
	At       time.Time `json:"at"`
}

// Upload is a tus upload of a result.
type Upload struct {
	ID        string    `json:"id"`
	LeaseID   string    `json:"lease_id"`
	Length    int64     `json:"length"`
	Offset    int64     `json:"offset"`
	CreatedAt time.Time `json:"created_at"`
	LastWrite time.Time `json:"last_write"`
	busy      bool
}

type forgotten struct {
	JobID    string `json:"job_id"`
	Revision int64  `json:"revision"`
}

type state struct {
	Version   int                `json:"version"`
	Revision  int64              `json:"revision"`
	Jobs      map[string]*Job    `json:"jobs"`
	Leases    map[string]*Lease  `json:"leases"`
	Workers   map[string]*Worker `json:"workers"`
	Uploads   map[string]*Upload `json:"uploads"`
	Forgotten []forgotten        `json:"forgotten,omitempty"`
}

// Store is the queue. Every method is safe for concurrent use.
type Store struct {
	cfg  Config
	mu   sync.Mutex
	st   *state
	logs map[string]*logRing

	changed  chan struct{} // closed and replaced on every change
	dirty    bool
	saveReq  chan struct{}
	draining bool
	// started: when this queue opened its state. Its downtime does not count against a worker
	// or a lease (Open, connected).
	started time.Time

	// OnPeers is called, with the lock held, whenever the peer list changes.
	OnPeers func(peers []PeerInfo)
	// PeerStatus is what WireGuard knows of each peer, by public key (base64); it is called
	// with the lock held. Nil: nothing is known.
	PeerStatus func() map[string]PeerStatus
}

// PeerStatus is a peer as WireGuard sees it.
type PeerStatus struct {
	Endpoint      string    // IP:port of the peer's last packet; "" before one
	LastHandshake time.Time // zero before the first
}

// PeerInfo is one worker's WireGuard peer.
type PeerInfo struct {
	PublicKey string
	Address   netip.Addr
}

// Open loads the queue's state from cfg.StateDir, or starts empty.
func Open(cfg Config) (*Store, error) {
	cfg.Defaults()
	for _, d := range []string{cfg.StateDir, cfg.ResultDir, filepath.Join(cfg.ResultDir, ".uploads")} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return nil, err
		}
	}
	s := &Store{cfg: cfg, logs: map[string]*logRing{}, changed: make(chan struct{}), saveReq: make(chan struct{}, 1)}
	st, err := load(s.statePath())
	if err != nil {
		return nil, err
	}
	s.st = st
	// **A restart keeps the leases** (v0.2.6): a worker heard of 30 s before a stop of a minute
	// and calling again 30 s after the start was "offline" for the housekeeping, and its encode
	// went back to the queue. Since the start counts as the last contact, and every active lease
	// gets one lease time from the start, a worker that comes back keeps its work; one that does
	// not is offline 3 presences after the start, as before.
	s.started = cfg.Now()
	for _, l := range st.Leases {
		if l.State == LeaseActive && l.ExpiresAt.Before(s.started.Add(cfg.Lease)) {
			l.ExpiresAt = s.started.Add(cfg.Lease)
		}
	}
	for _, j := range st.Jobs {
		if j.Priority == "" {
			j.Priority = j.Spec.Priority
		}
		if err := j.parse(); err != nil {
			return nil, fmt.Errorf("job %s in the state: %w", j.ID, err)
		}
	}
	// An upload's offset is what is on disk.
	for _, u := range st.Uploads {
		if fi, err := os.Stat(s.uploadPath(u.ID)); err == nil {
			u.Offset = fi.Size()
		} else {
			u.Offset = 0
		}
	}
	return s, nil
}

func (s *Store) statePath() string           { return filepath.Join(s.cfg.StateDir, "state.json") }
func (s *Store) uploadPath(id string) string { return filepath.Join(s.cfg.ResultDir, ".uploads", id) }

// Config is the configuration the store runs with, its defaults filled in.
func (s *Store) Config() Config { return s.cfg }

// Peers is the current peer list.
func (s *Store) Peers() []PeerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peers()
}

func (s *Store) peers() []PeerInfo {
	var out []PeerInfo
	for _, w := range s.st.Workers {
		a, err := netip.ParseAddr(w.Address)
		if err != nil {
			continue
		}
		out = append(out, PeerInfo{PublicKey: w.PublicKey, Address: a})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address.Less(out[j].Address) })
	return out
}

// Drain stops new leases (a shutting-down queue).
func (s *Store) Drain() {
	s.mu.Lock()
	s.draining = true
	s.touch(true)
	s.mu.Unlock()
}

// touch records a change: it wakes every long poll and asks for a save. important saves at
// once (before the caller answers); otherwise the saver writes within a second.
func (s *Store) touch(important bool) {
	close(s.changed)
	s.changed = make(chan struct{})
	s.dirty = true
	if important {
		if err := s.saveLocked(); err != nil {
			s.cfg.Log.Error("save the state", "err", err)
		}
		return
	}
	select {
	case s.saveReq <- struct{}{}:
	default:
	}
}

func (s *Store) bump(j *Job) {
	s.st.Revision++
	j.Revision = s.st.Revision
}

// RunSaver writes the state when it changed, at most once a second, until stop is closed;
// then it writes once more.
func (s *Store) RunSaver(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			s.Save()
			return
		case <-t.C:
			s.Save()
		}
	}
}

// Save writes the state if it changed.
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	data, err := json.Marshal(s.st)
	if err != nil {
		return err
	}
	if err := writeAtomic(s.statePath(), data); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

func load(path string) (*state, error) {
	st := &state{Version: 1, Jobs: map[string]*Job{}, Leases: map[string]*Lease{}, Workers: map[string]*Worker{}, Uploads: map[string]*Upload{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if st.Version != 1 {
		return nil, fmt.Errorf("%s: state version %d, this queue reads 1", path, st.Version)
	}
	if st.Jobs == nil {
		st.Jobs = map[string]*Job{}
	}
	if st.Leases == nil {
		st.Leases = map[string]*Lease{}
	}
	if st.Workers == nil {
		st.Workers = map[string]*Worker{}
	}
	if st.Uploads == nil {
		st.Uploads = map[string]*Upload{}
	}
	return st, nil
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".new"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// ---------------------------------------------------------------- ids and small helpers

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newLeaseID is a ULID: 48 bits of milliseconds, 80 random bits, Crockford base32.
func newLeaseID(now time.Time) string {
	var b [16]byte
	ms := uint64(now.UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	rand.Read(b[6:])
	// 128 bits in 26 characters, the first holding 3 bits.
	var out [26]byte
	var acc uint64
	bits := 0
	idx := 25
	for i := 15; i >= 0; i-- {
		acc |= uint64(b[i]) << bits
		bits += 8
		for bits >= 5 && idx >= 0 {
			out[idx] = crockford[acc&31]
			acc >>= 5
			bits -= 5
			idx--
		}
	}
	for idx >= 0 {
		out[idx] = crockford[acc&31]
		acc >>= 5
		idx--
	}
	return "ls_" + string(out[:])
}

func randomID(prefix string, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return prefix + string(b)
}

func hashSpec(spec JobSubmit) string {
	data, _ := json.Marshal(spec)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

func ptrTime(t time.Time) *time.Time { return &t }

func (j *Job) parse() error {
	j.rung, j.whisper, j.cut, j.remux, j.check = nil, nil, nil, nil, nil
	switch j.Spec.Kind {
	case KindCut:
		var p CutParams
		if err := strictUnmarshal(j.Spec.Params, &p); err != nil {
			return err
		}
		j.cut = &p
	case KindRemux:
		var p RemuxParams
		if err := strictUnmarshal(j.Spec.Params, &p); err != nil {
			return err
		}
		j.remux = &p
	case KindCheck:
		var p CheckParams
		if err := strictUnmarshal(j.Spec.Params, &p); err != nil {
			return err
		}
		j.check = &p
	case KindRung:
		var p RungParams
		if err := strictUnmarshal(j.Spec.Params, &p); err != nil {
			return err
		}
		j.rung = &p
	case KindWhisper:
		var p WhisperParams
		if err := strictUnmarshal(j.Spec.Params, &p); err != nil {
			return err
		}
		j.whisper = &p
	default:
		return fmt.Errorf("unknown kind %q", j.Spec.Kind)
	}
	return nil
}

func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- logs

const logKeep = 2000

// LogLine is one log line of a lease.
type LogLine struct {
	Seq     int64     `json:"seq"`
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Source  string    `json:"source,omitempty"`
	Message string    `json:"message"`
}

type logRing struct {
	lines []LogLine
	next  int64
}

func (r *logRing) add(l LogLine) {
	r.next++
	l.Seq = r.next
	r.lines = append(r.lines, l)
	if len(r.lines) > logKeep {
		r.lines = append([]LogLine(nil), r.lines[len(r.lines)-logKeep:]...)
	}
}

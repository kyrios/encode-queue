// Package queue keeps the jobs, their leases and attempts, and the workers, and decides which
// worker gets which job. It knows nothing of HTTP or WireGuard; internal/server puts the two
// APIs in front of it.
package queue

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job kinds.
const (
	KindRung    = "rung"
	KindWhisper = "whisper"
)

// Priorities, first first (ADR-0005's order).
var Priorities = []string{"stage1", "stage2", "archive"}

func prioIndex(p string) int {
	for i, q := range Priorities {
		if q == p {
			return i
		}
	}
	return len(Priorities)
}

// Job states. The first three are derived from the leases; the last three are stored.
const (
	StateQueued    = "queued"
	StateLeased    = "leased"
	StateChecking  = "checking"
	StateDone      = "done"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

// Lease states.
const (
	LeaseActive    = "active"
	LeaseDelivered = "delivered"
	LeaseGivenBack = "given_back"
	LeaseExpired   = "expired"
	LeasePreempted = "preempted"
	LeaseCancelled = "cancelled"
	LeaseFailed    = "failed"
)

// Verdicts.
const (
	VerdictChecking   = "checking"
	VerdictAccepted   = "accepted"
	VerdictRejected   = "rejected"
	VerdictSuperseded = "superseded"
	VerdictFailed     = "failed"
)

// Tool is a tool and its exact version string.
type Tool struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Model is a whisper model by its sha256.
type Model struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

// Limits is how much of its machine a worker may use.
type Limits struct {
	CPUThreads      *int `json:"cpu_threads,omitempty"`
	GPU             bool `json:"gpu"`
	MaxParallelJobs int  `json:"max_parallel_jobs"`
}

// Capabilities is what a worker says it runs.
type Capabilities struct {
	AppVersion string          `json:"app_version"`
	OS         string          `json:"os"`
	Tools      []Tool          `json:"tools"`
	Models     []Model         `json:"models,omitempty"`
	Fonts      []string        `json:"fonts,omitempty"`
	Hardware   json.RawMessage `json:"hardware,omitempty"`
}

func (c *Capabilities) tool(name string) (string, bool) {
	for _, t := range c.Tools {
		if t.Name == name {
			return t.Version, true
		}
	}
	return "", false
}

// Requires is what a worker must run to get a job.
type Requires struct {
	Tools  []Tool   `json:"tools,omitempty"`
	Models []string `json:"models,omitempty"`
	Fonts  []string `json:"fonts,omitempty"`
}

// RungParams are an encode's settings (helper.yaml).
type RungParams struct {
	Rung       string    `json:"rung,omitempty"`
	Codec      string    `json:"codec"`
	Preset     string    `json:"preset"`
	CRF        float64   `json:"crf"`
	Width      int       `json:"width"`
	Height     int       `json:"height"`
	Crop       *Crop     `json:"crop,omitempty"`
	VBVMaxrate *int      `json:"vbv_maxrate_kbps,omitempty"`
	VBVBufsize *int      `json:"vbv_bufsize_kbit,omitempty"`
	Audio      *Audio    `json:"audio,omitempty"`
	Captions   *Captions `json:"captions,omitempty"`
}

// Crop is a crop before the scale.
type Crop struct {
	Width  int `json:"width"`
	Height int `json:"height"`
	X      int `json:"x"`
	Y      int `json:"y"`
}

// Audio is what happens to the sound.
type Audio struct {
	Mode        string `json:"mode"`
	BitrateKbps int    `json:"bitrate_kbps,omitempty"`
	Mono        bool   `json:"mono,omitempty"`
}

// Captions are burnt in by libass from a job input.
type Captions struct {
	Input string `json:"input"`
	Font  string `json:"font"`
}

// WhisperParams are a whisper run's settings.
type WhisperParams struct {
	ModelSHA256 string `json:"model_sha256"`
	Model       string `json:"model,omitempty"`
	Language    string `json:"language"`
	Threads     int    `json:"threads,omitempty"`
}

// FileIn is a file under the source directory.
type FileIn struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// SourceIn is a job's source.
type SourceIn struct {
	FileIn
	Container string  `json:"container,omitempty"`
	DurationS float64 `json:"duration_s,omitempty"`
	Frames    int64   `json:"frames,omitempty"`
	Width     int     `json:"width,omitempty"`
	Height    int     `json:"height,omitempty"`
}

// ResultSpec is what the result must be.
type ResultSpec struct {
	Name     string `json:"name"`
	MaxBytes int64  `json:"max_bytes"`
}

// JobSubmit is a job as the pipeline puts it.
type JobSubmit struct {
	Kind        string            `json:"kind"`
	Priority    string            `json:"priority"`
	UploadID    string            `json:"upload_id,omitempty"`
	Title       string            `json:"title,omitempty"`
	Stage       *int              `json:"stage,omitempty"`
	Params      json.RawMessage   `json:"params"`
	Source      SourceIn          `json:"source"`
	Inputs      map[string]FileIn `json:"inputs,omitempty"`
	Result      ResultSpec        `json:"result"`
	Requires    *Requires         `json:"requires,omitempty"`
	MaxFailures int               `json:"max_failures,omitempty"`
}

// Progress is a worker's last report on a lease.
type Progress struct {
	Phase       string     `json:"phase"`
	Fraction    *float64   `json:"fraction,omitempty"`
	FramesDone  *int64     `json:"frames_done,omitempty"`
	FramesTotal *int64     `json:"frames_total,omitempty"`
	FPS         *float64   `json:"fps,omitempty"`
	Speed       *float64   `json:"speed,omitempty"`
	ETAS        *int64     `json:"eta_s,omitempty"`
	BytesDone   *int64     `json:"bytes_done,omitempty"`
	BytesTotal  *int64     `json:"bytes_total,omitempty"`
	PausedSince *time.Time `json:"paused_since,omitempty"`
}

// Check is one of the pipeline's checks, passed through for the worker to show.
type Check = json.RawMessage

// Counts is every attempt a worker ended, by how it ended.
type Counts struct {
	Accepted   int64 `json:"accepted"`
	Rejected   int64 `json:"rejected"`
	Superseded int64 `json:"superseded"`
	Failed     int64 `json:"failed"`
	GivenBack  int64 `json:"given_back"`
	Expired    int64 `json:"expired"`
	Preempted  int64 `json:"preempted"`
	Cancelled  int64 `json:"cancelled"`
}

// Error is a refusal with an HTTP status and a stable code.
type Error struct {
	Status int
	Code   string
	Detail string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Detail) }

func errf(status int, code, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Detail: fmt.Sprintf(format, args...)}
}

var toolsByKind = map[string][]string{
	KindRung:    {"ffmpeg", "libx265", "libx264"},
	KindWhisper: {"whisper.cpp"},
}

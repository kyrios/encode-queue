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
	// KindAlign times the words of a text in the source's sound (v0.2.16, internal/aligner).
	KindAlign = "align"
)

// Priorities, first first (ADR-0005's order). "now" (the Encoder page's "Sofort", v0.2.5) is for a
// job someone in Funkhaus waits for: captions from the audio, a Loop. It goes first in line, and
// with "stage1" may run in a worker's high-priority slots beside a long encode (slots.go).
const PriorityNow = "now"

var Priorities = []string{PriorityNow, "stage1", "stage2", "archive"}

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

// HostApp is the app a worker runs inside (Slipstream); never compared with a minimum.
type HostApp struct {
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
	// HighPrioritySlots: the slots on top of max_parallel_jobs that take only jobs of priority now
	// and stage1 (v0.2.14, slots.go). A worker before 0.2.14 does not send it; it has one, v0.2.5's
	// extra slot.
	HighPrioritySlots *int `json:"high_priority_slots,omitempty"`
	// Efficiency: which jobs run on the efficiency cores only, in macOS's background QoS
	// (v0.2.18): "all", "archive" (jobs of priority archive) or empty for none. EfficiencySpeed
	// is the share of the machine's full speed such a job gets, the worker's estimate (ect.go).
	Efficiency      string   `json:"efficiency,omitempty"`
	EfficiencySpeed *float64 `json:"efficiency_speed,omitempty"`
}

// Capabilities is what a worker says it runs.
type Capabilities struct {
	AppVersion string          `json:"app_version"`
	OS         string          `json:"os"`
	HostApp    *HostApp        `json:"host_app,omitempty"`
	Tools      []Tool          `json:"tools"`
	Models     []Model         `json:"models,omitempty"`
	Fonts      []string        `json:"fonts,omitempty"`
	Hardware   json.RawMessage `json:"hardware,omitempty"`
	// Calibration: the worker's fps on the calibration clip per rung class (v0.2.5).
	Calibration *Calibration `json:"calibration,omitempty"`
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
	// Check: what the worker checks of its result in the same lease (v0.2.8).
	Check *CheckSettings `json:"check,omitempty"`
	// FPS: the frame rate the file goes out at (v0.2.13, a clip for Instagram and Threads, 23 to
	// 60): `fps=` in the filter chain after the crop. Without it the file keeps the source's.
	FPS *FPS `json:"fps,omitempty"`
	// EditList false: the MP4 has no edit lists (v0.2.13, `-use_editlist 0`; Instagram's and
	// Threads' specs want none). Absent or true, ffmpeg writes them where they are needed.
	EditList *bool `json:"edit_list,omitempty"`
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
	Mono        Mono   `json:"mono,omitempty"`
	// MonoSideBelowDB: with mono "measure", how far below the mid the side must be (40 dB).
	MonoSideBelowDB *float64 `json:"mono_side_below_db,omitempty"`
	// SampleRate and Channels: the AAC's rate and channels (v0.2.13, `-ar` and `-ac`; a clip has
	// at most 48 kHz and two). Without them it keeps the source's.
	SampleRate int `json:"sample_rate,omitempty"`
	Channels   int `json:"channels,omitempty"`
}

// Captions are burnt in by libass from a job input.
type Captions struct {
	Input string `json:"input"`
	Font  string `json:"font"`
}

// WhisperParams are a whisper run's settings.
type WhisperParams struct {
	ModelSHA256 string `json:"model_sha256,omitempty"`
	Model       string `json:"model,omitempty"`
	Language    string `json:"language"`
	Threads     int    `json:"threads,omitempty"`
	// Models: the acceptable models by sha256, best first (v0.2.7; Thorsten, 2026-10-27:
	// large-v3-turbo where it runs fast enough, medium as the fallback). The queue gives the job
	// to a worker holding the first one it can, and writes the model it chose into the lease's
	// model_sha256, so every worker runs exactly that one.
	Models []string `json:"models,omitempty"`
}

// acceptable is the job's models, best first: Models, or the one model_sha256.
func (p *WhisperParams) acceptable() []string {
	if len(p.Models) > 0 {
		return p.Models
	}
	return []string{p.ModelSHA256}
}

// AlignParams are an align job's settings (v0.2.16): Qwen3-ForcedAligner by its sha256 (the
// manifest of its files, aligner.SHA256), whisper's language code, and the input that holds the
// words, {"words": [{"text", "start", "end"}]}, with the times the text already has, which only
// cut the pieces. The result is whisper-cli's JSON, one segment per word.
type AlignParams struct {
	ModelSHA256 string `json:"model_sha256"`
	Model       string `json:"model,omitempty"`
	Language    string `json:"language"`
	Text        string `json:"text,omitempty"`
}

// input is the name of the input that holds the words: Text, or "text".
func (p *AlignParams) input() string {
	if p.Text == "" {
		return "text"
	}
	return p.Text
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
	// ExpectedBytes: what the result is likely to weigh, for the time to deliver it (v0.2.7,
	// optional; the scoring counts 0 without it).
	ExpectedBytes int64 `json:"expected_bytes,omitempty"`
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
	Order       *float64          `json:"order,omitempty"`
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
	OutTimeS    *float64   `json:"out_time_s,omitempty"`
	TotalSize   *int64     `json:"total_size_bytes,omitempty"`
	BitrateKbps *float64   `json:"bitrate_kbps,omitempty"`
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
	KindAlign:   nil, // its engine, any of aligner.Engines (jobs.go), and ffmpeg for a source not in WAV
	KindCut:     {"ffmpeg", "libx264"},
	KindRemux:   {"ffmpeg"},
	KindCheck:   {"ffmpeg"},
}

package queue

import (
	"fmt"
	"regexp"
)

// **The peaks and strip jobs** (v0.2.22; Thorsten, 2026-10-28: "0 encoding/calculating/handling
// of media is happening outside of Slipstream instances. Funkhaus is 'just' the job manager,
// event manager, file manager and UI"). The two things video-pipeline still decoded itself are
// worker jobs now: the sound's peaks for Funkhaus's waveform (internal/worker/peaks.go) and the
// timeline's filmstrip of a stage's h264.mp4 (internal/worker/strip.go). Someone in Funkhaus
// waits for both, so they are realtime: priority now unless the pipeline says otherwise, which
// puts them first in line and in a worker's high-priority slot beside a long encode. Each is
// seconds of ffmpeg for an hour of media and a small result.

const (
	// KindPeaks is the sound's peaks of a source, upload-center's peaks file.
	KindPeaks = "peaks"
	// KindStrip is a video's filmstrip: a tar of thumbs.json and its JPEG sheets.
	KindStrip = "strip"
	// KindLoudness is a source's loudness by EBU R 128 (v0.2.24): LUFS, LRA, true peak and the
	// short-term loudness every second, as JSON (internal/worker/loudness.go).
	KindLoudness = "loudness"
)

// maxMediaResult is the most max_bytes of a peaks or strip job: a day of sound makes 23 MB of
// peaks, ten hours of video 12 MB of sheets.
const maxMediaResult = 64 << 20

// mediaPeakBytes is what either takes at its peak: ffmpeg measured 71 MB (peaks) and 101 MB
// (strip) on a two-hour h264.mp4, and 272 MB for the strip of a 4K 10-bit HEVC with slice
// threads (v0.2.23), 2026-10-28.
const mediaPeakBytes = 1 << 29

// PeaksParams are a peaks job's settings: none yet, `{}`.
type PeaksParams struct{}

// LoudnessParams are a loudness job's settings: none yet, `{}` (v0.2.24).
type LoudnessParams struct{}

// StripParams are a strip job's settings, each optional (internal/worker/strip.go).
type StripParams struct {
	Height     int     `json:"height,omitempty"`       // the pictures' height, 90 px
	FineEveryS float64 `json:"fine_every_s,omitempty"` // the fine level's spacing, 4 s
	Scenes     int     `json:"scenes,omitempty"`       // the scenes level's most pictures, 32 (one 8×4 sheet)
	From       string  `json:"from,omitempty"`         // the source's name in thumbs.json, h264.mp4
}

var stripFromRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// mediaKind says whether the job is a peaks, a strip or a loudness job.
func mediaKind(j *Job) bool { return j.peaks != nil || j.strip != nil || j.loudness != nil }

// defaultMediaPriority makes a peaks or strip job without a priority realtime.
func defaultMediaPriority(spec *JobSubmit) {
	if spec.Priority == "" && (spec.Kind == KindPeaks || spec.Kind == KindStrip || spec.Kind == KindLoudness) {
		spec.Priority = PriorityNow
	}
}

// validateMedia checks a peaks or strip job's params and result; nil for the other kinds.
func validateMedia(j *Job, spec *JobSubmit) error {
	if !mediaKind(j) {
		return nil
	}
	if spec.Result.MaxBytes > maxMediaResult {
		return fmt.Errorf("a %s job's result is at most %d bytes", spec.Kind, maxMediaResult)
	}
	if p := j.strip; p != nil {
		switch {
		case p.Height != 0 && (p.Height < 16 || p.Height > 360 || p.Height%2 != 0):
			return fmt.Errorf("strip height is even, 16 to 360")
		case p.FineEveryS != 0 && (p.FineEveryS < 0.5 || p.FineEveryS > 600):
			return fmt.Errorf("strip fine_every_s is 0.5 to 600")
		case p.Scenes != 0 && (p.Scenes < 1 || p.Scenes > 32):
			return fmt.Errorf("strip scenes is 1 to 32, one 8×4 sheet")
		case p.From != "" && !stripFromRe.MatchString(p.From):
			return fmt.Errorf("strip from %q does not match %s", p.From, stripFromRe)
		}
	}
	return nil
}

// mediaSeconds is a peaks, strip or loudness job's time: measured on this Mac's ffmpeg, a
// two-hour h264.mp4's sound in 10 s (its loudness as long) and its strip in 13 s; ffmpeg's start
// on top. A strip of a source
// larger than 1080p (v0.2.23: an upload, 4K HEVC with a keyframe a second) takes 21 ms a
// keyframe in each of its two passes, 5 minutes for two hours.
func mediaSeconds(j *Job) (float64, bool) {
	src := j.Spec.Source
	d := src.DurationS
	if d <= 0 {
		return 0, false
	}
	switch {
	case j.strip != nil && src.Width*src.Height > 1920*1080:
		return 1 + d/25, true
	case j.strip != nil:
		return 1 + d/550, true
	}
	return 1 + d/700, true
}

package queue

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// **Calibrated speed** (v0.2.5; Thorsten, 2026-10-27: "Calibration clip"). Every worker encodes the
// same short clip once per rung class at pairing and on a tools change (the worker's
// calibrate.go) and reports its fps with its capabilities. Until a worker has measured results
// on a class, its fps there against the reference's on the same clip is its relative speed; a
// measured result overrides it.

// Calibration is a worker's report: the clip it made, the tools it made it with, its fps per class.
type Calibration struct {
	ClipSHA256 string            `json:"clip_sha256"`
	Tools      []Tool            `json:"tools"`
	At         time.Time         `json:"at"`
	Classes    []CalibratedClass `json:"classes"`
	// Whisper: each model the worker holds, timed on the calibration speech (v0.2.7).
	Whisper []CalibratedModel `json:"whisper,omitempty"`
}

// CalibratedModel is a whisper model's speed on the calibration speech: seconds of audio per
// second, 1 is real time; Probe when only the first seconds ran (far too slow for the rest).
type CalibratedModel struct {
	SHA256  string  `json:"sha256"`
	Name    string  `json:"name"`
	Whisper string  `json:"whisper"`
	GPU     bool    `json:"gpu"`
	Threads int     `json:"threads"`
	AudioS  float64 `json:"audio_s"`
	Speed   float64 `json:"speed"`
	Probe   bool    `json:"probe,omitempty"`
}

// CalibratedClass is one class's result.
type CalibratedClass struct {
	Class  string  `json:"class"`
	Frames int     `json:"frames"`
	FPS    float64 `json:"fps"`
}

const maxCalibratedClasses = 32

// checkCalibration drops what cannot be a calibration: bad numbers, too many classes, tools
// other than the ones the worker reports beside it.
func checkCalibration(c *Capabilities) {
	cal := c.Calibration
	if cal == nil {
		return
	}
	if len(cal.ClipSHA256) != 64 || len(cal.Classes) == 0 || len(cal.Classes) > maxCalibratedClasses {
		c.Calibration = nil
		return
	}
	for _, t := range cal.Tools {
		if v, ok := c.tool(t.Name); !ok || v != t.Version {
			c.Calibration = nil // made with tools this worker no longer runs
			return
		}
	}
	whisper, _ := c.tool("whisper.cpp")
	var models []CalibratedModel
	for _, m := range cal.Whisper {
		if sha256Re.MatchString(m.SHA256) && m.Whisper == whisper && m.Speed > 0 && !math.IsInf(m.Speed, 0) && !math.IsNaN(m.Speed) && len(models) < 16 {
			models = append(models, m)
		}
	}
	cal.Whisper = models
	var keep []CalibratedClass
	for _, cc := range cal.Classes {
		if cc.FPS > 0 && !math.IsInf(cc.FPS, 0) && !math.IsNaN(cc.FPS) && cc.Class != "" && len(cc.Class) <= 80 {
			keep = append(keep, cc)
		}
	}
	if len(keep) == 0 {
		c.Calibration = nil
		return
	}
	cal.Classes = keep
}

// calibratedFPS is the worker's calibrated fps on a class, or 0.
func calibratedFPS(w *Worker, class string) (fps float64, clip string) {
	if w.Caps == nil || w.Caps.Calibration == nil {
		return 0, ""
	}
	for _, cc := range w.Caps.Calibration.Classes {
		if cc.Class == class {
			return cc.FPS, w.Caps.Calibration.ClipSHA256
		}
	}
	return 0, ""
}

// calibratedRelative is the worker's calibrated fps on a class over the reference's on the same
// clip, or nil; at most trustCalibrated (v0.2.9): the worker reports its calibration itself, so
// until measured results confirm more, it counts as at most four times the reference (security
// review M2, 2026-10-27: a rogue reporting 1e12 fps won every job). The Mac Studio is about 3×.
func (s *Store) calibratedRelative(w *Worker, class string) *float64 {
	ref := s.referenceWorker()
	if ref == nil {
		return nil
	}
	f, clip := calibratedFPS(w, class)
	rf, rclip := calibratedFPS(ref, class)
	if f <= 0 || rf <= 0 || clip != rclip {
		return nil
	}
	r := math.Min(f/rf, trustCalibrated)
	return &r
}

// trustCalibrated is the most a calibrated speed counts, relative to the reference.
const trustCalibrated = 4.0

// calibratedClasses are the classes a worker has calibrated, sorted.
func calibratedClasses(w *Worker) []string {
	if w.Caps == nil || w.Caps.Calibration == nil {
		return nil
	}
	var out []string
	for _, cc := range w.Caps.Calibration.Classes {
		out = append(out, cc.Class)
	}
	sort.Strings(out)
	return out
}

// parseClass splits "libx265 3840x2160 slower" into its parts; zero values where it does not fit.
func parseClass(c string) (codec string, width, height int, preset string) {
	var size string
	fmt.Sscanf(c, "%s %s %s", &codec, &size, &preset)
	fmt.Sscanf(size, "%dx%d", &width, &height)
	return
}

// whisperSpeed is the worker's speed with a model, seconds of audio per second: measured on the
// calibration speech, or guessed from small's measured speed and the catalogue's cost; false
// when neither is there.
func whisperSpeed(w *Worker, sha string) (float64, bool) {
	if w.Caps == nil || w.Caps.Calibration == nil {
		return 0, false
	}
	var small float64
	for _, m := range w.Caps.Calibration.Whisper {
		if m.SHA256 == sha {
			return m.Speed, true
		}
		if m.SHA256 == SHASmall {
			small = m.Speed
		}
	}
	c, ok := catalogue[sha]
	if !ok || small <= 0 || c.Cost <= 0 {
		return 0, false
	}
	return small / c.Cost, true
}

// wantedModels are the models the queue wants a worker to hold (v0.2.7; Thorsten, 2026-10-27:
// "Choose the whisper model based on measured performance at first launch"): small for the
// caption timing, and the best model it runs at least WhisperMinSpeed times real time, by its
// calibration, that fits its RAM with the headroom. Before small is timed, only small.
//
// A model the worker has not timed is guessed from small's speed, and wanted only when no model
// below it in quality measured too slow there (v0.2.8): on encoder small ran at 2.87×, medium
// at 0.96×, and the guess for large-v3-turbo said 1.4×, a 1.6 GB download for a model that
// would run slowly. When no model clears the threshold, the best one it holds stays wanted as
// its fallback (encoder's medium), whatever its speed.
func (s *Store) wantedModels(w *Worker) []CatalogueModel {
	if w.Caps == nil {
		return nil
	}
	out := []CatalogueModel{catalogue[SHASmall]}
	min := s.cfg.WhisperMinSpeed
	if min <= 0 {
		min = 1
	}
	total := s.system(w).MemoryBytes
	for i, sha := range Quality {
		if sha == SHASmall {
			break
		}
		c := catalogue[sha]
		if total != nil && c.PeakBytes+headroom > *total {
			continue
		}
		v, ok := whisperSpeed(w, sha)
		if !ok || v < min {
			continue
		}
		if !measured(w, sha) && slowerBelow(w, Quality[i+1:], min) {
			continue // a guess, against a measured model below it that is too slow
		}
		out = append(out, c)
		return out
	}
	// Nothing clears the threshold: once small is timed, the best model it holds (but small) is
	// its fallback.
	if !measured(w, SHASmall) {
		return out
	}
	for _, sha := range Quality {
		if sha == SHASmall {
			break
		}
		if holds(w, sha) {
			return append(out, catalogue[sha])
		}
	}
	return out
}

// measured says whether the worker's calibration timed the model.
func measured(w *Worker, sha string) bool {
	if w.Caps == nil || w.Caps.Calibration == nil {
		return false
	}
	for _, m := range w.Caps.Calibration.Whisper {
		if m.SHA256 == sha {
			return true
		}
	}
	return false
}

// slowerBelow says whether one of these models (below in quality, small left out) measured
// slower than min on the worker.
func slowerBelow(w *Worker, below []string, min float64) bool {
	for _, sha := range below {
		if sha == SHASmall {
			continue
		}
		if measured(w, sha) {
			if v, _ := whisperSpeed(w, sha); v < min {
				return true
			}
		}
	}
	return false
}

func holds(w *Worker, sha string) bool {
	if w.Caps == nil {
		return false
	}
	for _, m := range w.Caps.Models {
		if m.SHA256 == sha {
			return true
		}
	}
	return false
}

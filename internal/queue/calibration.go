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
// clip, or nil.
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
	r := f / rf
	return &r
}

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

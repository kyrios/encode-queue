package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// **The calibration** (v0.2.5; Thorsten, 2026-10-27: speed from the start, "Calibration clip").
// At pairing, and again when its tools change, every worker encodes the same short clip once per
// rung class, with the rung's real flags, and tells the queue its fps. The queue sets a new
// worker's relative speed from that, against the reference worker's fps on the same clip and
// class, until measured jobs exist.
//
// The clip is made here, by the worker's own ffmpeg, from a fixed filter graph and seed: ten
// seconds of 4K test pattern with grain, so x265 has texture and motion to work on. Workers that
// may run the reference's jobs run the same tools, byte for byte, so they make the same clip; its
// sha256 goes with the results, and the queue compares only calibrations of the same clip.

// Calibration is what the worker reports in its capabilities.
type Calibration struct {
	ClipSHA256 string            `json:"clip_sha256"`
	Tools      []Tool            `json:"tools"`
	At         time.Time         `json:"at"`
	Classes    []CalibratedClass `json:"classes"`
}

// CalibratedClass is one rung class's result.
type CalibratedClass struct {
	Class  string  `json:"class"`
	Frames int     `json:"frames"`
	FPS    float64 `json:"fps"`
}

// calibrationClass is a rung of the ladder (video-pipeline's SETTINGS, ADR-0005) and how many of
// the clip's frames it encodes: fewer for the slow ones. On an M2 Max (12 threads, Homebrew's
// ffmpeg, 2026-10-27) the eight took 44.8 s, the 4K `slower` 12 frames 15 of them; encoder, with
// 6 threads, takes about twice that.
type calibrationClass struct {
	p      RungParams
	frames int
}

const (
	clipSeconds = 10
	clipRate    = 25
	clipGraph   = "testsrc2=size=3840x2160:rate=25:duration=10,noise=alls=4:allf=t:all_seed=528,format=yuv420p"
)

func rung(codec string, w, h int, preset string, crf float64, maxrate int) RungParams {
	p := RungParams{Codec: codec, Width: w, Height: h, Preset: preset, CRF: crf}
	if maxrate > 0 {
		p.VBVMaxrate = &maxrate
	}
	return p
}

var calibrationClasses = []calibrationClass{
	{rung("libx265", 3840, 2160, "ultrafast", 29, 0), 100},
	{rung("libx265", 1920, 1080, "veryfast", 30, 0), 150},
	{rung("libx265", 1280, 720, "veryfast", 30, 0), 250},
	{rung("libx264", 1280, 720, "veryfast", 32, 0), 250},
	{rung("libx265", 3840, 2160, "slower", 29, 5000), 12},
	{rung("libx265", 1920, 1080, "slow", 30, 0), 40},
	{rung("libx265", 1280, 720, "slow", 30, 0), 80},
	{rung("libx264", 1280, 720, "slow", 32, 0), 150},
}

// className is the queue's name for a rung class: "libx265 3840x2160 slower".
func className(p RungParams) string {
	return fmt.Sprintf("%s %dx%d %s", p.Codec, p.Width, p.Height, p.Preset)
}

// encodeTools are the tools whose versions a calibration belongs to.
func encodeTools(all []Tool) []Tool {
	var out []Tool
	for _, t := range all {
		if t.Name == "ffmpeg" || t.Name == "libx265" || t.Name == "libx264" {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sameTools(a, b []Tool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (w *Worker) calibrationFile() string { return filepath.Join(w.cfg.StateDir, "calibration.json") }

// cachedCalibration is the last calibration, if it was made with these tools.
func (w *Worker) cachedCalibration(tools []Tool) *Calibration {
	data, err := os.ReadFile(w.calibrationFile())
	if err != nil {
		return nil
	}
	var c Calibration
	if json.Unmarshal(data, &c) != nil || !sameTools(c.Tools, tools) || len(c.Classes) == 0 {
		return nil
	}
	return &c
}

// calibrate returns the calibration for the worker's current tools: the cached one, or a new run.
// It runs at nice 19 with the worker's thread limit, as a job would.
func (w *Worker) calibrate(ctx context.Context) (*Calibration, error) {
	tools := encodeTools(w.caps.Tools)
	if c := w.cachedCalibration(tools); c != nil {
		return c, nil
	}
	if len(tools) == 0 {
		return nil, fmt.Errorf("no encoder tools to calibrate")
	}
	dir := filepath.Join(w.cfg.WorkDir, "calibration")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	w.emit("calibrating", map[string]any{"classes": len(calibrationClasses)})
	w.cfg.Log.Info("calibration", "classes", len(calibrationClasses))
	clip := filepath.Join(dir, "clip.h264")
	r := &run{w: w, dir: dir}
	// The clip: a raw H.264 stream, bit-exact, so no container writes an ID or a date into it and
	// the same tools give the same bytes (66 MB, 4 s on an M2 Max; two runs, one sha256).
	if err := r.runTool(ctx, []string{w.cfg.FFmpeg, "-hide_banner", "-nostdin", "-y", "-loglevel", "error",
		"-f", "lavfi", "-i", clipGraph, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "22", "-bitexact", "-f", "h264", clip}, "", nil); err != nil {
		return nil, fmt.Errorf("the calibration clip: %w", err)
	}
	sum, err := sha256File(clip)
	if err != nil {
		return nil, err
	}
	c := &Calibration{ClipSHA256: sum, Tools: tools, At: time.Now().UTC()}
	for i, cc := range calibrationClasses {
		args, err := calibrationArgs(w.cfg.FFmpeg, cc, clip, filepath.Join(dir, fmt.Sprintf("out%d.mp4", i)), w.cfg.CPUThreads)
		if err != nil {
			return nil, err
		}
		start := time.Now()
		if err := r.runTool(ctx, args, "", func(map[string]string) {}); err != nil {
			return nil, fmt.Errorf("calibration %s: %w", className(cc.p), err)
		}
		s := time.Since(start).Seconds()
		if s <= 0 {
			continue
		}
		c.Classes = append(c.Classes, CalibratedClass{Class: className(cc.p), Frames: cc.frames, FPS: float64(cc.frames) / s})
		w.cfg.Log.Info("calibrated", "class", className(cc.p), "fps", fmt.Sprintf("%.1f", float64(cc.frames)/s))
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(w.calibrationFile(), data, 0o600); err != nil {
		w.cfg.Log.Warn("calibration not kept", "err", err)
	}
	w.emit("calibrated", map[string]any{"classes": len(c.Classes)})
	return c, nil
}

// calibrationArgs is the rung's own command line (rungArgs) on the clip, for its first frames.
func calibrationArgs(ffmpeg string, cc calibrationClass, clip, out string, threads int) ([]string, error) {
	args, err := rungArgs(ffmpeg, cc.p, clip, 3840, 2160, "", out, threads)
	if err != nil {
		return nil, err
	}
	// -frames:v is an output option: before the output, after everything else.
	return append(args[:len(args)-1:len(args)-1], "-frames:v", fmt.Sprint(cc.frames), out), nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

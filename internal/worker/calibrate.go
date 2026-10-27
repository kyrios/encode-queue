package worker

import (
	"context"
	"crypto/sha256"
	_ "embed"
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
	// Whisper: each model it holds on the speech clip (v0.2.7).
	Whisper []CalibratedModel `json:"whisper,omitempty"`
}

// CalibratedModel is one whisper model's speed on the calibration speech: seconds of audio per
// second, 1 is real time. Probe: only the first seconds ran, because the model was far too
// slow there to be worth the whole clip.
type CalibratedModel struct {
	SHA256  string  `json:"sha256"`
	Name    string  `json:"name"`
	Whisper string  `json:"whisper"` // whisper.cpp's version it was measured with
	GPU     bool    `json:"gpu"`
	Threads int     `json:"threads"`
	AudioS  float64 `json:"audio_s"`
	Speed   float64 `json:"speed"`
	Probe   bool    `json:"probe,omitempty"`
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

// speechClip: 21.6 s of German speech (macOS's Anna reading a short podcast opening), 16 kHz
// mono PCM, ffmpeg -bitexact; the same bytes in every worker binary.
//
//go:embed calibration-speech.wav
var speechClip []byte

const (
	speechSeconds = 21.618
	// A model slower than probeSlow times real time on the probe's first seconds skips the rest.
	probeSeconds = 5
	probeSlow    = 0.25
)

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
	w.saveCalibration(c)
	w.emit("calibrated", map[string]any{"classes": len(c.Classes)})
	return c, nil
}

func (w *Worker) saveCalibration(c *Calibration) {
	data, _ := json.Marshal(c)
	if err := os.WriteFile(w.calibrationFile(), data, 0o600); err != nil {
		w.cfg.Log.Warn("calibration not kept", "err", err)
	}
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

// calibrateWhisper times every model the worker holds on the speech clip that the calibration
// does not have yet for this whisper.cpp and GPU setting, and keeps the others. A large model
// first runs on the clip's first seconds: a slow CPU does not spend minutes on it.
func (w *Worker) calibrateWhisper(ctx context.Context, c *Calibration) error {
	version := ""
	for _, t := range w.caps.Tools {
		if t.Name == "whisper.cpp" {
			version = t.Version
		}
	}
	held := w.heldModels()
	if version == "" || len(held) == 0 {
		return nil
	}
	threads := 4 // a whisper job's default (run.go)
	if t := w.cfg.CPUThreads; t > 0 && threads > t {
		threads = t
	}
	keep := c.Whisper[:0]
	have := map[string]bool{}
	for _, m := range c.Whisper {
		if _, ok := held[m.SHA256]; ok && m.Whisper == version && m.GPU == w.cfg.GPU && m.Threads == threads {
			keep = append(keep, m)
			have[m.SHA256] = true
		}
	}
	c.Whisper = keep
	var todo []string
	for sha := range held {
		if !have[sha] {
			todo = append(todo, sha)
		}
	}
	if len(todo) == 0 {
		return nil
	}
	sort.Strings(todo)
	dir := filepath.Join(w.cfg.WorkDir, "calibration-whisper")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	wav := filepath.Join(dir, "speech.wav")
	if err := os.WriteFile(wav, speechClip, 0o600); err != nil {
		return err
	}
	probe := filepath.Join(dir, "probe.wav")
	r := &run{w: w, dir: dir}
	if err := r.runTool(ctx, []string{w.cfg.FFmpeg, "-hide_banner", "-nostdin", "-y", "-loglevel", "error", "-i", wav,
		"-t", fmt.Sprint(probeSeconds), "-c:a", "pcm_s16le", "-bitexact", probe}, "", nil); err != nil {
		return fmt.Errorf("the speech probe: %w", err)
	}
	for _, sha := range todo {
		path := held[sha]
		name := filepath.Base(path)
		timed := func(in string, audio float64) (float64, error) {
			args, err := whisperArgs(w.cfg.WhisperCLI, path, WhisperParams{Language: "de"}, in, filepath.Join(dir, "out"), threads, w.cfg.GPU)
			if err != nil {
				return 0, err
			}
			start := time.Now()
			if err := r.runTool(ctx, args, "whisper", nil); err != nil {
				return 0, err
			}
			return audio / time.Since(start).Seconds(), nil
		}
		m := CalibratedModel{SHA256: sha, Name: name, Whisper: version, GPU: w.cfg.GPU, Threads: threads}
		w.emitModel(sha, name, "calibrating", nil)
		speed, err := timed(probe, probeSeconds)
		if err != nil {
			w.cfg.Log.Warn("whisper calibration", "model", name, "err", err)
			continue
		}
		m.AudioS, m.Speed, m.Probe = probeSeconds, speed, true
		if speed >= probeSlow {
			if full, err := timed(wav, speechSeconds); err == nil {
				m.AudioS, m.Speed, m.Probe = speechSeconds, full, false
			}
		}
		w.cfg.Log.Info("calibrated", "model", name, "speed", fmt.Sprintf("%.2fx", m.Speed), "probe", m.Probe)
		c.Whisper = append(c.Whisper, m)
		w.emitModel(sha, name, "ready", map[string]any{"speed": m.Speed})
	}
	return nil
}

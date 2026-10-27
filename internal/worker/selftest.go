package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// SelfTest is `encode-worker selftest` (v0.2.8): what video-pipeline's check-variant did on
// the pipeline's machine, now on the worker that encodes. It makes a short clip with this
// worker's ffmpeg, then a rung with the mono measurement, a cut and a remux, each with its
// check, and returns the figures and every problem. Nothing is kept.
func SelfTest(ctx context.Context, cfg Config) (map[string]any, []string) {
	cfg.defaults()
	dir, err := os.MkdirTemp("", "encode-worker-selftest-")
	if err != nil {
		return nil, []string{err.Error()}
	}
	defer os.RemoveAll(dir)
	cfg.WorkDir = dir
	w := &Worker{cfg: cfg}
	l := &Lease{ID: "selftest"}
	l.Job.Source.Width, l.Job.Source.Height = 1280, 720
	r := newRun(w, l)
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		return nil, []string{err.Error()}
	}
	src := filepath.Join(dir, "clip.mp4")
	if out, err := exec.CommandContext(ctx, cfg.FFmpeg, "-hide_banner", "-nostdin", "-v", "error",
		"-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30:duration=3",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3", "-ac", "2",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac",
		"-shortest", "-movflags", "+faststart", src).CombinedOutput(); err != nil {
		return nil, []string{fmt.Sprintf("the clip: %v %s", err, out)}
	}
	res, problems := map[string]any{}, []string{}
	check := &CheckSettings{Compare: &CompareSettings{Every: 5}}
	step := func(name string, args []string, out string, w, h int, c *CheckSettings, same bool) *CheckFigures {
		if err := r.runTool(ctx, args, "ffmpeg", nil); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", name, err))
			return nil
		}
		f, err := r.checkResult(ctx, c, out, src, w, h, same, nil, true)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: the check: %v", name, err))
			return nil
		}
		if f.Decode != nil && f.Decode.Errors > 0 {
			problems = append(problems, fmt.Sprintf("%s: %d decode errors", name, f.Decode.Errors))
		}
		res[name] = f
		return f
	}

	audio := &AudioParams{Mode: "aac", BitrateKbps: 64, Mono: Mono{Measure: true}}
	lv, err := r.measureMono(ctx, src, audio)
	if err != nil || lv.MidDB == nil || !lv.Mono {
		problems = append(problems, fmt.Sprintf("the mono measurement: %+v %v", lv, err))
	}
	res["audio_levels"] = lv
	p := RungParams{Rung: "640", Codec: "libx265", Preset: "ultrafast", CRF: 28, Width: 640, Height: 360, Audio: audio}
	if args, err := rungArgs(cfg.FFmpeg, p, src, 1280, 720, "", filepath.Join(dir, "640.mp4"), cfg.CPUThreads); err != nil {
		problems = append(problems, "rung: "+err.Error())
	} else if f := step("rung", args, filepath.Join(dir, "640.mp4"), 640, 360, check, true); f != nil {
		if f.Compare == nil || f.Compare.PSNRY == nil || f.Compare.SSIM == nil || f.Compare.XPSNR == nil {
			problems = append(problems, fmt.Sprintf("rung: scores missing: %+v", f.Compare))
		}
		if f.Frames == nil || *f.Frames != 90 {
			problems = append(problems, fmt.Sprintf("rung: %v frames, not 90", f.Frames))
		}
	}
	cut := CutParams{StartS: 0.5, LengthS: 2, FadeS: 0.5, Preset: "veryfast", CRF: 14, BitDepth: 8}
	cut.Audio = &struct {
		BitrateKbps int `json:"bitrate_kbps"`
	}{BitrateKbps: 256}
	if args, err := cutArgs(cfg.FFmpeg, cut, src, filepath.Join(dir, "cut.mp4")); err != nil {
		problems = append(problems, "cut: "+err.Error())
	} else if f := step("cut", args, filepath.Join(dir, "cut.mp4"), 1280, 720, &CheckSettings{}, false); f != nil {
		if f.DurationS == nil || *f.DurationS < 1.9 || *f.DurationS > 2.1 {
			problems = append(problems, fmt.Sprintf("cut: lasts %v s, not 2", f.DurationS))
		}
	}
	rem := filepath.Join(dir, "remux.mp4")
	if f := step("remux", remuxArgs(cfg.FFmpeg, RemuxParams{}, src, rem), rem, 1280, 720,
		&CheckSettings{Framemd5: true}, true); f != nil && (f.Framemd5 == nil || !f.Framemd5.Match) {
		problems = append(problems, fmt.Sprintf("remux: the packets differ: %+v", f.Framemd5))
	}
	return res, problems
}

package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestMeasureRemuxAndCheck is how Punkte-Tabelle v1 priced remux and check jobs (v0.2.15;
// Thorsten, 2026-10-28: "a small per-frame price; measure it once on the Mac Studio, as for the
// rungs"). On the calibration clip, as a 3840x2160 HEVC master like the archive's, it times the
// worker's own commands for:
//   - a remux job: remuxArgs and its check as the pipeline asks it (decode, frames, framemd5);
//   - a score check job: a 1080p rung against the master, compared every 5th frame on SSIM,
//     PSNR-Y and VMAF, as video-pipeline's score check asks.
//
// It runs only with EQ_MEASURE_POINTS=1 and the toolchain's ffmpeg first on PATH, and logs the
// frames per second that internal/queue/points.go freezes.
func TestMeasureRemuxAndCheck(t *testing.T) {
	if os.Getenv("EQ_MEASURE_POINTS") != "1" {
		t.Skip("EQ_MEASURE_POINTS=1 measures remux and check for the points table")
	}
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	w := &Worker{cfg: Config{FFmpeg: ff, WorkDir: dir}}
	w.cfg.defaults()
	l := &Lease{ID: "ls_MEASURE"}
	l.Job.Source.Width, l.Job.Source.Height = 3840, 2160
	r := newRun(w, l, false)
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sh := func(args ...string) {
		t.Helper()
		if out, err := exec.Command(ff, append([]string{"-hide_banner", "-v", "error", "-y"}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v %s", args, err, out)
		}
	}
	clip := filepath.Join(dir, "clip.h264")
	sh("-f", "lavfi", "-i", clipGraph, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "22", "-bitexact", "-f", "h264", clip)
	master := filepath.Join(dir, "master.mov")
	sh("-r", "25", "-i", clip, "-f", "lavfi", "-i", "sine=frequency=440:duration=10", "-c:v", "libx265", "-preset", "ultrafast",
		"-crf", "22", "-pix_fmt", "yuv420p10le", "-tag:v", "hvc1", "-x265-params", "log-level=error", "-c:a", "aac", "-shortest", master)
	rung := filepath.Join(dir, "1080.mp4")
	sh("-i", master, "-vf", "scale=1920:1080", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-an", rung)
	const frames = clipSeconds * clipRate
	yes := true

	t0 := time.Now()
	out := filepath.Join(dir, "remux.mp4")
	if err := r.runTool(ctx, remuxArgs(ff, RemuxParams{HVC1Tag: true}, master, out), "ffmpeg", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.checkResult(ctx, &CheckSettings{Decode: &yes, Frames: &yes, Framemd5: true}, out, master, 3840, 2160, true, nil, true); err != nil {
		t.Fatal(err)
	}
	remux := time.Since(t0).Seconds()

	t1 := time.Now()
	no := false
	if _, err := r.checkResult(ctx, &CheckSettings{Decode: &no, Frames: &no, Compare: &CompareSettings{Every: 5, Metrics: []string{"ssim", "psnr_y"},
		VMAF: &struct {
			Model string `json:"model"`
		}{Model: "vmaf_v0.6.1"}}}, rung, master, 1920, 1080, true, nil, false); err != nil {
		t.Fatal(err)
	}
	check := time.Since(t1).Seconds()
	t.Logf("3840x2160 master, %d frames: remux job %.2f s (%.1f fps), score check of a 1080p rung %.2f s (%.1f fps)",
		frames, remux, frames/remux, check, frames/check)
}

package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRealFFmpeg runs the arguments the worker builds through a real ffmpeg, when there is one
// with libx265 and libx264 (the worker image and CI have it), and reads its versions.
func TestRealFFmpeg(t *testing.T) {
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg")
	}
	tools, problems := DetectTools(context.Background(), Config{FFmpeg: ff})
	if len(tools) < 3 {
		t.Skipf("ffmpeg without both encoders: %v", problems)
	}
	t.Logf("tools %v", tools)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.mp4")
	if out, err := exec.Command(ff, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=s=640x360:d=1:r=25",
		"-f", "lavfi", "-i", "sine=d=1", "-c:v", "libx264", "-c:a", "aac", "-shortest", src).CombinedOutput(); err != nil {
		t.Fatalf("the test source: %v %s", err, out)
	}
	for _, p := range []RungParams{
		{Rung: "360", Codec: "libx265", Preset: "ultrafast", CRF: 28, Width: 320, Height: 180},
		{Rung: "h264", Codec: "libx264", Preset: "veryfast", CRF: 23, Width: 640, Height: 360},
	} {
		p.Audio = &struct {
			Mode        string `json:"mode"`
			BitrateKbps int    `json:"bitrate_kbps"`
			Mono        bool   `json:"mono"`
		}{Mode: "aac", BitrateKbps: 64, Mono: true}
		out := filepath.Join(dir, p.Rung+".mp4")
		args, err := rungArgs(ff, p, src, 640, 360, "", out, 2)
		if err != nil {
			t.Fatal(err)
		}
		res, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v %s", p.Rung, err, res)
		}
		if !strings.Contains(string(res), "progress=end") {
			t.Errorf("%s: no -progress output", p.Rung)
		}
		if fi, err := os.Stat(out); err != nil || fi.Size() < 1000 {
			t.Fatalf("%s: no file", p.Rung)
		}
	}
}

package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestCalibrationArgsAreTheRungs(t *testing.T) {
	cc := calibrationClasses[4] // libx265 3840x2160 slower, vbv 5000
	if className(cc.p) != "libx265 3840x2160 slower" || cc.frames != 12 {
		t.Fatalf("class %s, %d frames", className(cc.p), cc.frames)
	}
	got, err := calibrationArgs("ffmpeg", cc, "/w/clip.mkv", "/w/out4.mp4", 8)
	if err != nil {
		t.Fatal(err)
	}
	job, _ := rungArgs("ffmpeg", cc.p, "/w/clip.mkv", 3840, 2160, "", "/w/out4.mp4", 8)
	want := append(slices.Clone(job[:len(job)-1]), "-frames:v", "12", "/w/out4.mp4")
	if !slices.Equal(got, want) {
		t.Fatalf("\n got %v\nwant %v", got, want)
	}
	if !slices.Contains(got, "log-level=error:vbv-maxrate=5000:vbv-bufsize=10000:pools=8") {
		t.Fatalf("not the rung's x265 flags: %v", got)
	}
	// Every class builds, and the 8 are the ladder's.
	seen := map[string]bool{}
	for _, c := range calibrationClasses {
		if _, err := calibrationArgs("ffmpeg", c, "c", "o", 0); err != nil {
			t.Fatalf("%s: %v", className(c.p), err)
		}
		seen[className(c.p)] = true
	}
	if len(seen) != 8 {
		t.Fatalf("%d classes", len(seen))
	}
}

func TestCalibrationCache(t *testing.T) {
	dir := t.TempDir()
	w := &Worker{cfg: Config{StateDir: dir}}
	tools := encodeTools([]Tool{{"whisper.cpp", "v1.9.5"}, {"libx265", "4.1+1"}, {"ffmpeg", "7.1.2"}, {"libx264", "core 164"}})
	if len(tools) != 3 || tools[0].Name != "ffmpeg" {
		t.Fatalf("encode tools %+v", tools)
	}
	c := Calibration{ClipSHA256: "ab", Tools: tools, At: time.Now(), Classes: []CalibratedClass{{"libx265 1920x1080 veryfast", 150, 31.2}}}
	data, _ := json.Marshal(c)
	os.WriteFile(filepath.Join(dir, "calibration.json"), data, 0o600)
	if got := w.cachedCalibration(tools); got == nil || got.Classes[0].FPS != 31.2 {
		t.Fatalf("cache %+v", got)
	}
	// A tools change calibrates again.
	newer := slices.Clone(tools)
	newer[1].Version = "4.2"
	if w.cachedCalibration(newer) != nil {
		t.Fatal("kept a calibration of other tools")
	}
}

// The whole calibration with a real ffmpeg, about a minute: EQ_REAL_CALIBRATION=1 go test -run
// TestRealCalibration ./internal/worker/. Two runs make the same clip.
func TestRealCalibration(t *testing.T) {
	if os.Getenv("EQ_REAL_CALIBRATION") != "1" {
		t.Skip("EQ_REAL_CALIBRATION=1 runs it")
	}
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg")
	}
	tools, _ := DetectTools(context.Background(), Config{FFmpeg: ff})
	dir := t.TempDir()
	w := &Worker{cfg: Config{FFmpeg: ff, StateDir: dir, WorkDir: filepath.Join(dir, "work"), Log: slog.Default()}, caps: Capabilities{Tools: tools}}
	c, err := w.calibrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Classes) != len(calibrationClasses) || len(c.ClipSHA256) != 64 {
		t.Fatalf("%+v", c)
	}
	for _, cc := range c.Classes {
		t.Logf("%-28s %6.1f fps", cc.Class, cc.FPS)
	}
	os.Remove(w.calibrationFile())
	again, err := w.calibrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again.ClipSHA256 != c.ClipSHA256 {
		t.Fatalf("two clips: %s and %s", c.ClipSHA256, again.ClipSHA256)
	}
}

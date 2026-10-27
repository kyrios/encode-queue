package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// realRun is a run with this machine's ffmpeg and no queue: enough for the check step.
func realRun(t *testing.T) (*run, string, string) {
	ff := checkFFmpeg(t)
	dir := t.TempDir()
	w := &Worker{cfg: Config{FFmpeg: ff, WorkDir: dir}}
	w.cfg.defaults()
	l := &Lease{ID: "ls_TEST"}
	l.Job.Source.Width, l.Job.Source.Height = 640, 360
	r := newRun(w, l)
	if err := os.MkdirAll(r.dir, 0o700); err != nil { // execute makes it
		t.Fatal(err)
	}
	src := filepath.Join(dir, "src.mp4")
	if out, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=25:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2", "-ac", "2", "-c:v", "libx264", "-preset", "ultrafast",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", "-movflags", "+faststart", src).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	return r, ff, src
}

func TestCheckResultOfARung(t *testing.T) {
	r, ff, src := realRun(t)
	out := filepath.Join(r.w.cfg.WorkDir, "320.mp4")
	p := RungParams{Rung: "320", Codec: "libx264", Preset: "ultrafast", CRF: 23, Width: 320, Height: 180,
		Audio: &AudioParams{Mode: "aac", BitrateKbps: 64, Mono: Mono{Measure: true}}}
	lv, err := r.measureMono(context.Background(), src, p.Audio)
	if err != nil || !lv.Mono || !p.Audio.Mono.On || lv.MidDB == nil {
		t.Fatalf("the same tone on both channels: %+v %v", lv, err)
	}
	args, err := rungArgs(ff, p, src, 640, 360, "", out, 0)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	frames := int64(50)
	f, err := r.checkResult(context.Background(), &CheckSettings{Compare: &CompareSettings{Every: 5}},
		out, src, 320, 180, true, &frames, true)
	if err != nil {
		t.Fatal(err)
	}
	if f.Decode == nil || f.Decode.Errors != 0 || *f.Frames != 50 || *f.SourceFrames != 50 {
		t.Errorf("decode %+v frames %v", f.Decode, f.Frames)
	}
	if f.DurationS == nil || *f.DurationS < 1.9 || *f.DurationS > 2.1 {
		t.Errorf("duration %v", f.DurationS)
	}
	c := f.Compare
	if c == nil || c.Pairs != 10 || c.Every != 5 {
		t.Fatalf("compare %+v", c)
	}
	for name, v := range map[string]any{"ssim": c.SSIM, "psnr_y": c.PSNRY, "xpsnr": c.XPSNR} {
		if x, ok := v.(float64); !ok || x <= 0 {
			t.Errorf("%s %v", name, v)
		}
	}
	if b, _ := json.Marshal(f); !strings.Contains(string(b), `"check_s"`) {
		t.Errorf("figures %s", b)
	}
}

func TestCheckFindsABrokenFile(t *testing.T) {
	r, _, src := realRun(t)
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	broken := filepath.Join(r.w.cfg.WorkDir, "broken.mp4")
	// The index first (faststart), then the media data cut short.
	if err := os.WriteFile(broken, data[:len(data)*2/3], 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := r.checkResult(context.Background(), &CheckSettings{}, broken, "", 640, 360, true, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if f.Decode.Errors == 0 || len(f.Decode.Lines) == 0 {
		t.Errorf("a cut-short file decodes clean: %+v", f.Decode)
	}
}

// A `check` job streams: ffmpeg reads both files through the lease's proxy on 127.0.0.1,
// with range requests, and the proxy reads them from the queue.
func TestCheckStreamsThroughTheProxy(t *testing.T) {
	r, ff, src := realRun(t)
	rem := filepath.Join(r.w.cfg.WorkDir, "remux.mp4")
	ra := remuxArgs(ff, RemuxParams{}, src, rem)
	if b, err := exec.Command(ra[0], ra[1:]...).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	ranges := 0
	queue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Range") != "" {
			ranges++
		}
		switch q.URL.Path {
		case "/v1/jobs/j/source":
			http.ServeFile(w, q, src)
		case "/v1/jobs/j/inputs/candidate":
			http.ServeFile(w, q, rem)
		default:
			http.NotFound(w, q)
		}
	}))
	defer queue.Close()
	api, err := NewClient(queue.Client(), queue.URL+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	px, err := api.serveLease(map[string]FileRef{"source": {URL: "/jobs/j/source"},
		"candidate": {URL: "/jobs/j/inputs/candidate"}})
	if err != nil {
		t.Fatal(err)
	}
	defer px.Close()
	f, err := r.checkResult(context.Background(), &CheckSettings{Framemd5: true,
		Compare: &CompareSettings{Every: 10, Metrics: []string{"psnr_y"}}},
		px.URL("candidate"), px.URL("source"), 640, 360, true, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if f.Framemd5 == nil || !f.Framemd5.Match || f.Compare.PSNRY != "inf" || *f.SourceFrames != 50 {
		t.Errorf("framemd5 %+v compare %+v source frames %v", f.Framemd5, f.Compare, f.SourceFrames)
	}
	if ranges == 0 {
		t.Error("ffmpeg asked for no range")
	}
	resp, err := http.Get(strings.Replace(px.URL("source"), px.token, "wrong", 1))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("a wrong token: %d", resp.StatusCode)
	}
}

func TestSweepKeepsHeldLeases(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"ls_HELD", "ls_LEFT", "calibration"} {
		if err := os.MkdirAll(filepath.Join(dir, d, "out"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	w := &Worker{cfg: Config{WorkDir: dir}, runs: map[string]*run{"ls_HELD": {}}}
	w.cfg.defaults()
	w.sweepWork()
	for d, want := range map[string]bool{"ls_HELD": true, "ls_LEFT": false, "calibration": true} {
		if _, err := os.Stat(filepath.Join(dir, d)); (err == nil) != want {
			t.Errorf("%s there: %v, want %v", d, err == nil, want)
		}
	}
}

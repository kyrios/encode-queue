package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/queue"
	"github.com/kyrios/encode-queue/internal/worker"
)

// **The whole path that decides what gets published** (v0.2.8), with a real ffmpeg (the
// toolchain's, in its image) instead of the fake tool, which runs no check: a rung lease and a
// cut lease, each encoding and then checking its file in the same lease, and a check job that
// streams its two files and delivers figures alone. Each result waits in `checking` with its
// figures until the pipeline's verdict finishes it.

// realFFmpeg is this machine's ffmpeg when it has what the jobs run, else the test is skipped.
func realFFmpeg(t *testing.T) string {
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg")
	}
	filters, _ := exec.Command(ff, "-hide_banner", "-filters").Output()
	encoders, _ := exec.Command(ff, "-hide_banner", "-encoders").Output()
	for _, f := range []string{"xpsnr", "ssim", "psnr", "astats", "fade"} {
		if !regexp.MustCompile(`\s` + f + `\s`).Match(filters) {
			t.Skipf("ffmpeg without the %s filter (the toolchain has it)", f)
		}
	}
	if !regexp.MustCompile(`\slibx264\s`).Match(encoders) {
		t.Skip("ffmpeg without libx264")
	}
	return ff
}

// realSource makes three seconds of 640x360 at 30 fps with the same tone on both channels in
// the queue's source directory.
func (h *harness) realSource(ff, rel string) queue.FileIn {
	h.t.Helper()
	p := filepath.Join(h.srcDir, rel)
	os.MkdirAll(filepath.Dir(p), 0o755)
	if out, err := exec.Command(ff, "-hide_banner", "-v", "error", "-y", "-f", "lavfi",
		"-i", "testsrc2=size=640x360:rate=30:duration=3", "-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-ac", "2", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac",
		"-shortest", "-movflags", "+faststart", p).CombinedOutput(); err != nil {
		h.t.Fatalf("the source: %v %s", err, out)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		h.t.Fatal(err)
	}
	s := sha256.Sum256(data)
	return queue.FileIn{Path: rel, Bytes: int64(len(data)), SHA256: hex.EncodeToString(s[:])}
}

func (h *harness) submit(id string, body map[string]any) {
	h.t.Helper()
	if st := h.call("PUT", "/v1/jobs/"+id, pipelineToken, body, nil); st != 201 {
		h.t.Fatalf("submit %s: %d", id, st)
	}
}

type figures struct {
	Decode *struct {
		Errors int `json:"errors"`
	} `json:"decode"`
	Frames       *int64   `json:"frames"`
	SourceFrames *int64   `json:"source_frames"`
	DurationS    *float64 `json:"duration_s"`
	Compare      *struct {
		Every int   `json:"every"`
		Pairs int64 `json:"pairs"`
		SSIM  any   `json:"ssim"`
		PSNRY any   `json:"psnr_y"`
		XPSNR any   `json:"xpsnr"`
	} `json:"compare"`
	AudioLevels *struct {
		Mono bool `json:"mono"`
	} `json:"audio_levels"`
}

func TestRealChecksThroughTheQueue(t *testing.T) {
	ff := realFFmpeg(t)
	h := newHarness(t)
	// The pipeline: what it sees when it judges, then its verdict.
	var mu sync.Mutex
	judged := map[string]queue.AttemptResult{}
	states := map[string]string{}
	h.verdict = func(j queue.JobView, a queue.AttemptView, file []byte) (string, string) {
		mu.Lock()
		defer mu.Unlock()
		judged[j.ID], states[j.ID] = *a.Result, j.State
		return "accepted", ""
	}
	h.pipeline()
	h.pairWith("encoder", func(c *worker.Config) {
		c.Version = "0.2.8"
		c.ChunkBytes = 4 << 20
		c.FFmpeg = ff
		c.WhisperCLI = "no-whisper-cli"
	})

	src := h.realSource(ff, "real/master.mp4")
	source := map[string]any{"path": src.Path, "bytes": src.Bytes, "sha256": src.SHA256, "container": "mp4",
		"duration_s": 3.0, "frames": 90, "width": 640, "height": 360}
	h.submit("real.320-s1-test", map[string]any{"kind": "rung", "priority": "stage1", "upload_id": "real", "stage": 1,
		"params": map[string]any{"rung": "320", "codec": "libx264", "preset": "ultrafast", "crf": 23,
			"width": 320, "height": 180,
			"audio": map[string]any{"mode": "aac", "bitrate_kbps": 64, "mono": "measure", "mono_side_below_db": 40},
			"check": map[string]any{"decode": true, "frames": true, "compare": map[string]any{"every": 5}}},
		"source": source, "result": map[string]any{"name": "320.mp4", "max_bytes": 50 << 20}})
	h.submit("real.cut-s1-test", map[string]any{"kind": "cut", "priority": "stage1", "upload_id": "real", "stage": 1,
		"params": map[string]any{"start_s": 0.5, "length_s": 2, "fade_s": 0.5, "preset": "veryfast", "crf": 14,
			"bit_depth": 8, "audio": map[string]any{"bitrate_kbps": 256}, "check": map[string]any{"decode": true}},
		"source": source, "result": map[string]any{"name": "cut.mp4", "max_bytes": 50 << 20}})
	cand := h.realSource(ff, "real/candidate.mp4")
	h.submit("real.check-test", map[string]any{"kind": "check", "priority": "archive", "upload_id": "real",
		"params": map[string]any{"candidate": "candidate", "width": 640, "height": 360,
			"check": map[string]any{"decode": true, "frames": true,
				"compare": map[string]any{"every": 5, "metrics": []string{"psnr_y"}}}},
		"source": source, "inputs": map[string]any{"candidate": cand},
		"result": map[string]any{"name": "figures.json", "max_bytes": 1 << 20}})

	rung := h.waitState("real.320-s1-test", "done", 240*time.Second)
	h.waitState("real.cut-s1-test", "done", 240*time.Second)
	check := h.waitState("real.check-test", "done", 240*time.Second)

	mu.Lock()
	defer mu.Unlock()
	read := func(id string) figures {
		t.Helper()
		if states[id] != "checking" {
			t.Errorf("%s: the pipeline judged it in %q, not checking", id, states[id])
		}
		var f figures
		if err := json.Unmarshal(judged[id].Check, &f); err != nil || f.Decode == nil {
			t.Fatalf("%s: figures %s: %v", id, judged[id].Check, err)
		}
		return f
	}
	// The rung: measured, encoded, decoded and compared in its lease.
	f := read("real.320-s1-test")
	if f.Decode.Errors != 0 || f.Frames == nil || *f.Frames != 90 || f.SourceFrames == nil || *f.SourceFrames != 90 {
		t.Errorf("the rung's decode and frames: %+v", f)
	}
	if c := f.Compare; c == nil || c.Every != 5 || c.Pairs != 18 || c.SSIM == nil || c.XPSNR == nil {
		t.Errorf("the rung's comparison: %+v", c)
	} else if p, ok := c.PSNRY.(float64); !ok || p < 30 {
		t.Errorf("the rung's PSNR-Y %v", c.PSNRY)
	}
	if f.AudioLevels == nil || !f.AudioLevels.Mono {
		t.Errorf("the rung's mono: %+v", f.AudioLevels)
	}
	if r := judged["real.320-s1-test"]; r.Outcome != "file" || r.FileURL == nil || r.Frames != 90 {
		t.Errorf("the rung's result: %+v", r)
	}
	if got := h.fetch(rung, 1); !strings.Contains(got[:64], "ftyp") {
		t.Errorf("the rung's file: %.40q", got)
	}
	// The cut: its own range, decoded; the source has more frames and is not counted.
	f = read("real.cut-s1-test")
	if f.Decode.Errors != 0 || f.Frames == nil || *f.Frames != 60 || f.SourceFrames != nil || f.Compare != nil {
		t.Errorf("the cut's figures: %+v", f)
	}
	if f.DurationS == nil || *f.DurationS < 1.9 || *f.DurationS > 2.1 {
		t.Errorf("the cut lasts %v s", f.DurationS)
	}
	// The check job: figures alone, of two files it streamed.
	f = read("real.check-test")
	if r := judged["real.check-test"]; r.Outcome != "figures" || r.FileURL != nil {
		t.Errorf("the check job's result: %+v", r)
	}
	if f.Decode.Errors != 0 || f.Frames == nil || *f.Frames != 90 || f.SourceFrames == nil || *f.SourceFrames != 90 ||
		f.Compare == nil || f.Compare.PSNRY != "inf" || f.Compare.Pairs != 18 {
		t.Errorf("the check job's figures: %+v %+v", f, f.Compare)
	}
	if len(check.Attempts) != 1 || check.Attempts[0].Result.Verdict != "accepted" {
		t.Errorf("the check job's verdict: %+v", check.Attempts)
	}
	// The phase the worker reported while it checked.
	h.evMu.Lock()
	phases := map[string]bool{}
	for _, ev := range h.events {
		if ev["event"] == "job" {
			phases[fmt.Sprint(ev["phase"])] = true
		}
	}
	h.evMu.Unlock()
	if !phases["checking"] || !phases["encoding"] {
		t.Errorf("the phases the worker reported: %v", phases)
	}
}

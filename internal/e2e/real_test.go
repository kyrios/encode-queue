package e2e

import (
	"crypto/sha256"
	"encoding/binary"
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

// realFFmpeg is this machine's ffmpeg when it has what the jobs run, else the test is skipped,
// or fails where CI runs it in the toolchain's image (EQ_REQUIRE_REAL_FFMPEG=1): there a skip
// would hide that nothing tested the path that decides what gets published.
func realFFmpeg(t *testing.T) string {
	skip := t.Skipf
	if os.Getenv("EQ_REQUIRE_REAL_FFMPEG") == "1" {
		skip = t.Fatalf
	}
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		skip("no ffmpeg on PATH")
	}
	filters, _ := exec.Command(ff, "-hide_banner", "-filters").Output()
	encoders, _ := exec.Command(ff, "-hide_banner", "-encoders").Output()
	for _, f := range []string{"xpsnr", "ssim", "psnr", "astats", "fade"} {
		if !regexp.MustCompile(`\s` + f + `\s`).Match(filters) {
			skip("ffmpeg without the %s filter (the toolchain has it)", f)
		}
	}
	if !regexp.MustCompile(`\slibx264\s`).Match(encoders) {
		skip("ffmpeg without libx264")
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

// **A clip's rung with the real ffmpeg** (v0.2.13): a 120 fps source with 96 kHz sound on four
// channels comes out at 60 fps, 48 kHz and two channels, and with edit_list false without an
// edts box; the same rung without it has one. At its own rate the file reports its own frames
// and no source_frames, which would be the source's.
func TestRealClipRung(t *testing.T) {
	ff := realFFmpeg(t)
	h := newHarness(t)
	var mu sync.Mutex
	judged := map[string]queue.AttemptResult{}
	h.verdict = func(j queue.JobView, a queue.AttemptView, file []byte) (string, string) {
		mu.Lock()
		defer mu.Unlock()
		judged[j.ID] = *a.Result
		return "accepted", ""
	}
	h.pipeline()
	h.pairWith("encoder", func(c *worker.Config) {
		c.Version = "0.2.13"
		c.ChunkBytes = 4 << 20
		c.FFmpeg = ff
		c.WhisperCLI = "no-whisper-cli"
	})
	rel := "clip/master.mov"
	p := filepath.Join(h.srcDir, rel)
	os.MkdirAll(filepath.Dir(p), 0o755)
	if out, err := exec.Command(ff, "-hide_banner", "-v", "error", "-y", "-f", "lavfi",
		"-i", "testsrc2=size=640x360:rate=120:duration=3", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=96000:duration=3",
		"-filter_complex", "[1:a]pan=4c|c0=c0|c1=c0|c2=c0|c3=c0[a]", "-map", "0:v", "-map", "[a]",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "pcm_s16le", p).CombinedOutput(); err != nil {
		t.Fatalf("the source: %v %s", err, out)
	}
	data, _ := os.ReadFile(p)
	sum := sha256.Sum256(data)
	source := map[string]any{"path": rel, "bytes": len(data), "sha256": hex.EncodeToString(sum[:]), "container": "mov",
		"duration_s": 3.0, "frames": 360, "width": 640, "height": 360}
	params := func(editList any) map[string]any {
		// veryfast, stage 1's preset, has B-frames as every clip's: the picture starts two frames late,
		// later than the AAC priming. (Without B-frames and without edit lists the muxer stretches
		// the first picture by the priming's 21.3 ms instead, and ffmpeg reads 59.58 fps here.)
		return map[string]any{"rung": "mastodon", "codec": "libx264", "preset": "veryfast", "crf": 23, "width": 202, "height": 360,
			"crop": map[string]any{"width": 202, "height": 360, "x": 218, "y": 0}, "vbv_maxrate_kbps": 2000,
			"audio": map[string]any{"mode": "aac", "bitrate_kbps": 128, "sample_rate": 48000, "channels": 2},
			"check": map[string]any{"decode": true, "frames": true}, "fps": 60, "edit_list": editList}
	}
	for id, el := range map[string]any{"clip.mastodon-s2": false, "clip.with-elst": nil} {
		h.submit(id, map[string]any{"kind": "rung", "priority": "now", "upload_id": "clip", "params": params(el),
			"source": source, "result": map[string]any{"name": "mastodon.mp4", "max_bytes": 50 << 20}})
	}
	// The comparison at another rate: the whole picture scaled, the source taken at 60 fps too,
	// so each pair is one moment. testsrc2 moves, and pairing the result's frame n with the 120 fps
	// source's frame n would compare pictures 1/120 s × n apart.
	compared := map[string]any{"rung": "180", "codec": "libx264", "preset": "veryfast", "crf": 18, "width": 320, "height": 180,
		"audio": map[string]any{"mode": "none"}, "fps": 60,
		"check": map[string]any{"decode": true, "frames": true, "compare": map[string]any{"every": 5, "metrics": []string{"psnr_y"}}}}
	h.submit("clip.180-compare", map[string]any{"kind": "rung", "priority": "now", "upload_id": "clip", "params": compared,
		"source": source, "result": map[string]any{"name": "180.mp4", "max_bytes": 50 << 20}})
	for id, wantEdts := range map[string]bool{"clip.mastodon-s2": false, "clip.with-elst": true} {
		j := h.waitState(id, "done", 240*time.Second)
		file := filepath.Join(t.TempDir(), "mastodon.mp4")
		os.WriteFile(file, []byte(h.fetch(j, 1)), 0o644)
		info, _ := exec.Command(ff, "-hide_banner", "-i", file).CombinedOutput()
		for _, w := range []string{"202x360", "60 fps", "48000 Hz, stereo"} {
			if !strings.Contains(string(info), w) {
				t.Errorf("%s: no %q in\n%s", id, w, info)
			}
		}
		if got := edtsBoxes(t, file); got != wantEdts {
			t.Errorf("%s: an edts box %v, want %v", id, got, wantEdts)
		}
		mu.Lock()
		var f figures
		if err := json.Unmarshal(judged[id].Check, &f); err != nil || f.Decode == nil || f.Decode.Errors != 0 ||
			f.Frames == nil || *f.Frames != 180 || f.SourceFrames != nil {
			t.Errorf("%s: figures %s", id, judged[id].Check)
		}
		mu.Unlock()
	}
	checkClipCompare(t, h, &mu, judged)
}

// TestRealClipRung's comparison, after its two files.
func checkClipCompare(t *testing.T, h *harness, mu *sync.Mutex, judged map[string]queue.AttemptResult) {
	t.Helper()
	h.waitState("clip.180-compare", "done", 240*time.Second)
	mu.Lock()
	defer mu.Unlock()
	var f figures
	if err := json.Unmarshal(judged["clip.180-compare"].Check, &f); err != nil || f.Compare == nil || f.Compare.Pairs != 36 {
		t.Fatalf("the comparison at 60 fps: %s", judged["clip.180-compare"].Check)
	}
	if p, ok := f.Compare.PSNRY.(float64); !ok || p < 30 {
		t.Errorf("PSNR-Y %v at 60 fps: the pairs are not the same moments", f.Compare.PSNRY)
	}
}

// edtsBoxes says whether any trak of an MP4's moov has an edts box (an edit list).
func edtsBoxes(t *testing.T, path string) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	children := func(b []byte) map[string][][]byte {
		out := map[string][][]byte{}
		for len(b) >= 8 {
			size := int(binary.BigEndian.Uint32(b))
			if size < 8 || size > len(b) {
				break
			}
			out[string(b[4:8])] = append(out[string(b[4:8])], b[8:size])
			b = b[size:]
		}
		return out
	}
	moov := children(data)["moov"]
	if len(moov) != 1 {
		t.Fatalf("%s: %d moov boxes", path, len(moov))
	}
	traks := children(moov[0])["trak"]
	if len(traks) != 2 {
		t.Fatalf("%s: %d traks", path, len(traks))
	}
	for _, trak := range traks {
		if len(children(trak)["edts"]) > 0 {
			return true
		}
	}
	return false
}

// **Captions in any work dir** (v0.2.14; Alex's Vimes, 2026-10-28: "the captions' path …/Library/
// Application Support/Slipstream/worker/work/…/input-captions cannot go into a filter"). A Mac's
// work dir sits under "Application Support". The worker gives libass the captions by their name
// in the lease's folder, where ffmpeg runs, so a work dir with a space, a quote and a colon burns
// them in like any other: white words on a black clip come out white.
func TestRealCaptionsInAnyWorkDir(t *testing.T) {
	ff := realFFmpeg(t)
	if filters, _ := exec.Command(ff, "-hide_banner", "-filters").Output(); !regexp.MustCompile(`\sass\s`).Match(filters) {
		if os.Getenv("EQ_REQUIRE_REAL_FFMPEG") == "1" {
			t.Fatal("ffmpeg without libass (the toolchain has it)")
		}
		t.Skip("ffmpeg without libass: run with the toolchain's ffmpeg first on PATH")
	}
	h := newHarness(t)
	h.pipeline()
	work := filepath.Join(t.TempDir(), "Application Support", `Alex's: work "x"`)
	h.pairWith("vimes", func(c *worker.Config) {
		c.Version = "0.2.14"
		c.ChunkBytes = 4 << 20
		c.FFmpeg = ff
		c.WhisperCLI = "no-whisper-cli"
		c.WorkDir = work
		c.Fonts = []string{"Inter ExtraBold"}
	})
	rel := "caps/master.mov"
	p := filepath.Join(h.srcDir, rel)
	os.MkdirAll(filepath.Dir(p), 0o755)
	if out, err := exec.Command(ff, "-hide_banner", "-v", "error", "-y", "-f", "lavfi", "-i", "color=c=black:size=320x180:rate=25:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", p).CombinedOutput(); err != nil {
		t.Fatalf("the source: %v %s", err, out)
	}
	data, _ := os.ReadFile(p)
	sum := sha256.Sum256(data)
	ass := "[Script Info]\nScriptType: v4.00+\nPlayResX: 320\nPlayResY: 180\n\n[V4+ Styles]\n" +
		"Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n" +
		"Style: Default,Inter ExtraBold,72,&H00FFFFFF,&H00FFFFFF,&H00FFFFFF,&H00000000,-1,0,0,0,100,100,0,0,1,0,0,5,10,10,10,1\n\n" +
		"[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n" +
		"Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,HALLO\n"
	caps := h.source("caps/captions.ass", ass)
	h.submit("caps.mastodon", map[string]any{"kind": "rung", "priority": "now", "upload_id": "caps",
		"params": map[string]any{"rung": "mastodon", "codec": "libx264", "preset": "veryfast", "crf": 18, "width": 320, "height": 180,
			"audio": map[string]any{"mode": "none"}, "captions": map[string]any{"input": "captions", "font": "Inter ExtraBold"},
			"check": map[string]any{"decode": true, "frames": true}},
		"source": map[string]any{"path": rel, "bytes": len(data), "sha256": hex.EncodeToString(sum[:]), "container": "mov",
			"duration_s": 2.0, "frames": 50, "width": 320, "height": 180},
		"inputs": map[string]any{"captions": map[string]any{"path": caps.Path, "bytes": caps.Bytes, "sha256": caps.SHA256}},
		"result": map[string]any{"name": "mastodon.mp4", "max_bytes": 50 << 20}})
	j := h.waitState("caps.mastodon", "done", 240*time.Second)
	file := filepath.Join(t.TempDir(), "mastodon.mp4")
	os.WriteFile(file, []byte(h.fetch(j, 1)), 0o644)
	frame, err := exec.Command(ff, "-hide_banner", "-v", "error", "-ss", "1", "-i", file, "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gray", "-").Output()
	if err != nil || len(frame) != 320*180 {
		t.Fatalf("a frame of the result: %v, %d bytes", err, len(frame))
	}
	white := 0
	for _, v := range frame {
		if v > 200 {
			white++
		}
	}
	if white < 500 {
		t.Fatalf("%d white pixels: the captions are not burnt in", white)
	}
}

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// The test binary stands in for ffmpeg and whisper-cli: the worker runs it with EQ_FAKE_TOOL=1
// in its environment, and TestMain sends it here.
//
// ffmpeg -version, the one-frame encodes that name x265's and x264's versions, the calibration
// clip (a fixed file), an encode that
// copies its source with the arguments appended and reports FAKE_FRAMES frames of progress
// FAKE_FRAME_MS apart, and FAKE_FAIL=1 for a failing encode. whisper-cli writes a JSON.
func fakeTool(args []string) int {
	argv := strings.Join(args, " ")
	// FAKE_STDERR: every call writes a line to stderr first, as a real ffmpeg or whisper-cli
	// warning does (v0.2.12: a calibration's stderr line crashed the worker in Slipstream).
	if os.Getenv("FAKE_STDERR") == "1" {
		fmt.Fprintln(os.Stderr, "[warning] fake stderr line")
	}
	switch {
	case len(args) >= 3 && args[0] == "-I" && strings.HasSuffix(args[1], "align.py"):
		return fakeAligner(args[2:])
	case strings.HasSuffix(argv, "-filters"): // the filter list: no ass in this ffmpeg
		fmt.Println(" ... null              V->V       Pass the source unchanged to the output.")
		return 0
	case len(args) == 1 && args[0] == "-version":
		fmt.Println("ffmpeg version 7.1.2-fake Copyright (c) 2000-2025 the FFmpeg developers")
		return 0
	case strings.Contains(argv, "testsrc2"): // the calibration clip (worker/calibrate.go)
		os.WriteFile(args[len(args)-1], []byte("a calibration clip"), 0o644)
		return 0
	case strings.Contains(argv, "-f lavfi") && strings.Contains(argv, "libx265"):
		fmt.Print("\x00\x00\x01x265 (build 215) - 4.1+1-fake:[Linux][GCC 14.2.0][64 bit] 10bit - H.265/HEVC codec")
		return 0
	case strings.Contains(argv, "-f lavfi") && strings.Contains(argv, "libx264"):
		fmt.Print("\x00\x00\x01x264 - core 164 r3108 31e19f9 - H.264/MPEG-4 AVC codec")
		return 0
	case strings.Contains(argv, "-oj"): // whisper-cli
		var base string
		for i, a := range args {
			if a == "-of" && i+1 < len(args) {
				base = args[i+1]
			}
		}
		for p := 0; p <= 100; p += 50 {
			fmt.Fprintf(os.Stderr, "whisper_print_progress_callback: progress = %d%%\n", p)
			time.Sleep(20 * time.Millisecond)
		}
		os.WriteFile(base+".json", []byte(`{"transcription":[{"text":" Hallo","offsets":{"from":0,"to":500}}]}`), 0o644)
		return 0
	}
	// An encode: -i SRC ... OUT.
	var src string
	for i, a := range args {
		if a == "-i" && i+1 < len(args) {
			src = args[i+1]
		}
	}
	out := args[len(args)-1]
	// A calibration class's encode (-frames:v, worker/calibrate.go) takes FAKE_CALIBRATE_MS.
	if strings.Contains(argv, "-frames:v") {
		if ms, _ := strconv.Atoi(os.Getenv("FAKE_CALIBRATE_MS")); ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
	if os.Getenv("FAKE_FAIL") == "1" {
		fmt.Fprintln(os.Stderr, "x265 [error]: fake failure")
		return 1
	}
	frames, _ := strconv.Atoi(os.Getenv("FAKE_FRAMES"))
	if frames == 0 {
		frames = 10
	}
	ms, _ := strconv.Atoi(os.Getenv("FAKE_FRAME_MS"))
	if ms == 0 {
		ms = 10
	}
	progress := strings.Contains(argv, "-progress pipe:1")
	for f := 1; f <= frames; f++ {
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if progress {
			fmt.Printf("frame=%d\nfps=%d\nout_time_us=%d\nspeed=%.2fx\nprogress=continue\n", f, 1000/ms, f*40000, 0.5)
		}
	}
	data, err := os.ReadFile(src)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	data = append(data, []byte("\nencoded with: "+argv)...)
	if err := os.WriteFile(out, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if progress {
		fmt.Printf("frame=%d\nprogress=end\n", frames)
	}
	return 0
}

// fakeAligner stands in for the aligner's Python running align.py (v0.2.16): `version` names
// FAKE_ALIGNER's engine (qwen-asr without it); `run` gives each word of the words file half a
// second in whisper-cli's shape, with its arguments, and FAKE_ALIGN_REFUSE=1 refuses with 3.
func fakeAligner(args []string) int {
	engine := os.Getenv("FAKE_ALIGNER")
	if engine == "" {
		engine = "qwen-asr"
	}
	if args[0] == "version" {
		fmt.Printf(`{"name": %q, "version": "0.0.6", "backend": "torch fake"}`+"\n", engine)
		return 0
	}
	opt := map[string]string{}
	for i := 1; i+1 < len(args); i += 2 {
		opt[strings.TrimPrefix(args[i], "--")] = args[i+1]
	}
	if os.Getenv("FAKE_ALIGN_REFUSE") == "1" {
		fmt.Fprintln(os.Stderr, "refused: word 0 is not one token of 1 to 200 characters")
		return 3
	}
	if _, err := os.Stat(opt["wav"]); err != nil {
		fmt.Fprintln(os.Stderr, "no sound:", err)
		return 1
	}
	var in struct{ Words []struct{ Text string } }
	data, _ := os.ReadFile(opt["words"])
	if err := json.Unmarshal(data, &in); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var segs []map[string]any
	for i, w := range in.Words {
		segs = append(segs, map[string]any{"offsets": map[string]int{"from": 500 * i, "to": 500*i + 400}, "text": " " + w.Text})
	}
	for p := 0; p <= 100; p += 50 {
		fmt.Fprintf(os.Stderr, "progress = %d%%\n", p)
	}
	out, _ := json.Marshal(map[string]any{"transcription": segs, "argv": opt})
	os.WriteFile(opt["out"], out, 0o644)
	return 0
}

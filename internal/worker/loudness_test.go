package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoudnessGolden: ffmpeg's ebur128 log of 4 s of a stereo 1 kHz tone at -23 dBFS, 2 s of
// silence and 4 s at -13 dBFS (testdata/loudness-tone.log, FFREPORT's file, prefixes glued into
// its summary as FFREPORT writes them) gives testdata/loudness-tone.json. Its short-term values
// are the 3 s windows' energy means: at 5 s two thirds of -23 (-24.8), at 7 s one third of -13
// (-17.8). video-pipeline's worker.py holds its twin to the same two files.
func TestLoudnessGolden(t *testing.T) {
	f, err := os.Open("testdata/loudness-tone.log")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := json.Marshal(LoudnessFromLog(f))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/loudness-tone.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(got, '\n'), want) {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestLoudnessOfSilence(t *testing.T) {
	log := "[Parsed_ebur128_0 @ 0x1] t: 0.999979   TARGET:-23 LUFS    M:-120.7 S:-120.7     I: -70.0 LUFS       LRA:   0.0 LU  FTPK: -inf -inf dBFS  TPK: -inf -inf dBFS\n" +
		"[Parsed_ebur128_0 @ 0x1] Summary:\n\n  Integrated loudness:\n    I:         -70.0 LUFS\n    Threshold:   0.0 LUFS\n\n" +
		"  Loudness range:\n    LRA:         0.0 LU\n\n  True peak:\n    Peak:       -inf dBFS\n"
	l := LoudnessFromLog(strings.NewReader(log))
	b, _ := json.Marshal(l)
	if string(b) != `{"schema":1,"integrated_lufs":null,"lra_lu":0,"true_peak_dbtp":null,"every_s":1,"short_term":[null]}` {
		t.Errorf("silence: %s", b)
	}
}

// TestRealLoudness: EBU Tech 3341's first case through this machine's ffmpeg, a stereo 1 kHz
// tone at -23 dBFS, reads -23.0 LUFS; a file without sound fails as decode_failed.
func TestRealLoudness(t *testing.T) {
	r, ff, _ := realRun(t)
	src := filepath.Join(r.w.cfg.WorkDir, "tone.wav")
	if b, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi",
		"-i", "aevalsrc=0.0707946*sin(2*PI*1000*t)|0.0707946*sin(2*PI*1000*t):s=48000:d=6", src).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	out := filepath.Join(r.w.cfg.WorkDir, "loudness.json")
	if _, err := r.workLoudness(context.Background(), src, out); err != nil {
		t.Fatal(err)
	}
	var l Loudness
	b, _ := os.ReadFile(out)
	if err := json.Unmarshal(b, &l); err != nil {
		t.Fatal(err)
	}
	if l.LUFS == nil || math.Abs(*l.LUFS+23) > 0.1 || l.TruePeak == nil || math.Abs(*l.TruePeak+23) > 0.6 || l.LRA == nil {
		t.Fatalf("EBU case 1: %s", b)
	}
	if len(l.ShortTerm) != 6 || l.ShortTerm[0] != nil || l.ShortTerm[5] == nil || math.Abs(*l.ShortTerm[5]+23) > 0.1 {
		t.Errorf("short-term: %s", b)
	}
	for _, line := range r.tail {
		if strings.Contains(line, "TARGET:") {
			t.Fatalf("ebur128's lines reached the lease's log: %s", line)
		}
	}

	silent := filepath.Join(r.w.cfg.WorkDir, "silent.mp4")
	if b, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=96x54:rate=25:duration=1",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", silent).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	_, err := r.workLoudness(context.Background(), silent, filepath.Join(r.w.cfg.WorkDir, "silent.json"))
	if f, ok := err.(*failure); !ok || f.code != "decode_failed" || !strings.Contains(f.msg, "no sound") {
		t.Fatalf("a file without sound: %v", err)
	}
}

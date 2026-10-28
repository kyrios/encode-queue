package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// **The loudness job** (v0.2.24; Thorsten, 2026-10-28, idea 6 of the NLE research: "Lautheit in
// LUFS"): how loud an upload is, by EBU R 128, for Funkhaus's Checks and a lane on its waveform.
// ffmpeg's ebur128 filter reads the source's first sound with true peak on; its per-100 ms lines
// go to an FFREPORT file (two a second for an hour would flood the lease's log) and its summary
// ends that file. The result is a small JSON file: the integrated loudness in LUFS, the loudness
// range in LU, the true peak in dBTP and the short-term loudness (its 3 s window) at the end of
// every second, null where it is below the absolute gate of -70 LUFS. A stereo 1 kHz tone at
// -23 dBFS reads -23.0 LUFS (EBU Tech 3341, case 1).

// Loudness is the loudness job's result.
type Loudness struct {
	Schema    int        `json:"schema"`
	LUFS      *float64   `json:"integrated_lufs"`
	LRA       *float64   `json:"lra_lu"`
	TruePeak  *float64   `json:"true_peak_dbtp"`
	EveryS    int        `json:"every_s"`
	ShortTerm []*float64 `json:"short_term"`
}

// loudnessGate is EBU R 128's absolute gate: quieter is silence.
const loudnessGate = -70.0

func loudnessArgs(ffmpeg, src string) []string {
	return []string{ffmpeg, "-hide_banner", "-nostdin", "-nostats", "-y", "-v", "error",
		"-threads", "1", "-i", src, "-map", "0:a:0", "-vn", "-sn", "-dn",
		"-af", "ebur128=peak=true:framelog=info", "-f", "null", "-"}
}

var (
	ebPrefix  = regexp.MustCompile(`\[Parsed_ebur128_\d+ @ 0x[0-9a-f]+\] ?`)
	ebFrame   = regexp.MustCompile(`\bt:\s*([0-9.]+)\s.*\bS:\s*(-?[0-9.]+|-?inf|nan)`)
	ebSummary = regexp.MustCompile(`^\s*(I|LRA|Peak):\s*(-?[0-9.]+|-?inf|nan)\s`)
)

// LoudnessFromLog reads ebur128's log: the short-term loudness at the end of each second, and
// from the summary the integrated loudness, the loudness range and the true peak.
func LoudnessFromLog(r io.Reader) Loudness {
	l := Loudness{Schema: 1, EveryS: 1, ShortTerm: []*float64{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	summary := false
	for sc.Scan() {
		line := ebPrefix.ReplaceAllString(sc.Text(), "")
		if strings.HasPrefix(strings.TrimSpace(line), "Summary:") {
			summary = true
			continue
		}
		if !summary {
			// A line every 100 ms; the one at a whole second (t 0.999979 is the first) closes it.
			if m := ebFrame.FindStringSubmatch(line); m != nil {
				t, _ := strconv.ParseFloat(m[1], 64)
				if tenths := int(math.Round(t * 10)); tenths > 0 && tenths%10 == 0 && tenths/10 == len(l.ShortTerm)+1 {
					l.ShortTerm = append(l.ShortTerm, gated(m[2]))
				}
			}
			continue
		}
		if m := ebSummary.FindStringSubmatch(line); m != nil {
			v := number(m[2])
			switch m[1] {
			case "I":
				l.LUFS = gatedValue(v)
			case "LRA":
				l.LRA = v
			case "Peak":
				l.TruePeak = v
			}
		}
	}
	return l
}

// number is a figure of the log rounded to 0.1, nil for -inf and nan.
func number(s string) *float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
		return nil
	}
	v = math.Round(v*10) / 10
	return &v
}

func gated(s string) *float64 { return gatedValue(number(s)) }

// gatedValue is v, or nil at or below the absolute gate (silence).
func gatedValue(v *float64) *float64 {
	if v == nil || *v <= loudnessGate {
		return nil
	}
	return v
}

// workLoudness runs a loudness job: ebur128 over the source's first sound, its log read into
// the JSON result. A source without sound fails as decode_failed, as a peaks job does.
func (r *run) workLoudness(ctx context.Context, src, out string) ([]Tool, error) {
	logPath, err := r.runStripPass(ctx, loudnessArgs(r.w.cfg.FFmpeg, src), "loudness.log")
	if err != nil {
		if f, ok := err.(*failure); ok && f.code == "encode_failed" && noSoundIn(r.dir, "loudness.log", f.msg) {
			return nil, fail("decode_failed", "the source has no sound")
		}
		return nil, err
	}
	f, err := os.Open(logPath)
	if err != nil {
		return nil, fail("decode_failed", "ffmpeg wrote no log: %v", err)
	}
	l := LoudnessFromLog(f)
	f.Close()
	if l.LRA == nil {
		return nil, fail("decode_failed", "ebur128 gave no summary")
	}
	b, err := json.Marshal(l)
	if err != nil {
		return nil, fail("other", "%v", err)
	}
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		return nil, fail("other", "%v", err)
	}
	r.log("info", "worker", "loudness: "+strings.TrimSpace(string(b[:min(len(b), 120)])))
	return r.w.toolsNamed("ffmpeg"), nil
}

// noSoundIn says whether ffmpeg's failure, or its report file, says the source has no sound.
func noSoundIn(dir, logName, msg string) bool {
	lines := []string{msg}
	if b, err := os.ReadFile(filepath.Join(dir, logName)); err == nil {
		lines = append(lines, strings.Split(string(b), "\n")...)
	}
	return noSound(lines)
}

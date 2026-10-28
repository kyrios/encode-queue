package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// **A clip's rung** (v0.2.13; video-pipeline v0.2.10, iPhoneblog-web/video-pipeline#20): one 9:16
// file for Instagram Reels, YouTube Shorts, Mastodon, Threads and Bluesky, at 23 to 60 frames per
// second, AAC at most 48 kHz and two channels, and without edit lists. So a rung may set `fps`,
// `audio.sample_rate`, `audio.channels` and `edit_list: false`. A worker before 0.2.13 reads its
// params loosely and drops what it does not know: it would make the file without them, so such
// a rung goes only to a worker of 0.2.13 or later (clipFields, minVersion).

// FPS is a rung's frame rate: a JSON number (60, 29.97) or a ratio as text ("30000/1001"), kept
// as the text ffmpeg's fps filter reads.
type FPS string

var fpsRe = regexp.MustCompile(`^[0-9]{1,6}(\.[0-9]{1,3})?$|^[0-9]{1,6}/[0-9]{1,6}$`)

func (f *FPS) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	s := string(b)
	if len(b) > 0 && b[0] == '"' {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	}
	if FPS(s).Value() == 0 {
		return fmt.Errorf("fps is a number or a ratio such as \"30000/1001\", from 1 to 240, not %s", b)
	}
	*f = FPS(s)
	return nil
}

func (f FPS) MarshalJSON() ([]byte, error) {
	if strings.Contains(string(f), "/") {
		return json.Marshal(string(f))
	}
	return []byte(f), nil
}

// Value is the rate in frames per second, or 0 when it is no rate from 1 to 240.
func (f FPS) Value() float64 {
	s := string(f)
	if !fpsRe.MatchString(s) {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	if n, d, ok := strings.Cut(s, "/"); ok {
		nf, _ := strconv.ParseFloat(n, 64)
		df, _ := strconv.ParseFloat(d, 64)
		if df == 0 {
			return 0
		}
		v = nf / df
	}
	if v < 1 || v > 240 {
		return 0
	}
	return v
}

// aacRates are the sample rates ffmpeg's AAC encoder takes.
var aacRates = []int{8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000, 64000, 88200, 96000}

// validateClip checks a rung's sample rate and channels: AAC only, a rate the encoder takes, 1 to
// 8 channels, and not beside a mono downmix, which sets the channels itself.
func validateClip(p *RungParams) error {
	a := p.Audio
	if a == nil || (a.SampleRate == 0 && a.Channels == 0) {
		return nil
	}
	if a.Mode != "aac" {
		return fmt.Errorf("audio.sample_rate and audio.channels need audio.mode aac")
	}
	if a.SampleRate != 0 && !containsInt(aacRates, a.SampleRate) {
		return fmt.Errorf("audio.sample_rate %d is not one of %v", a.SampleRate, aacRates)
	}
	if a.Channels < 0 || a.Channels > 8 {
		return fmt.Errorf("audio.channels is 1 to 8")
	}
	if a.Channels != 0 && (a.Mono.On || a.Mono.Measure) {
		return fmt.Errorf("audio.channels and audio.mono both set the channels; send one")
	}
	return nil
}

func containsInt(list []int, x int) bool {
	for _, v := range list {
		if v == x {
			return true
		}
	}
	return false
}

// clipFields names what a rung asks for that only a worker of 0.2.13 or later does; nil for a
// rung without them and for every other kind.
func clipFields(j *Job) []string {
	r := j.rung
	if r == nil {
		return nil
	}
	var f []string
	if r.FPS != nil {
		f = append(f, "fps")
	}
	if a := r.Audio; a != nil {
		if a.SampleRate != 0 {
			f = append(f, "audio.sample_rate")
		}
		if a.Channels != 0 {
			f = append(f, "audio.channels")
		}
	}
	if r.EditList != nil && !*r.EditList {
		f = append(f, "edit_list")
	}
	return f
}

// minVersion is the oldest worker version that makes the job as asked, and what needs it; "" for
// a job every worker runs.
func minVersion(j *Job) (version, what string) {
	if f := clipFields(j); len(f) > 0 {
		return "0.2.13", "sets " + strings.Join(f, ", ")
	}
	if needsV028(j) {
		return "0.2.8", "runs cut, remux and check jobs and the check step"
	}
	return "", ""
}

// outFrames is how many frames the job's file will have, for the time it takes: the source's,
// or with a rung's fps its length times the rate.
func (j *Job) outFrames() int64 {
	if r := j.rung; r != nil && r.FPS != nil && j.Spec.Source.DurationS > 0 {
		return int64(math.Round(j.Spec.Source.DurationS * r.FPS.Value()))
	}
	return j.Spec.Source.Frames
}

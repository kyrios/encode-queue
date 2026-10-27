package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
)

// **The job kinds of v0.2.8** (ADR-0007: every encode and every decode runs on a worker; the
// pipeline reads headers only; helper.yaml is the worker side's, the video-pipeline session's).
// The queue keeps their parameters strict, matches them on the reference's ffmpeg (and libx264
// where they encode), and stores the figures a check finds for the pipeline to judge. A `check`
// job delivers figures without a file.

const (
	KindCut   = "cut"
	KindRemux = "remux"
	KindCheck = "check"
)

// Mono is audio.mono: true, false, or "measure" (v0.2.8: the worker measures mid and side and
// downmixes when the side is silent or far below the mid).
type Mono struct {
	On      bool
	Measure bool
}

func (m *Mono) UnmarshalJSON(b []byte) error {
	switch string(bytes.TrimSpace(b)) {
	case "true":
		*m = Mono{On: true}
	case "false", "null":
		*m = Mono{}
	case `"measure"`:
		*m = Mono{Measure: true}
	default:
		return fmt.Errorf("mono is true, false or \"measure\", not %s", b)
	}
	return nil
}

func (m Mono) MarshalJSON() ([]byte, error) {
	if m.Measure {
		return []byte(`"measure"`), nil
	}
	return json.Marshal(m.On)
}

// CheckSettings: what the worker checks of its result in the same lease (v0.2.8).
type CheckSettings struct {
	Decode   *bool         `json:"decode,omitempty"`
	Frames   *bool         `json:"frames,omitempty"`
	Compare  *CheckCompare `json:"compare,omitempty"`
	Framemd5 bool          `json:"framemd5,omitempty"`
}

type CheckCompare struct {
	Every   int      `json:"every,omitempty"`
	Metrics []string `json:"metrics,omitempty"`
	VMAF    *struct {
		Model string `json:"model"`
	} `json:"vmaf,omitempty"`
}

// CutParams: a stretch of the source as its own file (a #one preview from its Folge).
type CutParams struct {
	StartS   float64        `json:"start_s"`
	LengthS  float64        `json:"length_s"`
	FadeS    float64        `json:"fade_s,omitempty"`
	Preset   string         `json:"preset"`
	CRF      float64        `json:"crf"`
	BitDepth int            `json:"bit_depth"`
	Colour   *Colour        `json:"colour,omitempty"`
	Audio    *CutAudio      `json:"audio,omitempty"`
	Check    *CheckSettings `json:"check,omitempty"`
}

type Colour struct {
	Primaries string `json:"primaries,omitempty"`
	Transfer  string `json:"transfer,omitempty"`
	Space     string `json:"space,omitempty"`
}

type CutAudio struct {
	BitrateKbps int `json:"bitrate_kbps"`
}

// RemuxParams: the source's streams in a new MP4, untouched.
type RemuxParams struct {
	HVC1Tag bool           `json:"hvc1_tag,omitempty"`
	Check   *CheckSettings `json:"check,omitempty"`
}

// CheckParams: only the check, for a file no worker encoded; the result is figures.
type CheckParams struct {
	Candidate string         `json:"candidate,omitempty"`
	Width     int            `json:"width,omitempty"`
	Height    int            `json:"height,omitempty"`
	Audio     *bool          `json:"audio,omitempty"`
	Check     *CheckSettings `json:"check"`
}

func (p CheckParams) candidate() string {
	if p.Candidate == "" {
		return "candidate"
	}
	return p.Candidate
}

var (
	colourRe  = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
	metricSet = []string{"ssim", "psnr_y", "xpsnr"}
	vmafRe    = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)
)

func (c *CheckSettings) validate() error {
	if c == nil || c.Compare == nil {
		return nil
	}
	if c.Compare.Every < 0 {
		return fmt.Errorf("check.compare.every is 1 or more")
	}
	for _, m := range c.Compare.Metrics {
		if !contains(metricSet, m) {
			return fmt.Errorf("check.compare.metrics: %q is not one of %v", m, metricSet)
		}
	}
	if v := c.Compare.VMAF; v != nil && !vmafRe.MatchString(v.Model) {
		return fmt.Errorf("check.compare.vmaf.model %q", v.Model)
	}
	return nil
}

// validateKind checks the params of the v0.2.8 kinds; nil for the others.
func validateKind(j *Job, spec *JobSubmit) error {
	switch {
	case j.cut != nil:
		p := j.cut
		if p.StartS < 0 || p.LengthS <= 0 || p.FadeS < 0 || p.FadeS > p.LengthS {
			return fmt.Errorf("cut needs start_s ≥ 0, length_s > 0 and fade_s between 0 and length_s")
		}
		if !contains(presets, p.Preset) {
			return fmt.Errorf("preset %q unknown", p.Preset)
		}
		if p.BitDepth != 8 && p.BitDepth != 10 {
			return fmt.Errorf("bit_depth is 8 or 10")
		}
		if c := p.Colour; c != nil {
			for _, v := range []string{c.Primaries, c.Transfer, c.Space} {
				if v != "" && !colourRe.MatchString(v) {
					return fmt.Errorf("colour %q", v)
				}
			}
		}
		if p.Audio != nil && p.Audio.BitrateKbps < 1 {
			return fmt.Errorf("cut audio needs bitrate_kbps")
		}
		return p.Check.validate()
	case j.remux != nil:
		return j.remux.Check.validate()
	case j.check != nil:
		p := j.check
		if p.Check == nil {
			return fmt.Errorf("a check job needs check")
		}
		if _, ok := spec.Inputs[p.candidate()]; !ok {
			return fmt.Errorf("the candidate %q is not among the inputs", p.candidate())
		}
		if p.Check.Compare != nil && (p.Width < 2 || p.Height < 2) {
			return fmt.Errorf("compare needs the candidate's width and height")
		}
		return p.Check.validate()
	case j.rung != nil:
		return j.rung.Check.validate()
	}
	return nil
}

// maxFigures is the most a check's figures may weigh.
const maxFigures = 64 << 10

// checkFigures takes a result's figures: a JSON object, or nothing.
func checkFigures(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil
	}
	if len(raw) > maxFigures {
		return nil, fmt.Errorf("check is more than %d bytes", maxFigures)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("check is not an object: %v", err)
	}
	return raw, nil
}

// needsV028 says whether a job asks for what only a worker of 0.2.8 or later runs: a new kind, a
// rung's check step, or mono "measure" (an older worker reads mono as a bool and fails on it).
func needsV028(j *Job) bool {
	if j.cut != nil || j.remux != nil || j.check != nil {
		return true
	}
	if r := j.rung; r != nil {
		return r.Check != nil || (r.Audio != nil && r.Audio.Mono.Measure)
	}
	return false
}

package worker

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// RungParams are an encode's settings (helper.yaml).
type RungParams struct {
	Rung       string                             `json:"rung"`
	Codec      string                             `json:"codec"`
	Preset     string                             `json:"preset"`
	CRF        float64                            `json:"crf"`
	Width      int                                `json:"width"`
	Height     int                                `json:"height"`
	Crop       *struct{ Width, Height, X, Y int } `json:"crop"`
	VBVMaxrate *int                               `json:"vbv_maxrate_kbps"`
	VBVBufsize *int                               `json:"vbv_bufsize_kbit"`
	Audio      *AudioParams                       `json:"audio"`
	Captions   *struct {
		Input string `json:"input"`
		Font  string `json:"font"`
	} `json:"captions"`
	Check *CheckSettings `json:"check"`
	// FPS and EditList (v0.2.13, a clip for Instagram and Threads): the rate the file goes out at,
	// and false for an MP4 without edit lists.
	FPS      *FPS  `json:"fps"`
	EditList *bool `json:"edit_list"`
}

// AudioParams is what happens to a rung's sound.
type AudioParams struct {
	Mode            string   `json:"mode"`
	BitrateKbps     int      `json:"bitrate_kbps"`
	Mono            Mono     `json:"mono"`
	MonoSideBelowDB *float64 `json:"mono_side_below_db"`
	// SampleRate and Channels: the AAC's (v0.2.13); 0 keeps the source's.
	SampleRate int `json:"sample_rate"`
	Channels   int `json:"channels"`
}

// FPS is `fps` (v0.2.13): a JSON number or a ratio as text ("30000/1001"), kept as the text
// ffmpeg's fps filter reads. rungArgs checks it.
type FPS string

func (f *FPS) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if strings.HasPrefix(s, `"`) {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	}
	*f = FPS(s)
	return nil
}

// value is the rate, or 0 when it is no rate from 1 to 240 (the queue's bounds).
func (f FPS) value() float64 {
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

// frames is how many frames a file of durS seconds has at this rate; 0 when the length is unknown.
func (f FPS) frames(durS float64) int64 {
	if durS <= 0 {
		return 0
	}
	return int64(math.Round(durS * f.value()))
}

// Mono is `mono`: true, false, or "measure" (v0.2.8), which the worker turns into true or false
// before it builds the command.
type Mono struct{ On, Measure bool }

func (m *Mono) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case "true":
		*m = Mono{On: true}
	case "false", "null":
		*m = Mono{}
	case `"measure"`:
		*m = Mono{Measure: true}
	default:
		return fmt.Errorf("mono %s: true, false or \"measure\"", b)
	}
	return nil
}

func (m Mono) MarshalJSON() ([]byte, error) {
	if m.Measure {
		return []byte(`"measure"`), nil
	}
	return []byte(strconv.FormatBool(m.On)), nil
}

// WhisperParams are a whisper run's settings.
type WhisperParams struct {
	ModelSHA256 string `json:"model_sha256"`
	Model       string `json:"model"`
	Language    string `json:"language"`
	Threads     int    `json:"threads"`
}

// The downmix of a two-channel source that carries one signal (video-encode's MONO_PAN).
const monoPan = "pan=mono|c0=0.5*c0+0.5*c1"

var (
	presetRe     = regexp.MustCompile(`^(ultrafast|superfast|veryfast|faster|fast|medium|slow|slower|veryslow)$`)
	filterPathRe = regexp.MustCompile(`^[A-Za-z0-9/._-]+$`)
	languageRe   = regexp.MustCompile(`^[a-z]{2,3}$|^auto$`)
	fpsRe        = regexp.MustCompile(`^[0-9]{1,6}(\.[0-9]{1,3})?$|^[0-9]{1,6}/[0-9]{1,6}$`)
	// The sample rates ffmpeg's AAC encoder takes.
	aacRates = map[int]bool{8000: true, 11025: true, 12000: true, 16000: true, 22050: true, 24000: true,
		32000: true, 44100: true, 48000: true, 64000: true, 88200: true, 96000: true}
)

func num(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// rungArgs builds ffmpeg's arguments for a rung in the order video-encode used, so the file is
// the one `encoder` would make. srcW and srcH are the source's size (0 when unknown, which
// scales always); threads is the x265 pool's size (0: x265's own). x264 keeps its own
// threading, as video-encode did.
func rungArgs(ffmpeg string, p RungParams, src string, srcW, srcH int, ass, out string, threads int) ([]string, error) {
	if !presetRe.MatchString(p.Preset) {
		return nil, fmt.Errorf("preset %q unknown", p.Preset)
	}
	if p.Width < 2 || p.Height < 2 || p.Width%2 != 0 || p.Height%2 != 0 {
		return nil, fmt.Errorf("size %dx%d", p.Width, p.Height)
	}
	audio := "none"
	if p.Audio != nil {
		audio = p.Audio.Mode
	}
	a := []string{ffmpeg, "-hide_banner", "-nostdin", "-y", "-loglevel", "error", "-nostats",
		"-progress", "pipe:1", "-i", src, "-map", "0:v:0"}
	if audio != "none" {
		a = append(a, "-map", "0:a:0")
	}
	a = append(a, "-fps_mode", "passthrough")
	inW, inH := srcW, srcH
	var vf []string
	if p.Crop != nil {
		vf = append(vf, fmt.Sprintf("crop=%d:%d:%d:%d", p.Crop.Width, p.Crop.Height, p.Crop.X, p.Crop.Y))
		inW, inH = p.Crop.Width, p.Crop.Height
	}
	// The rate after the crop and before the scale: a 120 fps source drops half its frames before
	// the scale works on them. The captions come later, drawn at the output frames' own times.
	if p.FPS != nil {
		if p.FPS.value() == 0 {
			return nil, fmt.Errorf("fps %q is no rate from 1 to 240", string(*p.FPS))
		}
		vf = append(vf, "fps="+string(*p.FPS))
	}
	if inW != p.Width || inH != p.Height {
		vf = append(vf, fmt.Sprintf("scale=%d:%d:flags=lanczos", p.Width, p.Height))
	}
	switch p.Codec {
	case "libx265":
		if p.Captions != nil {
			return nil, fmt.Errorf("captions are burnt in with libx264 only")
		}
		if len(vf) > 0 {
			a = append(a, "-vf", join(vf))
		}
		params := "log-level=error"
		if p.VBVMaxrate != nil {
			buf := 2 * *p.VBVMaxrate
			if p.VBVBufsize != nil {
				buf = *p.VBVBufsize
			}
			params += fmt.Sprintf(":vbv-maxrate=%d:vbv-bufsize=%d", *p.VBVMaxrate, buf)
		}
		if threads > 0 {
			params += fmt.Sprintf(":pools=%d", threads)
		}
		a = append(a, "-c:v", "libx265", "-preset", p.Preset, "-crf", num(p.CRF),
			"-pix_fmt", "yuv420p10le", "-profile:v", "main10", "-tag:v", "hvc1", "-x265-params", params)
	case "libx264":
		vf = append(vf, "format=yuv420p")
		if p.Captions != nil {
			if !filterPathRe.MatchString(ass) {
				return nil, fmt.Errorf("the captions' path %q cannot go into a filter", ass)
			}
			vf = append(vf, "ass=filename="+ass)
		}
		a = append(a, "-vf", join(vf), "-c:v", "libx264", "-preset", p.Preset, "-crf", num(p.CRF))
		if p.VBVMaxrate != nil {
			buf := 2 * *p.VBVMaxrate
			if p.VBVBufsize != nil {
				buf = *p.VBVBufsize
			}
			a = append(a, "-maxrate", fmt.Sprintf("%dk", *p.VBVMaxrate), "-bufsize", fmt.Sprintf("%dk", buf))
		}
		a = append(a, "-profile:v", "high", "-pix_fmt", "yuv420p")
	default:
		return nil, fmt.Errorf("codec %q unknown", p.Codec)
	}
	if p.Audio != nil && (p.Audio.SampleRate != 0 || p.Audio.Channels != 0) {
		switch {
		case audio != "aac":
			return nil, fmt.Errorf("audio sample_rate and channels need mode aac")
		case p.Audio.SampleRate != 0 && !aacRates[p.Audio.SampleRate]:
			return nil, fmt.Errorf("audio sample_rate %d", p.Audio.SampleRate)
		case p.Audio.Channels < 0 || p.Audio.Channels > 8:
			return nil, fmt.Errorf("audio channels %d", p.Audio.Channels)
		case p.Audio.Channels != 0 && p.Audio.Mono.On:
			return nil, fmt.Errorf("audio channels beside a mono downmix")
		}
	}
	switch audio {
	case "none":
	case "copy":
		a = append(a, "-c:a", "copy")
	case "aac":
		if p.Audio.Mono.On {
			a = append(a, "-af", monoPan)
		}
		if p.Audio.BitrateKbps < 1 {
			return nil, fmt.Errorf("audio aac without bitrate_kbps")
		}
		a = append(a, "-c:a", "aac", "-b:a", fmt.Sprintf("%dk", p.Audio.BitrateKbps))
		if p.Audio.SampleRate != 0 {
			a = append(a, "-ar", strconv.Itoa(p.Audio.SampleRate))
		}
		if p.Audio.Channels != 0 {
			a = append(a, "-ac", strconv.Itoa(p.Audio.Channels))
		}
	default:
		return nil, fmt.Errorf("audio mode %q unknown", audio)
	}
	a = append(a, "-map_metadata", "-1", "-map_chapters", "-1")
	if p.EditList != nil && !*p.EditList {
		// The mov muxer then shifts both tracks so the first dts is 0 and stretches the earlier
		// track's first sample to the later one's start: the AAC priming plays before the sound.
		a = append(a, "-use_editlist", "0")
	}
	a = append(a, "-movflags", "+faststart", out)
	return a, nil
}

func join(parts []string) string {
	s := ""
	for i, p := range parts {
		if i > 0 {
			s += ","
		}
		s += p
	}
	return s
}

// speechArgs turns a source's sound into 16 kHz mono PCM for whisper.cpp (video-encode's
// speech_args).
func speechArgs(ffmpeg, src, wav string) []string {
	return []string{ffmpeg, "-hide_banner", "-nostdin", "-y", "-v", "error", "-i", src,
		"-map", "0:a:0", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", wav}
}

// whisperArgs is whisper-cli greedy, one segment per word, JSON to base.json, with progress.
func whisperArgs(cli, model string, p WhisperParams, wav, base string, threads int, gpu bool) ([]string, error) {
	if !languageRe.MatchString(p.Language) {
		return nil, fmt.Errorf("language %q", p.Language)
	}
	a := []string{cli, "-m", model, "-l", p.Language, "-f", wav, "-t", strconv.Itoa(threads),
		"-bs", "1", "-bo", "1", "-ml", "1", "-sow", "-oj", "-of", base, "-pp"}
	if !gpu {
		a = append(a, "-ng")
	}
	return a, nil
}

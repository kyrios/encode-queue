package worker

import (
	"fmt"
	"regexp"
	"strconv"
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
	Audio      *struct {
		Mode        string `json:"mode"`
		BitrateKbps int    `json:"bitrate_kbps"`
		Mono        bool   `json:"mono"`
	} `json:"audio"`
	Captions *struct {
		Input string `json:"input"`
		Font  string `json:"font"`
	} `json:"captions"`
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
	switch audio {
	case "none":
	case "copy":
		a = append(a, "-c:a", "copy")
	case "aac":
		if p.Audio.Mono {
			a = append(a, "-af", monoPan)
		}
		if p.Audio.BitrateKbps < 1 {
			return nil, fmt.Errorf("audio aac without bitrate_kbps")
		}
		a = append(a, "-c:a", "aac", "-b:a", fmt.Sprintf("%dk", p.Audio.BitrateKbps))
	default:
		return nil, fmt.Errorf("audio mode %q unknown", audio)
	}
	a = append(a, "-map_metadata", "-1", "-map_chapters", "-1", "-movflags", "+faststart", out)
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

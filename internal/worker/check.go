package worker

import (
	"bufio"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// **The check of a result runs on the worker that made it** (ADR-0007; Thorsten, 2026-10-27:
// "Trust the figures"). After the encode, in the same lease and working folder, the worker
// decodes its file and compares it with the source; the figures go with the result, and the
// pipeline, which reads headers only, compares them with its floors. A worker never sees a
// floor: it measures, the pipeline decides.

// CheckSettings is what the check measures (helper.yaml).
type CheckSettings struct {
	Decode   *bool            `json:"decode"`
	Frames   *bool            `json:"frames"`
	Compare  *CompareSettings `json:"compare"`
	Framemd5 bool             `json:"framemd5"`
}

// CompareSettings is the comparison with the source.
type CompareSettings struct {
	Every   int      `json:"every"`
	Metrics []string `json:"metrics"`
	VMAF    *struct {
		Model string `json:"model"`
	} `json:"vmaf"`
}

func (c *CheckSettings) decode() bool { return c == nil || c.Decode == nil || *c.Decode }
func (c *CheckSettings) frames() bool { return c == nil || c.Frames == nil || *c.Frames }

// CheckFigures is what the check found (helper.yaml).
type CheckFigures struct {
	Decode       *DecodeFigures   `json:"decode,omitempty"`
	Frames       *int64           `json:"frames,omitempty"`
	SourceFrames *int64           `json:"source_frames,omitempty"`
	DurationS    *float64         `json:"duration_s,omitempty"`
	Compare      *CompareFigures  `json:"compare,omitempty"`
	Framemd5     *Framemd5Figures `json:"framemd5,omitempty"`
	AudioLevels  *AudioLevels     `json:"audio_levels,omitempty"`
	CheckS       float64          `json:"check_s"`
}

type DecodeFigures struct {
	Errors int      `json:"errors"`
	Lines  []string `json:"lines,omitempty"`
}

type CompareFigures struct {
	Every     int    `json:"every"`
	Pairs     int64  `json:"pairs"`
	SSIM      any    `json:"ssim"`
	PSNRY     any    `json:"psnr_y"`
	XPSNR     any    `json:"xpsnr"`
	VMAF      any    `json:"vmaf"`
	VMAFModel string `json:"vmaf_model,omitempty"`
}

type Framemd5Figures struct {
	Match           bool    `json:"match"`
	Packets         int     `json:"packets"`
	FirstDifference *string `json:"first_difference"`
}

type AudioLevels struct {
	MidDB  any  `json:"mid_db"`
	SideDB any  `json:"side_db"`
	Mono   bool `json:"mono"`
}

// CutParams, RemuxParams and CheckParams are the v0.2.8 kinds' settings (helper.yaml).
type CutParams struct {
	StartS   float64 `json:"start_s"`
	LengthS  float64 `json:"length_s"`
	FadeS    float64 `json:"fade_s"`
	Preset   string  `json:"preset"`
	CRF      float64 `json:"crf"`
	BitDepth int     `json:"bit_depth"`
	Colour   struct {
		Primaries string `json:"primaries"`
		Transfer  string `json:"transfer"`
		Space     string `json:"space"`
	} `json:"colour"`
	Audio *struct {
		BitrateKbps int `json:"bitrate_kbps"`
	} `json:"audio"`
	Check *CheckSettings `json:"check"`
}

type RemuxParams struct {
	HVC1Tag bool           `json:"hvc1_tag"`
	Check   *CheckSettings `json:"check"`
}

type CheckParams struct {
	Candidate string         `json:"candidate"`
	Width     int            `json:"width"`
	Height    int            `json:"height"`
	Audio     *bool          `json:"audio"`
	Check     *CheckSettings `json:"check"`
}

var colourRe = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)

// cutArgs is the pipeline's cut_args: seeking before the input, which decodes from the
// keyframe before and starts at start_s exactly; the fade-out on picture and sound; x264 in the
// source's bit depth with its colour tags.
func cutArgs(ffmpeg string, p CutParams, src, out string) ([]string, error) {
	if !presetRe.MatchString(p.Preset) {
		return nil, fmt.Errorf("preset %q unknown", p.Preset)
	}
	if p.LengthS <= 0 || p.StartS < 0 || p.FadeS < 0 || p.FadeS > p.LengthS {
		return nil, fmt.Errorf("start %v, length %v, fade %v", p.StartS, p.LengthS, p.FadeS)
	}
	if p.BitDepth != 8 && p.BitDepth != 10 {
		return nil, fmt.Errorf("bit depth %d", p.BitDepth)
	}
	a := []string{ffmpeg, "-hide_banner", "-nostdin", "-y", "-loglevel", "error", "-nostats",
		"-progress", "pipe:1", "-ss", fmt.Sprintf("%.3f", p.StartS), "-i", src,
		"-t", fmt.Sprintf("%.3f", p.LengthS), "-map", "0:v:0"}
	if p.Audio != nil {
		a = append(a, "-map", "0:a:0")
	}
	vf, profile := "format=yuv420p", "high"
	if p.BitDepth == 10 {
		vf, profile = "format=yuv420p10le", "high10"
	}
	if p.FadeS > 0 {
		vf = fmt.Sprintf("fade=t=out:st=%.3f:d=%s:color=black,", p.LengthS-p.FadeS, num(p.FadeS)) + vf
	}
	a = append(a, "-vf", vf, "-c:v", "libx264", "-preset", p.Preset, "-crf", num(p.CRF), "-profile:v", profile)
	for _, c := range []struct{ opt, v string }{{"-color_primaries", p.Colour.Primaries},
		{"-color_trc", p.Colour.Transfer}, {"-colorspace", p.Colour.Space}} {
		if c.v == "" {
			continue
		}
		if !colourRe.MatchString(c.v) {
			return nil, fmt.Errorf("colour %q", c.v)
		}
		a = append(a, c.opt, c.v)
	}
	if p.Audio != nil {
		if p.Audio.BitrateKbps < 1 {
			return nil, fmt.Errorf("audio without bitrate_kbps")
		}
		if p.FadeS > 0 {
			a = append(a, "-af", fmt.Sprintf("afade=t=out:st=%.3f:d=%s", p.LengthS-p.FadeS, num(p.FadeS)))
		}
		a = append(a, "-c:a", "aac", "-b:a", fmt.Sprintf("%dk", p.Audio.BitrateKbps))
	}
	return append(a, "-map_metadata", "-1", "-map_chapters", "-1", "-movflags", "+faststart", out), nil
}

// remuxArgs is the pipeline's remux_args: the streams untouched in a new MP4, the sound when
// there is one.
func remuxArgs(ffmpeg string, p RemuxParams, src, out string) []string {
	a := []string{ffmpeg, "-hide_banner", "-nostdin", "-y", "-loglevel", "error", "-nostats",
		"-progress", "pipe:1", "-i", src, "-map", "0:v:0", "-map", "0:a:0?", "-c", "copy"}
	if p.HVC1Tag {
		a = append(a, "-tag:v", "hvc1")
	}
	return append(a, "-movflags", "+faststart", out)
}

// decodeArgs decodes every stream of a file and throws the frames away; -progress counts
// them. The null muxer's complaints about equal timestamps of variable-rate frames are not the
// file's fault (decodeErrors drops them).
func decodeArgs(ffmpeg, path string, video bool) []string {
	a := []string{ffmpeg, "-hide_banner", "-nostdin", "-v", "error", "-xerror", "-nostats",
		"-progress", "pipe:1", "-threads", "2", "-i", path}
	if video {
		a = append(a, "-map", "0:v:0")
	} else {
		a = append(a, "-map", "0")
	}
	return append(a, "-f", "null", "-")
}

func decodeErrors(lines []string) []string {
	var out []string
	for _, l := range lines {
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(l, "[null @") {
			out = append(out, l)
		}
	}
	return out
}

// scoreThreads bounds the comparison as the pipeline's SCORE_THREADS did: two decoder threads
// per input and for the filter graph, libvmaf without a pool. A 4K comparison peaks at about
// 0.7 GB, 1.1 GB with libvmaf (video-pipeline, ScorePeak).
const scoreThreads = "2"

var vmafModelRe = regexp.MustCompile(`^[a-z0-9_.]{1,40}$`)

// compareArgs compares a result with its source scaled to the result's size, on every Nth
// frame. pts become 0, 1, 2 … on both sides, so pairs line up by count, and fps=1 names that
// rate (xpsnr divides by it). SSIM and PSNR on the 10-bit path; XPSNR and libvmaf in 8 bits
// (ffmpeg's vf_xpsnr misreads 10-bit frames whose stride is not their width). srcW and srcH
// are the source's size. rate is a rung's fps (v0.2.13; "" for none): the source goes through
// the same fps filter first, which picks the frames the encode kept, so the pairs show one
// moment each, not the 120 fps source's frame n beside the 60 fps result's.
func compareArgs(ffmpeg string, c CompareSettings, result, source string, w, h, srcW, srcH int, rate FPS) ([]string, error) {
	every := c.Every
	if every < 1 {
		every = 1
	}
	metrics := c.Metrics
	if metrics == nil {
		metrics = []string{"ssim", "psnr_y", "xpsnr"}
	}
	want := map[string]bool{}
	for _, m := range metrics {
		switch m {
		case "ssim", "psnr_y", "xpsnr":
			want[m] = true
		default:
			return nil, fmt.Errorf("metric %q unknown", m)
		}
	}
	model := ""
	if c.VMAF != nil {
		if !vmafModelRe.MatchString(c.VMAF.Model) {
			return nil, fmt.Errorf("vmaf model %q", c.VMAF.Model)
		}
		model = c.VMAF.Model
	}
	var steps []string
	for _, m := range []string{"ssim", "psnr_y"} {
		if want[m] {
			steps = append(steps, map[string]string{"ssim": "ssim", "psnr_y": "psnr"}[m])
		}
	}
	n := len(steps)
	if want["xpsnr"] {
		n++
	}
	if model != "" {
		n++
	}
	if n == 0 {
		return nil, fmt.Errorf("nothing to compare")
	}
	sel := fmt.Sprintf("select='not(mod(n\\,%d))',setpts=N/TB,fps=1", every)
	scale := ""
	if srcW != w || srcH != h {
		scale = fmt.Sprintf("scale=%d:%d:flags=lanczos,", w, h)
	}
	resample := ""
	if rate != "" {
		if rate.value() == 0 {
			return nil, fmt.Errorf("fps %q is no rate from 1 to 240", string(rate))
		}
		resample = "fps=" + string(rate) + ","
	}
	g := fmt.Sprintf("[0:v]%s,format=yuv420p10le[d0];[1:v]%s%s,%sformat=yuv420p10le,split=%d", sel, resample, sel, scale, n)
	for i := 1; i <= n; i++ {
		g += fmt.Sprintf("[r%d]", i)
	}
	cur, i := "d0", 0
	for _, f := range steps {
		i++
		g += fmt.Sprintf(";[%s][r%d]%s=shortest=1[d%d]", cur, i, f, i)
		cur = fmt.Sprintf("d%d", i)
	}
	if want["xpsnr"] || model != "" {
		g += fmt.Sprintf(";[%s]format=yuv420p[e0]", cur)
		cur = "e0"
	}
	if want["xpsnr"] {
		i++
		g += fmt.Sprintf(";[r%d]format=yuv420p[q%d];[%s][q%d]xpsnr=shortest=1[e1]", i, i, cur, i)
		cur = "e1"
	}
	if model != "" {
		i++
		g += fmt.Sprintf(";[r%d]format=yuv420p[q%d];[%s][q%d]libvmaf=model=version=%s:shortest=1[e2]", i, i, cur, i, model)
		cur = "e2"
	}
	return []string{ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "info", "-nostats",
		"-progress", "pipe:1",
		"-threads", scoreThreads, "-i", result, "-threads", scoreThreads, "-i", source,
		"-filter_complex_threads", scoreThreads, "-filter_complex", g, "-map", "[" + cur + "]",
		"-f", "null", "-"}, nil
}

var (
	ssimRe  = regexp.MustCompile(`SSIM Y:([\d.]+)`)
	psnrRe  = regexp.MustCompile(`PSNR y:([\d.]+|inf)`)
	xpsnrRe = regexp.MustCompile(`XPSNR\s+y:\s*([\d.]+|inf)`)
	vmafRe  = regexp.MustCompile(`VMAF score:\s*([\d.]+)`)
)

// parseScores reads ffmpeg's summary lines: a number rounded to four places, "inf" for
// identical pictures, nil for a metric that printed nothing.
func parseScores(text string) (ssim, psnr, xpsnr, vmaf any) {
	get := func(re *regexp.Regexp) any {
		m := re.FindStringSubmatch(text)
		if m == nil {
			return nil
		}
		if m[1] == "inf" {
			return "inf"
		}
		f, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return nil
		}
		return math.Round(f*1e4) / 1e4
	}
	return get(ssimRe), get(psnrRe), get(xpsnrRe), get(vmafRe)
}

// framemd5Args lists every packet of a file's video and first sound, when asked and there is
// one, without decoding them.
func framemd5Args(ffmpeg, path string, audio bool) []string {
	a := []string{ffmpeg, "-hide_banner", "-nostdin", "-v", "error", "-i", path, "-map", "0:v:0"}
	if audio {
		a = append(a, "-map", "0:a:0?")
	}
	return append(a, "-c", "copy", "-f", "framemd5", "-")
}

type packet struct {
	stream             int
	dts, pts, duration *big.Rat
	size               int64
	md5, side          string
}

var tbRe = regexp.MustCompile(`^#tb (\d+): (\d+)/(\d+)\s*$`)

// parseFramemd5 is the pipeline's parse_framemd5: every packet with its times in seconds as
// exact fractions. **Seconds, not the stream's time base**: ffmpeg's MP4 muxer writes a remux
// in a timescale of its own (an Apple file counts in 1/600 s, its remux in 1/19200), so the same
// packet prints different numbers.
func parseFramemd5(text string) ([]packet, error) {
	bases := map[int]*big.Rat{}
	var out []packet
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if m := tbRe.FindStringSubmatch(line); m != nil {
			s, _ := strconv.Atoi(m[1])
			n, _ := strconv.ParseInt(m[2], 10, 64)
			d, _ := strconv.ParseInt(m[3], 10, 64)
			if d == 0 {
				return nil, fmt.Errorf("time base %s", line)
			}
			bases[s] = big.NewRat(n, d)
			continue
		}
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, ",")
		if len(f) < 6 {
			return nil, fmt.Errorf("packet line %q", line)
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
		}
		s, err := strconv.Atoi(f[0])
		if err != nil {
			return nil, fmt.Errorf("packet line %q", line)
		}
		tb := bases[s]
		if tb == nil {
			tb = big.NewRat(1, 1)
		}
		ints := make([]int64, 4)
		for i := 1; i <= 4; i++ {
			if ints[i-1], err = strconv.ParseInt(f[i], 10, 64); err != nil {
				return nil, fmt.Errorf("packet line %q", line)
			}
		}
		at := func(v int64) *big.Rat { return new(big.Rat).Mul(big.NewRat(v, 1), tb) }
		out = append(out, packet{stream: s, dts: at(ints[0]), pts: at(ints[1]), duration: at(ints[2]),
			size: ints[3], md5: f[5], side: strings.Join(f[6:], ",")})
	}
	return out, sc.Err()
}

// comparePackets is nil when both lists are the same packet for packet, else the first
// difference for a person.
func comparePackets(src, res []packet) *string {
	diff := func(format string, args ...any) *string { s := fmt.Sprintf(format, args...); return &s }
	for i := 0; i < len(src) && i < len(res); i++ {
		a, b := src[i], res[i]
		switch {
		case a.stream != b.stream:
			return diff("packet %d: stream %d against %d", i, b.stream, a.stream)
		case a.dts.Cmp(b.dts) != 0:
			return diff("packet %d of stream %d: dts %s s against %s s", i, a.stream, b.dts.RatString(), a.dts.RatString())
		case a.pts.Cmp(b.pts) != 0:
			return diff("packet %d of stream %d: pts %s s against %s s", i, a.stream, b.pts.RatString(), a.pts.RatString())
		case a.duration.Cmp(b.duration) != 0:
			return diff("packet %d of stream %d: duration %s s against %s s", i, a.stream, b.duration.RatString(), a.duration.RatString())
		case a.size != b.size:
			return diff("packet %d of stream %d: size %d against %d", i, a.stream, b.size, a.size)
		case a.md5 != b.md5:
			return diff("packet %d of stream %d: md5 differs", i, a.stream)
		case a.side != b.side:
			return diff("packet %d of stream %d: side data differs", i, a.stream)
		}
	}
	if len(src) != len(res) {
		return diff("%d packets against the source's %d", len(res), len(src))
	}
	return nil
}

// midSideArgs measures a two-channel sound's mid (channel 1) and side (channel 2), the
// pipeline's mid_side_args.
func midSideArgs(ffmpeg, src string) []string {
	return []string{ffmpeg, "-hide_banner", "-nostdin", "-v", "info", "-nostats", "-i", src,
		"-map", "0:a:0", "-af", "pan=stereo|c0=0.5*c0+0.5*c1|c1=0.5*c0-0.5*c1," +
			"astats=measure_perchannel=RMS_level:measure_overall=none", "-f", "null", "-"}
}

var (
	channelRe = regexp.MustCompile(`\bChannel: (\d+)\s*$`)
	rmsRe     = regexp.MustCompile(`\bRMS level dB: (-inf|-?\d+(?:\.\d+)?)\s*$`)
)

// parseMidSide is (mid dB, side dB, ok): ok only when both channels printed a level.
func parseMidSide(lines []string) (float64, float64, bool) {
	levels, channel := map[int]float64{}, 0
	for _, l := range lines {
		if m := channelRe.FindStringSubmatch(l); m != nil {
			channel, _ = strconv.Atoi(m[1])
			continue
		}
		if m := rmsRe.FindStringSubmatch(l); m != nil && (channel == 1 || channel == 2) {
			if _, seen := levels[channel]; seen {
				continue
			}
			if m[1] == "-inf" {
				levels[channel] = math.Inf(-1)
			} else {
				levels[channel], _ = strconv.ParseFloat(m[1], 64)
			}
		}
	}
	mid, ok1 := levels[1]
	side, ok2 := levels[2]
	return mid, side, ok1 && ok2
}

// dualMono is whether two channels carry one signal: the side silent, or `below` dB or more
// under the mid (the pipeline's dual_mono).
func dualMono(mid, side, below float64) bool {
	if math.IsInf(side, -1) {
		return true
	}
	return math.Round((mid-side)*1e6)/1e6 >= below
}

// dbFigure is a level as JSON, which has no infinity.
func dbFigure(v float64) any {
	switch {
	case math.IsInf(v, -1):
		return "-inf"
	case math.IsInf(v, 1), math.IsNaN(v):
		return nil
	}
	return math.Round(v*100) / 100
}

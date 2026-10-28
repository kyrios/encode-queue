package worker

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// **The strip job** (v0.2.22; Thorsten, 2026-10-28, "Server strip + local bonus"; the spec was
// video-pipeline#29's): the pictures Funkhaus's timeline shows along a video, as Final Cut does,
// from a stage's h264.mp4. ffmpeg decodes only the keyframes (`-skip_frame nokey`), so each
// picture is one decode, and gives each its time, its scene score against the keyframe before and
// its mean colour. Two levels, Height px tall at the video's aspect, in JPEG sheets named by
// their content, so a page may keep them for good:
//   - `scenes`: at most Scenes keyframes where the picture changes most (scene score over 0.3, x264
//     puts a keyframe at a cut), at least 1/(2·Scenes) of the video apart, filled with the keyframe
//     nearest the middle of each even stretch that has none; one 8×4 sheet;
//   - `fine`: the keyframes at least FineEveryS apart, 10×10 to a sheet.
//
// thumbs.json names each level's pictures' times (showinfo's pts_time, the edit list applied)
// and sheets, and the colours of 128 even stretches, which a page draws before any picture. The
// result is an uncompressed tar of thumbs.json and the sheets, which the pipeline unpacks into
// the set.

// StripParams are a strip job's settings; zero is the default.
type StripParams struct {
	Height     int     `json:"height,omitempty"`       // the pictures' height, 90
	FineEveryS float64 `json:"fine_every_s,omitempty"` // the fine level's spacing, 4 s
	Scenes     int     `json:"scenes,omitempty"`       // the scenes level's most pictures, 32
	From       string  `json:"from,omitempty"`         // the source's name in thumbs.json, h264.mp4
}

const (
	stripIndex     = "thumbs.json"
	stripSceneCut  = 0.3
	stripColors    = 128
	stripQuality   = "5"
	stripSceneCols = 8
	stripSceneRows = 4
	stripFineCols  = 10
	stripFineRows  = 10
)

func (p StripParams) withDefaults() StripParams {
	if p.Height == 0 {
		p.Height = 90
	}
	if p.FineEveryS == 0 {
		p.FineEveryS = 4
	}
	if p.Scenes == 0 {
		p.Scenes = stripSceneCols * stripSceneRows
	}
	if p.From == "" {
		p.From = "h264.mp4"
	}
	return p
}

// StripKey is one keyframe of the first pass: its time, scene score and mean Y, U, V.
type StripKey struct {
	T     float64
	Score float64
	Mean  [3]int
}

// StripLevel is one level of thumbs.json.
type StripLevel struct {
	Name    string    `json:"name"`
	Every   float64   `json:"every"`
	Columns int       `json:"columns"`
	Rows    int       `json:"rows"`
	Times   []float64 `json:"times"`
	Sheets  []string  `json:"sheets"`
}

// StripIndex is thumbs.json.
type StripIndex struct {
	Schema   int          `json:"schema"`
	From     string       `json:"from"`
	Width    int          `json:"width"`
	Height   int          `json:"height"`
	Duration float64      `json:"duration"`
	Colors   string       `json:"colors"`
	Levels   []StripLevel `json:"levels"`
}

var stripSheetRe = regexp.MustCompile(`^thumbs-(scenes|fine)-[0-9a-f]{16}\.jpg$`)

func stripCmd(ffmpeg, src, graph, out string) []string {
	return []string{ffmpeg, "-hide_banner", "-nostdin", "-nostats", "-y", "-v", "error",
		"-protocol_whitelist", "file", "-skip_frame", "nokey", "-i", src, "-an", "-sn", "-dn",
		"-vf", graph, "-fps_mode", "passthrough", "-q:v", stripQuality, "-start_number", "0",
		"-f", "image2", out}
}

// stripArgs is the first pass: every keyframe's scene score (`metadata@scene`), time and mean
// colour (`showinfo@all`), and the fine level, keyframes at least FineEveryS apart
// (`showinfo@fine`), tiled row by row into numbered sheets.
func stripArgs(ffmpeg, src, dir string, p StripParams) []string {
	graph := fmt.Sprintf("select@scene='gte(scene,0)',metadata@scene=print:key=lavfi.scene_score,"+
		"scale=-2:%d,showinfo@all,select@fine='isnan(prev_selected_t)+gte(t-prev_selected_t,%s)',"+
		"showinfo@fine,tile=%dx%d", p.Height, strconv.FormatFloat(p.FineEveryS, 'f', -1, 64), stripFineCols, stripFineRows)
	return stripCmd(ffmpeg, src, graph, filepath.Join(dir, ".thumbs-fine-%03d.jpg"))
}

// scenesArgs is the second pass: the keyframes numbered picks into the scenes sheet.
func scenesArgs(ffmpeg, src, dir string, p StripParams, picks []int) []string {
	eqs := make([]string, len(picks))
	for i, k := range picks {
		eqs[i] = fmt.Sprintf("eq(n,%d)", k)
	}
	graph := fmt.Sprintf("select='%s',scale=-2:%d,tile=%dx%d", strings.Join(eqs, "+"), p.Height, stripSceneCols, stripSceneRows)
	return stripCmd(ffmpeg, src, graph, filepath.Join(dir, ".thumbs-scenes-%03d.jpg"))
}

var (
	showinfoN    = regexp.MustCompile(`\bn:\s*(\d+)\b`)
	showinfoTime = regexp.MustCompile(`\bpts_time:\s*(-?[0-9.]+)`)
	showinfoSize = regexp.MustCompile(`\bs:(\d+)x(\d+)`)
	showinfoMean = regexp.MustCompile(`\bmean:\[(\d+) (\d+) (\d+)\]`)
	sceneFrame   = regexp.MustCompile(`\bframe:\s*(\d+)`)
	sceneScore   = regexp.MustCompile(`lavfi\.scene_score=([0-9.]+)`)
	// logPrefix is a filter's prefix, which FFREPORT's file repeats inside a line that showinfo
	// writes in parts: `mean:[[showinfo@all @ 0x1] 76 84[showinfo@all @ 0x1]  255...`.
	logPrefix = regexp.MustCompile(`\[[a-z_0-9]+@[a-z_0-9]+ @ 0x[0-9a-f]+\] ?`)
)

// KeyframesFromLog reads the first pass's log: every keyframe, the fine level's times and the
// pictures' size. A keyframe's score comes on the line after its `frame:` line.
func KeyframesFromLog(r io.Reader) (keys []StripKey, fine []float64, w, h int) {
	scores := map[int]float64{}
	frame := -1
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if loc := logPrefix.FindStringIndex(line); loc != nil {
			line = line[:loc[1]] + logPrefix.ReplaceAllString(line[loc[1]:], "")
		}
		if strings.Contains(line, "metadata@scene") {
			if m := sceneFrame.FindStringSubmatch(line); m != nil {
				frame, _ = strconv.Atoi(m[1])
			} else if m := sceneScore.FindStringSubmatch(line); m != nil && frame >= 0 {
				scores[frame], _ = strconv.ParseFloat(m[1], 64)
			}
			continue
		}
		m := showinfoTime.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		t, _ := strconv.ParseFloat(m[1], 64)
		when := round3(max(0, t))
		switch {
		case strings.Contains(line, "showinfo@all"):
			k := StripKey{T: when, Mean: [3]int{16, 128, 128}}
			if n := showinfoN.FindStringSubmatch(line); n != nil {
				i, _ := strconv.Atoi(n[1])
				k.Score = scores[i]
			}
			if mm := showinfoMean.FindStringSubmatch(line); mm != nil {
				for i := range 3 {
					k.Mean[i], _ = strconv.Atoi(mm[i+1])
				}
			}
			if s := showinfoSize.FindStringSubmatch(line); s != nil && w == 0 {
				w, _ = strconv.Atoi(s[1])
				h, _ = strconv.Atoi(s[2])
			}
			keys = append(keys, k)
		case strings.Contains(line, "showinfo@fine"):
			fine = append(fine, when)
		}
	}
	return keys, fine, w, h
}

// ScenePicks is which keyframes the scenes level shows, by number: the first, which starts the
// first scene, and those with the highest scene score over 0.3, at least 1/(2·most) of the video
// apart; then, for each of most even stretches that holds none of them, the keyframe nearest its
// middle, while there is room. All of them when there are no more than most.
func ScenePicks(times, scores []float64, duration float64, most int) []int {
	if len(times) <= most {
		out := make([]int, len(times))
		for i := range out {
			out[i] = i
		}
		return out
	}
	sc := append([]float64{1}, scores[1:]...)
	gap := 0.0
	if duration > 0 {
		gap = duration / float64(2*most)
	}
	order := make([]int, len(times))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return sc[order[a]] > sc[order[b]] })
	var picks []int
	picked := map[int]bool{}
	for _, k := range order {
		if sc[k] <= stripSceneCut || len(picks) >= most {
			break
		}
		ok := true
		for _, j := range picks {
			if math.Abs(times[k]-times[j]) < gap {
				ok = false
				break
			}
		}
		if ok {
			picks, picked[k] = append(picks, k), true
		}
	}
	w := duration / float64(most)
	for i := 0; i < most && len(picks) < most; i++ {
		lo, hi := float64(i)*w, float64(i+1)*w
		mid := (float64(i) + 0.5) * w
		has := false
		for _, j := range picks {
			if lo <= times[j] && times[j] < hi {
				has = true
				break
			}
		}
		if has {
			continue
		}
		best := -1
		for k := range times {
			if !picked[k] && (best < 0 || math.Abs(times[k]-mid) < math.Abs(times[best]-mid)) {
				best = k
			}
		}
		if best >= 0 {
			picks, picked[best] = append(picks, best), true
		}
	}
	sort.Ints(picks)
	return picks
}

// YUVHex is a mean Y, U, V (BT.709, limited range, as h264.mp4 is) as `rrggbb`.
func YUVHex(m [3]int) string {
	y := float64(m[0]-16) * 255 / 219
	u := float64(m[1]-128) * 255 / 224
	v := float64(m[2]-128) * 255 / 224
	var b strings.Builder
	for _, c := range []float64{y + 1.5748*v, y - 0.1873*u - 0.4681*v, y + 1.8556*u} {
		fmt.Fprintf(&b, "%02x", int(max(0, min(255, roundEven(c)))))
	}
	return b.String()
}

// StretchColors is the placeholder: for each of count even stretches of the video, the mean
// colour of the keyframe shown there (the last one at or before its middle).
func StretchColors(keys []StripKey, duration float64, count int) string {
	var b strings.Builder
	for i := range count {
		mid := (float64(i) + 0.5) * duration / float64(count)
		k := keys[0]
		for _, key := range keys {
			if key.T <= mid {
				k = key
			}
		}
		b.WriteString(YUVHex(k.Mean))
	}
	return b.String()
}

// round3 rounds to milliseconds; roundEven rounds as Python's round() does, as video-pipeline#29.
func round3(x float64) float64    { return math.RoundToEven(x*1000) / 1000 }
func roundEven(x float64) float64 { return math.RoundToEven(x) }

// hashedSheets renames a level's numbered sheets by their content (`thumbs-fine-<sha256 16>.jpg`)
// and returns them in their order.
func hashedSheets(dir, level string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var parts []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".thumbs-"+level+"-") && strings.HasSuffix(e.Name(), ".jpg") {
			parts = append(parts, e.Name())
		}
	}
	sort.Strings(parts)
	var names []string
	for _, n := range parts {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		name := "thumbs-" + level + "-" + hex.EncodeToString(sum[:8]) + ".jpg"
		if err := os.Rename(filepath.Join(dir, n), filepath.Join(dir, name)); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// runStripPass runs one ffmpeg pass with its log in the file logName in the lease's directory
// (FFREPORT): showinfo writes two lines a keyframe, which would flood the lease's log.
func (r *run) runStripPass(ctx context.Context, args []string, logName string) (string, error) {
	keep := r.env
	r.env = append(append([]string(nil), keep...), "FFREPORT=file="+logName+":level=32")
	defer func() { r.env = keep }()
	if err := r.runTool(ctx, args, "ffmpeg", nil); err != nil {
		return "", err
	}
	return filepath.Join(r.dir, logName), nil
}

// workStrip runs a strip job: the two passes in a folder of the lease's directory, thumbs.json,
// and the tar of both into out.
func (r *run) workStrip(ctx context.Context, src, out string) ([]Tool, error) {
	var p StripParams
	if len(r.l.Params) > 0 {
		if err := json.Unmarshal(r.l.Params, &p); err != nil {
			return nil, fail("other", "params: %v", err)
		}
	}
	p = p.withDefaults()
	dir := filepath.Join(r.dir, "strip")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fail("other", "%v", err)
	}
	logPath, err := r.runStripPass(ctx, stripArgs(r.w.cfg.FFmpeg, src, dir, p), "strip-keyframes.log")
	if err != nil {
		return nil, err
	}
	f, err := os.Open(logPath)
	if err != nil {
		return nil, fail("decode_failed", "ffmpeg wrote no log: %v", err)
	}
	keys, fine, width, height := KeyframesFromLog(f)
	f.Close()
	duration := r.l.Job.Source.DurationS
	if duration <= 0 && len(keys) > 0 {
		duration = keys[len(keys)-1].T
	}
	if duration <= 0 && len(keys) > 0 {
		// Funkhaus refuses a thumbs.json without a length; one keyframe at 0 gives none.
		return nil, fail("decode_failed", "the video has no length: send source.duration_s")
	}
	fineSheets, err := hashedSheets(dir, "fine")
	if err != nil {
		return nil, fail("other", "%v", err)
	}
	per := stripFineCols * stripFineRows
	if len(keys) == 0 || len(fine) == 0 || width == 0 || len(fineSheets) != (len(fine)+per-1)/per {
		return nil, fail("decode_failed", "keyframes: %d keyframes, %d pictures, %d sheets", len(keys), len(fine), len(fineSheets))
	}
	times, scores := make([]float64, len(keys)), make([]float64, len(keys))
	for i, k := range keys {
		times[i], scores[i] = k.T, k.Score
	}
	picks := ScenePicks(times, scores, duration, p.Scenes)
	if _, err := r.runStripPass(ctx, scenesArgs(r.w.cfg.FFmpeg, src, dir, p, picks), "strip-scenes.log"); err != nil {
		return nil, err
	}
	sceneSheets, err := hashedSheets(dir, "scenes")
	if err != nil {
		return nil, fail("other", "%v", err)
	}
	if len(sceneSheets) != 1 {
		return nil, fail("decode_failed", "scenes: %d sheets", len(sceneSheets))
	}
	sceneTimes := make([]float64, len(picks))
	for i, k := range picks {
		sceneTimes[i] = times[k]
	}
	index := StripIndex{Schema: 1, From: p.From, Width: width, Height: height,
		Duration: round3(duration), Colors: StretchColors(keys, duration, stripColors),
		Levels: []StripLevel{
			{Name: "scenes", Every: round3(max(p.FineEveryS, duration/float64(p.Scenes))), Columns: stripSceneCols,
				Rows: stripSceneRows, Times: sceneTimes, Sheets: sceneSheets},
			{Name: "fine", Every: p.FineEveryS, Columns: stripFineCols, Rows: stripFineRows, Times: fine, Sheets: fineSheets},
		}}
	js, err := json.Marshal(index)
	if err != nil {
		return nil, fail("other", "%v", err)
	}
	if err := writeStripTar(out, append(js, '\n'), dir, append(sceneSheets, fineSheets...)); err != nil {
		return nil, fail("other", "the strip's tar: %v", err)
	}
	r.log("info", "worker", fmt.Sprintf("strip: %d keyframes, %d scenes, %d fine pictures, %d sheets",
		len(keys), len(picks), len(fine), 1+len(fineSheets)))
	return r.w.toolsNamed("ffmpeg"), nil
}

// writeStripTar is thumbs.json and the sheets, in that order, as an uncompressed tar with plain
// names, mode 0644 and no times, so the same strip makes the same bytes.
func writeStripTar(out string, index []byte, dir string, sheets []string) error {
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(f)
	add := func(name string, b []byte) error {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(b)), Format: tar.FormatUSTAR}); err != nil {
			return err
		}
		_, err := tw.Write(b)
		return err
	}
	err = add(stripIndex, index)
	for _, n := range sheets {
		if err != nil {
			break
		}
		var b []byte
		if b, err = os.ReadFile(filepath.Join(dir, n)); err == nil {
			err = add(n, b)
		}
	}
	if err == nil {
		err = tw.Close()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

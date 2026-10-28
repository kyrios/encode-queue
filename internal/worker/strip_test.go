package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The first pass's log as ffmpeg 9.0.2 writes it (video-pipeline#29's fixture).
const keyframeLog = `[metadata@scene @ 0x7b1012100] frame:0    pts:0       pts_time:0
[metadata@scene @ 0x7b1012100] lavfi.scene_score=0.000000
[showinfo@all @ 0x7b1012640] config in time_base: 1/15360, frame_rate: 30/1
[showinfo@all @ 0x7b1012640] n:   0 pts:      0 pts_time:0       duration:    512 duration_time:0.0333333 fmt:yuv420p cl:unspecified sar:1/1 s:180x90 i:P iskey:1 type:I checksum:C9441A9D plane_checksum:[B29B458A 3417D991 5D84FB64] mean:[54 127 129] stdev:[56.0 3.6 3.6]
[showinfo@fine @ 0x7b10127c0] n:   0 pts:      0 pts_time:0       duration:    512 duration_time:0.0333333 fmt:yuv420p cl:unspecified sar:1/1 s:180x90 i:P iskey:1 type:I checksum:C9441A9D plane_checksum:[B29B458A 3417D991 5D84FB64] mean:[54 127 129] stdev:[56.0 3.6 3.6]
[metadata@scene @ 0x7b1012100] frame:1    pts:128000  pts_time:8.333333
[metadata@scene @ 0x7b1012100] lavfi.scene_score=0.784913
[showinfo@all @ 0x7b1012640] n:   1 pts: 128000 pts_time:8.33333 duration:    512 duration_time:0.0333333 fmt:yuv420p cl:unspecified sar:1/1 s:180x90 i:P iskey:1 type:I checksum:5D84FB64 plane_checksum:[B29B458A 3417D991 5D84FB64] mean:[180 120 140] stdev:[50.0 3.6 3.6]
[showinfo@all @ 0x7b1012640]   color_range:unknown color_space:unknown
`

func TestKeyframesFromLog(t *testing.T) {
	keys, fine, w, h := KeyframesFromLog(strings.NewReader(keyframeLog))
	want := []StripKey{{T: 0, Score: 0, Mean: [3]int{54, 127, 129}}, {T: 8.333, Score: 0.784913, Mean: [3]int{180, 120, 140}}}
	if len(keys) != 2 || keys[0] != want[0] || keys[1] != want[1] {
		t.Errorf("keys %+v", keys)
	}
	if len(fine) != 1 || fine[0] != 0 || w != 180 || h != 90 {
		t.Errorf("fine %v, %dx%d", fine, w, h)
	}
	// FFREPORT's file, where showinfo's parts each carry the prefix.
	split := "[showinfo@all @ 0xb6a82d800] n:   3 pts:  76800 pts_time:6       duration:    512 s:160x90 iskey:1 type:I " +
		"mean:[[showinfo@all @ 0xb6a82d800] 76 84[showinfo@all @ 0xb6a82d800]  255[showinfo@all @ 0xb6a82d800] ] stdev:[0.0\n"
	if keys, _, _, _ := KeyframesFromLog(strings.NewReader(split)); len(keys) != 1 || keys[0].Mean != [3]int{76, 84, 255} {
		t.Errorf("a split line: %+v", keys)
	}
}

func TestScenePicks(t *testing.T) {
	// Few keyframes: all of them.
	if got := ScenePicks([]float64{0, 2, 4}, []float64{0, 0, 0}, 6, 32); fmt.Sprint(got) != "[0 1 2]" {
		t.Errorf("few: %v", got)
	}
	// 100 keyframes a second apart, cuts at 10, 11 (too close to 10), 50 and 90: the cuts first,
	// then the middle of each of the 8 stretches without one.
	times, scores := make([]float64, 100), make([]float64, 100)
	for i := range times {
		times[i] = float64(i)
	}
	scores[10], scores[11], scores[50], scores[90] = 0.9, 0.8, 0.7, 0.5
	scores[30] = 0.2 // under the cut
	got := ScenePicks(times, scores, 100, 8)
	if fmt.Sprint(got) != "[0 10 19 31 44 50 69 90]" {
		t.Errorf("cuts and fill: %v", got)
	}
}

func TestYUVHex(t *testing.T) {
	for mean, want := range map[[3]int]string{{16, 128, 128}: "000000", {235, 128, 128}: "ffffff", {63, 102, 240}: "ff0000"} {
		if got := YUVHex(mean); !closeColour(got, want) {
			t.Errorf("YUVHex(%v) = %s, want %s", mean, got, want)
		}
	}
}

// TestRealStrip: a strip of a real H.264 file of six one-colour shots, ten seconds each, with a
// keyframe every two seconds and one at each cut: the scenes level holds the cuts, each stretch
// has its shot's colour, the fine level is the keyframes four seconds apart, and the tar holds
// thumbs.json and its sheets, named by their content.
func TestRealStrip(t *testing.T) {
	r, ff, _ := realRun(t)
	shots := []string{"black", "white", "blue", "yellow", "red", "white"} // each far from the last in luma
	var inputs []string
	var concat strings.Builder
	for i, c := range shots {
		inputs = append(inputs, "-f", "lavfi", "-i", "color=c="+c+":size=320x180:rate=25:duration=10")
		fmt.Fprintf(&concat, "[%d:v]", i)
	}
	src := filepath.Join(r.w.cfg.WorkDir, "h264.mp4")
	args := append([]string{"-hide_banner", "-v", "error"}, inputs...)
	args = append(args, "-filter_complex", concat.String()+fmt.Sprintf("concat=n=%d:v=1,scale=out_color_matrix=bt709:out_range=tv,format=yuv420p", len(shots)),
		"-colorspace", "bt709", "-color_range", "tv",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "50", "-pix_fmt", "yuv420p", src)
	if b, err := exec.Command(ff, args...).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	r.l.Job.Source.DurationS = 60
	// A finer spacing and fewer scenes than the defaults, so 60 s have more keyframes than
	// scenes and the picking matters.
	r.l.Params = json.RawMessage(`{"fine_every_s": 4, "scenes": 12}`)
	out := filepath.Join(r.w.cfg.WorkDir, "strip.tar")
	if _, err := r.workStrip(context.Background(), src, out); err != nil {
		t.Fatal(err)
	}
	files := readTar(t, out)
	if files[0].name != "thumbs.json" {
		t.Fatalf("the tar starts with %s", files[0].name)
	}
	var idx StripIndex
	if err := json.Unmarshal(files[0].body, &idx); err != nil {
		t.Fatal(err)
	}
	if idx.Schema != 1 || idx.From != "h264.mp4" || idx.Height != 90 || idx.Width != 160 || idx.Duration != 60 || len(idx.Levels) != 2 {
		t.Fatalf("index %+v", idx)
	}
	scenes, fine := idx.Levels[0], idx.Levels[1]
	if scenes.Name != "scenes" || len(scenes.Times) != 12 || len(scenes.Sheets) != 1 {
		t.Errorf("scenes %+v", scenes)
	}
	for _, cut := range []float64{0, 10, 20, 30, 40, 50} {
		if !near(scenes.Times, cut, 0.05) {
			t.Errorf("no scene at the cut at %v s: %v", cut, scenes.Times)
		}
	}
	if fine.Name != "fine" || fine.Every != 4 || len(fine.Times) != 15 || len(fine.Sheets) != 1 {
		t.Errorf("fine %+v", fine)
	}
	for i := 1; i < len(fine.Times); i++ {
		if fine.Times[i]-fine.Times[i-1] < 4-1e-9 {
			t.Errorf("fine pictures %v apart", fine.Times[i]-fine.Times[i-1])
		}
	}
	wantColour := []string{"000000", "ffffff", "0000ff", "ffff00", "ff0000", "ffffff"}
	for i := range stripColors {
		got := idx.Colors[6*i : 6*i+6]
		shot := int((float64(i) + 0.5) * 60 / stripColors / 10)
		if !closeColour(got, wantColour[shot]) {
			t.Errorf("stretch %d: %s, the shot's %s", i, got, wantColour[shot])
		}
	}
	names := map[string]bool{}
	for _, f := range files[1:] {
		names[f.name] = true
		if !stripSheetRe.MatchString(f.name) || !bytes.HasPrefix(f.body, []byte{0xff, 0xd8, 0xff}) {
			t.Errorf("sheet %s: % x", f.name, f.body[:4])
		}
	}
	for _, n := range append(scenes.Sheets, fine.Sheets...) {
		if !names[n] {
			t.Errorf("%s is not in the tar", n)
		}
	}
	if len(files) != 1+len(scenes.Sheets)+len(fine.Sheets) {
		t.Errorf("%d files in the tar", len(files))
	}
	for _, l := range r.tail {
		if strings.Contains(l, "showinfo") {
			t.Fatalf("showinfo's lines reached the lease's log: %s", l)
		}
	}
}

type tarFile struct {
	name string
	body []byte
}

func readTar(t *testing.T, path string) []tarFile {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []tarFile
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(tr)
		out = append(out, tarFile{h.Name, b})
	}
}

func near(xs []float64, x, tol float64) bool {
	for _, v := range xs {
		if math.Abs(v-x) <= tol {
			return true
		}
	}
	return false
}

// closeColour: each channel within 24 of the other's.
func closeColour(a, b string) bool {
	var x, y [3]int
	fmt.Sscanf(a, "%02x%02x%02x", &x[0], &x[1], &x[2])
	fmt.Sscanf(b, "%02x%02x%02x", &y[0], &y[1], &y[2])
	for i := range 3 {
		if d := x[i] - y[i]; d > 24 || d < -24 {
			return false
		}
	}
	return true
}

// TestStripAsVideoPipeline29: with EQ_STRIP_COMPARE naming a folder with h264.mp4 and the
// thumbs.json video-pipeline#29 made of it (and its sheets), the worker's strip is the same:
// the times, the sheets by name (so by content), and the colours.
func TestStripAsVideoPipeline29(t *testing.T) {
	dir := os.Getenv("EQ_STRIP_COMPARE")
	if dir == "" {
		t.Skip("EQ_STRIP_COMPARE names no folder")
	}
	r, _, _ := realRun(t)
	var want StripIndex
	b, err := os.ReadFile(filepath.Join(dir, "thumbs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	// The pipeline sends the upload's duration, as video-pipeline#29 took it.
	r.l.Job.Source.DurationS = want.Duration
	out := filepath.Join(r.w.cfg.WorkDir, "strip.tar")
	if _, err := r.workStrip(context.Background(), filepath.Join(dir, "h264.mp4"), out); err != nil {
		t.Fatal(err)
	}
	var got StripIndex
	if err := json.Unmarshal(readTar(t, out)[0].body, &got); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d scenes, %d fine pictures in %d sheets", len(got.Levels[0].Times), len(got.Levels[1].Times), len(got.Levels[1].Sheets))
	if got.Width != want.Width || got.Height != want.Height || got.Duration != want.Duration || got.From != want.From {
		t.Errorf("%s %dx%d %v s, video-pipeline#29's %s %dx%d %v s", got.From, got.Width, got.Height, got.Duration,
			want.From, want.Width, want.Height, want.Duration)
	}
	if got.Colors != want.Colors {
		t.Errorf("other colours, from character %d", firstDiff([]byte(got.Colors), []byte(want.Colors)))
	}
	for i, lv := range got.Levels {
		w := want.Levels[i]
		if lv.Name != w.Name || lv.Every != w.Every || lv.Columns != w.Columns || lv.Rows != w.Rows {
			t.Errorf("level %s every %v %dx%d, video-pipeline#29's %s every %v %dx%d", lv.Name, lv.Every, lv.Columns, lv.Rows,
				w.Name, w.Every, w.Columns, w.Rows)
		}
		if fmt.Sprint(lv.Times) != fmt.Sprint(w.Times) {
			t.Errorf("%s: %d times, video-pipeline#29's %d; first %v / %v", lv.Name, len(lv.Times), len(w.Times),
				lv.Times[:min(5, len(lv.Times))], w.Times[:min(5, len(w.Times))])
		}
		if fmt.Sprint(lv.Sheets) != fmt.Sprint(w.Sheets) {
			t.Errorf("%s: sheets %v, video-pipeline#29's %v", lv.Name, lv.Sheets, w.Sheets)
		}
	}
}

// TestStripWithoutALength: a video of one keyframe and no duration_s has no length, which
// Funkhaus would refuse; the job fails instead.
func TestStripWithoutALength(t *testing.T) {
	r, ff, _ := realRun(t)
	src := filepath.Join(r.w.cfg.WorkDir, "one.mp4")
	if b, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "color=c=red:size=160x90:rate=25:duration=1",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "250", "-pix_fmt", "yuv420p", src).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	_, err := r.workStrip(context.Background(), src, filepath.Join(r.w.cfg.WorkDir, "one.tar"))
	if f, ok := err.(*failure); !ok || f.code != "decode_failed" || !strings.Contains(f.msg, "no length") {
		t.Fatalf("%v", err)
	}
	r.l.Job.Source.DurationS = 1
	if _, err := r.workStrip(context.Background(), src, filepath.Join(r.w.cfg.WorkDir, "one.tar")); err != nil {
		t.Fatalf("with duration_s: %v", err)
	}
}

// TestStripOf10BitHEVC: an iPhone's upload is 10-bit HEVC; its colours are still 8-bit means.
// Grey, since a 10-bit white read as 8-bit clips to white all the same.
func TestStripOf10BitHEVC(t *testing.T) {
	r, ff, _ := realRun(t)
	src := filepath.Join(r.w.cfg.WorkDir, "hevc10.mp4")
	if b, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "color=c=gray:size=640x360:rate=25:duration=4",
		"-vf", "scale=out_color_matrix=bt709:out_range=tv,format=yuv420p10le", "-c:v", "libx265",
		"-x265-params", "log-level=error:keyint=50", src).CombinedOutput(); err != nil {
		t.Skipf("no 10-bit libx265: %v %s", err, b)
	}
	r.l.Job.Source.DurationS = 4
	out := filepath.Join(r.w.cfg.WorkDir, "hevc10.tar")
	if _, err := r.workStrip(context.Background(), src, out); err != nil {
		t.Fatal(err)
	}
	var idx StripIndex
	if err := json.Unmarshal(readTar(t, out)[0].body, &idx); err != nil {
		t.Fatal(err)
	}
	if !closeColour(idx.Colors[:6], "808080") || idx.Width != 160 || idx.Height != 90 {
		t.Errorf("grey 10-bit: %s, %dx%d", idx.Colors[:6], idx.Width, idx.Height)
	}
}

// TestStripArgs: keyframes only, slice threads (never frame threads, which hold a 4K picture
// each), and the pictures 8-bit before showinfo.
func TestStripArgs(t *testing.T) {
	p := StripParams{}.withDefaults()
	for _, a := range [][]string{stripArgs("ffmpeg", "/w/up", "/w", p, 3), scenesArgs("ffmpeg", "/w/up", "/w", p, []int{0, 7}, 3)} {
		line := strings.Join(a, " ")
		for _, want := range []string{"-skip_frame nokey -threads 3 -thread_type slice -i /w/up", "scale=-2:90,format=yuv420p,"} {
			if !strings.Contains(line, want) {
				t.Errorf("%q not in %s", want, line)
			}
		}
	}
}

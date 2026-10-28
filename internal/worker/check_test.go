package worker

import (
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// testdata/pipeline-v028-args.json holds the commands video-pipeline ran itself before v0.2.8
// (cut_args, score_args, mid_side_args and its framemd5, run in Python), with the params the
// pipeline sends for each now. The worker must run the same commands, so its cut, its scores
// and its levels are the pipeline's.
type golden struct {
	Kind   string          `json:"kind"`
	Name   string          `json:"name"`
	Params json.RawMessage `json:"params"`
	Args   []string        `json:"args"`
	Every  int             `json:"every"`
	Source [2]int          `json:"source"`
	Size   [2]int          `json:"size"`
	Audio  bool            `json:"audio"`
}

func goldens(t *testing.T) []golden {
	data, err := os.ReadFile("testdata/pipeline-v028-args.json")
	if err != nil {
		t.Fatal(err)
	}
	var g []golden
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatal(err)
	}
	return g
}

// withoutProgress drops `-progress pipe:1`, which the worker adds for its progress reports.
func withoutProgress(a []string) []string {
	var out []string
	for i := 0; i < len(a); i++ {
		if a[i] == "-progress" && i+1 < len(a) && a[i+1] == "pipe:1" {
			i++
			continue
		}
		out = append(out, a[i])
	}
	return out
}

var labelRe = regexp.MustCompile(`\[([a-z][a-z0-9]*)\]`)

// canonical names a filter graph's labels by their first appearance, so two graphs that differ
// only in label names compare equal.
func canonical(a []string) []string {
	names := map[string]string{}
	out := append([]string(nil), a...)
	for i, s := range out {
		out[i] = labelRe.ReplaceAllStringFunc(s, func(m string) string {
			k := m[1 : len(m)-1]
			if _, ok := names[k]; !ok {
				names[k] = "L" + string(rune('a'+len(names)))
			}
			return "[" + names[k] + "]"
		})
	}
	return out
}

func TestCommandsMatchThePipeline(t *testing.T) {
	for _, g := range goldens(t) {
		var got []string
		switch g.Kind {
		case "cut":
			var p CutParams
			if err := json.Unmarshal(g.Params, &p); err != nil {
				t.Fatal(err)
			}
			var err error
			if got, err = cutArgs("ffmpeg", p, "SRC", "OUT"); err != nil {
				t.Fatalf("%s: %v", g.Name, err)
			}
			got = withoutProgress(got)
		case "compare":
			var err error
			got, err = compareArgs("ffmpeg", CompareSettings{Every: g.Every}, "OUT", "SRC",
				g.Size[0], g.Size[1], g.Source[0], g.Source[1], "")
			if err != nil {
				t.Fatalf("%s: %v", g.Name, err)
			}
			got, g.Args = canonical(withoutProgress(got)), canonical(g.Args)
		case "midside":
			got = midSideArgs("ffmpeg", "SRC")
		case "framemd5":
			got = framemd5Args("ffmpeg", "SRC", g.Audio)
			// The worker maps the sound when there is one; the pipeline knew whether there was.
			for i := range got {
				got[i] = strings.TrimSuffix(got[i], "?")
			}
		default:
			t.Fatalf("kind %q", g.Kind)
		}
		if !reflect.DeepEqual(got, g.Args) {
			t.Errorf("%s:\n got %q\nwant %q", g.Name, got, g.Args)
		}
	}
}

func TestRemuxArgs(t *testing.T) {
	got := remuxArgs("ffmpeg", RemuxParams{HVC1Tag: true}, "SRC", "OUT")
	want := []string{"ffmpeg", "-hide_banner", "-nostdin", "-y", "-loglevel", "error", "-nostats",
		"-progress", "pipe:1", "-i", "SRC", "-map", "0:v:0", "-map", "0:a:0?", "-c", "copy",
		"-tag:v", "hvc1", "-movflags", "+faststart", "OUT"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q", got)
	}
}

func TestCutArgsRefuse(t *testing.T) {
	for _, p := range []CutParams{
		{StartS: 1, LengthS: 0, Preset: "fast", BitDepth: 8},
		{StartS: 1, LengthS: 2, FadeS: 3, Preset: "fast", BitDepth: 8},
		{StartS: 1, LengthS: 2, Preset: "fast", BitDepth: 12},
		{StartS: 1, LengthS: 2, Preset: "fast; rm", BitDepth: 8},
	} {
		if _, err := cutArgs("ffmpeg", p, "SRC", "OUT"); err == nil {
			t.Errorf("%+v: no error", p)
		}
	}
	p := CutParams{LengthS: 2, Preset: "fast", BitDepth: 8}
	p.Colour.Primaries = "bt709,drawtext"
	if _, err := cutArgs("ffmpeg", p, "SRC", "OUT"); err == nil {
		t.Error("a colour with a comma went through")
	}
}

func TestCompareWithVMAFOnly(t *testing.T) {
	c := CompareSettings{Every: 1, Metrics: []string{}}
	c.VMAF = &struct {
		Model string `json:"model"`
	}{Model: "vmaf_v0.6.1"}
	got, err := compareArgs("ffmpeg", c, "OUT", "SRC", 1280, 720, 1920, 1080, "")
	if err != nil {
		t.Fatal(err)
	}
	g := got[len(got)-6]
	want := "[0:v]select='not(mod(n\\,1))',setpts=N/TB,fps=1,format=yuv420p10le[d0];" +
		"[1:v]select='not(mod(n\\,1))',setpts=N/TB,fps=1,scale=1280:720:flags=lanczos,format=yuv420p10le,split=1[r1];" +
		"[d0]format=yuv420p[e0];[r1]format=yuv420p[q1];[e0][q1]libvmaf=model=version=vmaf_v0.6.1:shortest=1[e2]"
	if g != want {
		t.Errorf("graph\n got %s\nwant %s", g, want)
	}
	c.VMAF.Model = "../x"
	if _, err := compareArgs("ffmpeg", c, "OUT", "SRC", 1, 1, 1, 1, ""); err == nil {
		t.Error("a model name with a path went through")
	}
	if _, err := compareArgs("ffmpeg", CompareSettings{Metrics: []string{}}, "OUT", "SRC", 1, 1, 1, 1, ""); err == nil {
		t.Error("nothing to compare went through")
	}
}

func TestParseScores(t *testing.T) {
	text := "[Parsed_ssim_4 @ 0x1] SSIM Y:0.987654 (19.1) U:0.99 V:0.99 All:0.98 (18.2)\n" +
		"[Parsed_psnr_5 @ 0x2] PSNR y:inf u:inf v:inf average:inf min:inf max:inf\n" +
		"[Parsed_xpsnr_9 @ 0x3] XPSNR  y: 41.0712  u: 44.1  v: 44.2  (minimum: 41.0712)\n" +
		"[Parsed_libvmaf_10 @ 0x4] VMAF score: 95.123456\n"
	s, p, x, v := parseScores(text)
	if s != 0.9877 || p != "inf" || x != 41.0712 || v != 95.1235 {
		t.Errorf("%v %v %v %v", s, p, x, v)
	}
	if s, p, x, v := parseScores("nothing"); s != nil || p != nil || x != nil || v != nil {
		t.Errorf("figures from nothing: %v %v %v %v", s, p, x, v)
	}
}

func TestFramemd5Seconds(t *testing.T) {
	// The same packets in a 1/600 and a 1/19200 time base (an Apple file and its remux).
	a := "#tb 0: 1/600\n#tb 1: 1/48000\n0, -20, 0, 20, 1000, aaa\n1, -1024, -1024, 1024, 20, bbb, S=1, 10, ccc\n"
	b := "#tb 0: 1/19200\n#tb 1: 1/48000\n0, -640, 0, 640, 1000, aaa\n1, -1024, -1024, 1024, 20, bbb, S=1, 10, ccc\n"
	pa, err := parseFramemd5(a)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := parseFramemd5(b)
	if err != nil {
		t.Fatal(err)
	}
	if d := comparePackets(pa, pb); d != nil {
		t.Errorf("equal packets differ: %s", *d)
	}
	c := strings.Replace(b, "0, -640, 0, 640, 1000, aaa", "0, -640, 0, 640, 1001, aaa", 1)
	pc, _ := parseFramemd5(c)
	if d := comparePackets(pa, pc); d == nil || !strings.Contains(*d, "size 1001 against 1000") {
		t.Errorf("a size difference: %v", d)
	}
	if d := comparePackets(pa, pa[:1]); d == nil || !strings.Contains(*d, "1 packets against the source's 2") {
		t.Errorf("a missing packet: %v", d)
	}
}

func TestMidSide(t *testing.T) {
	lines := []string{"[Parsed_astats_1 @ 0x1] Channel: 1", "[Parsed_astats_1 @ 0x1] RMS level dB: -23.10",
		"[Parsed_astats_1 @ 0x1] Channel: 2", "[Parsed_astats_1 @ 0x1] RMS level dB: -inf"}
	mid, side, ok := parseMidSide(lines)
	if !ok || mid != -23.1 || !math.IsInf(side, -1) || !dualMono(mid, side, 40) {
		t.Errorf("%v %v %v", mid, side, ok)
	}
	if dbFigure(side) != "-inf" || dbFigure(mid) != -23.1 {
		t.Errorf("%v %v", dbFigure(side), dbFigure(mid))
	}
	if dualMono(-20, -59.9, 40) || !dualMono(-20, -60, 40) {
		t.Error("the 40 dB line")
	}
	if _, _, ok := parseMidSide(lines[:2]); ok {
		t.Error("one channel read as two")
	}
}

func TestMonoJSON(t *testing.T) {
	for in, want := range map[string]Mono{`true`: {On: true}, `false`: {}, `"measure"`: {Measure: true}} {
		var a AudioParams
		if err := json.Unmarshal([]byte(`{"mode":"aac","mono":`+in+`}`), &a); err != nil || a.Mono != want {
			t.Errorf("%s: %+v %v", in, a.Mono, err)
		}
		b, _ := json.Marshal(a.Mono)
		if string(b) != in {
			t.Errorf("%s back as %s", in, b)
		}
	}
	var a AudioParams
	if err := json.Unmarshal([]byte(`{"mono":"yes"}`), &a); err == nil {
		t.Error(`mono "yes" went through`)
	}
}

// checkFFmpeg is this machine's ffmpeg when it has what the check runs (the toolchain's does;
// XPSNR came with ffmpeg 7), else the test is skipped.
func checkFFmpeg(t *testing.T) string {
	skip := t.Skipf
	if os.Getenv("EQ_REQUIRE_REAL_FFMPEG") == "1" { // CI's job in the toolchain image
		skip = t.Fatalf
	}
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		skip("no ffmpeg on PATH")
	}
	out, _ := exec.Command(ff, "-hide_banner", "-filters").Output()
	for _, f := range []string{"xpsnr", "ssim", "psnr", "astats"} {
		if !regexp.MustCompile(`\s` + f + `\s`).Match(out) {
			skip("ffmpeg without the %s filter", f)
		}
	}
	return ff
}

// With this machine's ffmpeg: the commands run and their output parses. A file against
// itself scores PSNR inf; a remux matches its source packet for packet.
func TestCheckToolsReal(t *testing.T) {
	ff := checkFFmpeg(t)
	var err error
	dir := t.TempDir()
	src := filepath.Join(dir, "src.mp4")
	if out, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2", "-ac", "2", "-c:v", "libx264", "-preset", "ultrafast",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", src).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	args, err := compareArgs(ff, CompareSettings{Every: 5}, src, src, 320, 180, 320, 180, "")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("%v %s", err, out)
	}
	s, p, x, _ := parseScores(string(out))
	if s != 1.0 || p != "inf" || x != "inf" {
		t.Errorf("a file against itself: ssim %v psnr %v xpsnr %v", s, p, x)
	}
	if n := strings.Count(string(out), "progress=end"); n != 1 {
		t.Errorf("progress blocks end %d times", n)
	}
	rem := filepath.Join(dir, "remux.mp4")
	ra := remuxArgs(ff, RemuxParams{}, src, rem)
	if out, err := exec.Command(ra[0], ra[1:]...).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	var lists [][]packet
	for _, f := range []string{src, rem} {
		a := framemd5Args(ff, f, true)
		out, err := exec.Command(a[0], a[1:]...).Output()
		if err != nil {
			t.Fatal(err)
		}
		p, err := parseFramemd5(string(out))
		if err != nil {
			t.Fatal(err)
		}
		lists = append(lists, p)
	}
	if d := comparePackets(lists[0], lists[1]); d != nil || len(lists[0]) < 50 {
		t.Errorf("the remux: %v, %d packets", d, len(lists[0]))
	}
	ms := midSideArgs(ff, src)
	mo, err := exec.Command(ms[0], ms[1:]...).CombinedOutput()
	if err != nil {
		t.Fatalf("%v %s", err, mo)
	}
	mid, side, ok := parseMidSide(strings.Split(string(mo), "\n"))
	if !ok || !dualMono(mid, side, 40) {
		t.Errorf("the same tone on both channels: mid %v side %v ok %v", mid, side, ok)
	}
}

// A rung at another rate (v0.2.13): the comparison takes the source through the same fps filter,
// so the 60 fps result's frame n meets the source's picture of the same moment; the result's
// side is as before.
func TestCompareAtTheRungsRate(t *testing.T) {
	plain, _ := compareArgs("ffmpeg", CompareSettings{Every: 5}, "OUT", "SRC", 1920, 1080, 3840, 2160, "")
	got, err := compareArgs("ffmpeg", CompareSettings{Every: 5}, "OUT", "SRC", 1920, 1080, 3840, 2160, "60000/1001")
	if err != nil {
		t.Fatal(err)
	}
	g := got[len(got)-6]
	want := "[0:v]select='not(mod(n\\,5))',setpts=N/TB,fps=1,format=yuv420p10le[d0];" +
		"[1:v]fps=60000/1001,select='not(mod(n\\,5))',setpts=N/TB,fps=1,scale=1920:1080:flags=lanczos,format=yuv420p10le,split=3"
	if !strings.HasPrefix(g, want) {
		t.Fatalf("the graph:\n got  %s\n want %s…", g, want)
	}
	if strings.Replace(g, "fps=60000/1001,", "", 1) != plain[len(plain)-6] {
		t.Fatalf("more than the source's rate changed:\n %s\n %s", g, plain[len(plain)-6])
	}
	if _, err := compareArgs("ffmpeg", CompareSettings{Every: 5}, "OUT", "SRC", 1, 1, 1, 1, "60,scale=2:2"); err == nil {
		t.Fatal("took a rate that is a filter")
	}
}

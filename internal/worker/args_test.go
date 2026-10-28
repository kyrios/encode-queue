package worker

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// testdata/video-encode-args.json holds the arguments infra's roles/encoder/files/video-encode
// builds for these rungs (encode_args, variant_args, speech_args, run in Python), with the
// RungParams the pipeline sends for each. The worker must build exactly the same, so its file
// is the one `encoder` made before the split (ADR-0007, test 6).
func TestArgsMatchVideoEncode(t *testing.T) {
	data, err := os.ReadFile("testdata/video-encode-args.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name   string          `json:"name"`
		Args   []string        `json:"args"`
		Params json.RawMessage `json:"params"`
		Source struct{ Width, Height int }
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		var got []string
		if string(c.Params) == "null" {
			got = speechArgs("ffmpeg", "SRC", "WAV")
		} else {
			var p RungParams
			if err := json.Unmarshal(c.Params, &p); err != nil {
				t.Fatal(err)
			}
			got, err = rungArgs("ffmpeg", p, "SRC", c.Source.Width, c.Source.Height, "/work/input-captions", "OUT", 0)
			if err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
		}
		if strings.Join(got, " ") != strings.Join(c.Args, " ") {
			t.Errorf("%s:\n got  %s\n want %s", c.Name, strings.Join(got, " "), strings.Join(c.Args, " "))
		}
	}
}

func TestThreadsGoToX265Pools(t *testing.T) {
	p := RungParams{Codec: "libx265", Preset: "slower", CRF: 29, Width: 1920, Height: 1080}
	a, err := rungArgs("ffmpeg", p, "SRC", 1920, 1080, "", "OUT", 6)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(a, " "), "-x265-params log-level=error:pools=6 ") {
		t.Fatalf("%v", a)
	}
}

func TestArgsRefuseWhatIsNoSetting(t *testing.T) {
	for _, p := range []RungParams{
		{Codec: "libx265", Preset: "slower; rm -rf /", CRF: 29, Width: 1920, Height: 1080},
		{Codec: "libvpx", Preset: "slower", CRF: 29, Width: 1920, Height: 1080},
		{Codec: "libx265", Preset: "slower", CRF: 29, Width: 1921, Height: 1080},
	} {
		if _, err := rungArgs("ffmpeg", p, "SRC", 0, 0, "", "OUT", 0); err == nil {
			t.Errorf("accepted %+v", p)
		}
	}
}

// v0.2.13: a clip's rung as video-pipeline v0.2.10 sends it (the 9:16 window of a 3840x1920
// master, captions burnt in), with the frame rate, the AAC's rate and channels, and no edit lists.
const clipParams = `{"rung":"mastodon","codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,` +
	`"crop":{"width":1080,"height":1920,"x":1380,"y":0},"vbv_maxrate_kbps":3992,"vbv_bufsize_kbit":7984,` +
	`"audio":{"mode":"aac","bitrate_kbps":128,"sample_rate":48000,"channels":2},` +
	`"captions":{"input":"captions","font":"Inter ExtraBold"},"check":{"decode":true,"frames":true},` +
	`"fps":60,"edit_list":false}`

func TestClipArgs(t *testing.T) {
	var p RungParams
	if err := json.Unmarshal([]byte(clipParams), &p); err != nil {
		t.Fatal(err)
	}
	a, err := rungArgs("ffmpeg", p, "SRC", 3840, 1920, "/work/input-captions", "OUT", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := "ffmpeg -hide_banner -nostdin -y -loglevel error -nostats -progress pipe:1 -i SRC -map 0:v:0 -map 0:a:0 " +
		"-fps_mode passthrough -vf crop=1080:1920:1380:0,fps=60,format=yuv420p,ass=filename=/work/input-captions " +
		"-c:v libx264 -preset slow -crf 23 -maxrate 3992k -bufsize 7984k -profile:v high -pix_fmt yuv420p " +
		"-c:a aac -b:a 128k -ar 48000 -ac 2 -map_metadata -1 -map_chapters -1 -use_editlist 0 -movflags +faststart OUT"
	if got := strings.Join(a, " "); got != want {
		t.Fatalf("the clip:\n got  %s\n want %s", got, want)
	}
}

// Each of v0.2.13's fields, with and without: absent, the arguments are v0.2.12's.
func TestClipFieldsWithAndWithout(t *testing.T) {
	for _, c := range []struct {
		name, params string
		w, h         int
		has, hasNot  []string
	}{
		{"none", `{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128}}`, 1080, 1920,
			[]string{"-vf format=yuv420p -c:v", "-b:a 128k -map_metadata -1 -map_chapters -1 -movflags +faststart OUT"},
			[]string{"fps=", "-ar", "-ac", "-use_editlist"}},
		{"fps before the scale", `{"codec":"libx265","preset":"slower","crf":29,"width":1920,"height":1080,"fps":"30000/1001"}`, 3840, 2160,
			[]string{"-fps_mode passthrough -vf fps=30000/1001,scale=1920:1080:flags=lanczos -c:v libx265"}, nil},
		{"fps alone on x265", `{"codec":"libx265","preset":"slower","crf":29,"width":1920,"height":1080,"fps":29.97}`, 1920, 1080,
			[]string{"-vf fps=29.97 -c:v libx265"}, nil},
		{"fps between crop and scale", `{"codec":"libx264","preset":"veryfast","crf":23,"width":606,"height":1080,"crop":{"width":1080,"height":1920,"x":0,"y":0},"fps":24}`, 1920, 1920,
			[]string{"-vf crop=1080:1920:0:0,fps=24,scale=606:1080:flags=lanczos,format=yuv420p -c:v libx264"}, nil},
		{"sample rate alone", `{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"sample_rate":44100}}`, 1080, 1920,
			[]string{"-c:a aac -b:a 128k -ar 44100 -map_metadata"}, []string{"-ac"}},
		{"channels alone", `{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"channels":2}}`, 1080, 1920,
			[]string{"-c:a aac -b:a 128k -ac 2 -map_metadata"}, []string{"-ar"}},
		{"edit lists kept", `{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"edit_list":true}`, 1080, 1920,
			[]string{"-map_chapters -1 -movflags +faststart OUT"}, []string{"-use_editlist"}},
		{"no edit lists", `{"codec":"libx265","preset":"slower","crf":29,"width":1920,"height":1080,"audio":{"mode":"copy"},"edit_list":false}`, 1920, 1080,
			[]string{"-c:a copy -map_metadata -1 -map_chapters -1 -use_editlist 0 -movflags +faststart OUT"}, nil},
	} {
		var p RungParams
		if err := json.Unmarshal([]byte(c.params), &p); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		a, err := rungArgs("ffmpeg", p, "SRC", c.w, c.h, "", "OUT", 0)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got := strings.Join(a, " ")
		for _, s := range c.has {
			if !strings.Contains(got, s) {
				t.Errorf("%s: no %q in %s", c.name, s, got)
			}
		}
		for _, s := range c.hasNot {
			if strings.Contains(got, s) {
				t.Errorf("%s: %q in %s", c.name, s, got)
			}
		}
	}
}

func TestClipArgsRefuseWhatIsNoSetting(t *testing.T) {
	for _, params := range []string{
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"fps":0}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"fps":241}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"fps":"30/0"}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"fps":"ntsc"}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"fps":"60,scale=2:2"}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"sample_rate":47000}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"channels":9}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"copy","sample_rate":48000}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":64,"mono":true,"channels":2}}`,
	} {
		var p RungParams
		if err := json.Unmarshal([]byte(params), &p); err != nil {
			t.Fatalf("%s: %v", params, err)
		}
		if a, err := rungArgs("ffmpeg", p, "SRC", 1080, 1920, "", "OUT", 0); err == nil {
			t.Errorf("accepted %s: %v", params, a)
		}
	}
}

func TestFPSReadsANumberOrARatio(t *testing.T) {
	for in, want := range map[string]FPS{`60`: "60", `29.97`: "29.97", `"30000/1001"`: "30000/1001", ` "24" `: "24"} {
		var f FPS
		if err := json.Unmarshal([]byte(in), &f); err != nil || f != want {
			t.Errorf("%s: %q %v", in, f, err)
		}
	}
	if n := FPS("60000/1001").frames(60); n != 3596 {
		t.Errorf("60 s at 59.94: %d frames", n)
	}
}

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

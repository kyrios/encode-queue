package queue

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// "Calibration clip" (Thorsten, 2026-10-27): a new worker's tempo from the start, from the same
// clip on every machine, until measured jobs exist.

var clipA = strings.Repeat("a", 64)

// calibrated gives a worker a calibration of its reference tools: fps per class.
func (e *env) calibrated(wid, clip string, fps map[string]float64) {
	e.t.Helper()
	cal := &Calibration{ClipSHA256: clip, Tools: reference[:3], At: e.clock.Now()}
	for c, f := range fps {
		cal.Classes = append(cal.Classes, CalibratedClass{Class: c, Frames: 100, FPS: f})
	}
	if _, err := e.s.PutCapabilities(wid, Capabilities{AppVersion: "0.2.5", OS: "test", Tools: reference, Models: []Model{{"ggml-medium.bin", modelSHA}}, Calibration: cal}); err != nil {
		e.t.Fatal(err)
	}
}

const k4slower = "libx265 3840x2160 slower"
const hd = "libx265 1920x1080 veryfast"

func TestCalibratedSpeed(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.calibrated(mac, clipA, map[string]float64{k4slower: 6, hd: 180})
	if v := must(e.s.GetWorker(mac)); v.RelativeSpeed != nil || v.RelativeSpeedSource != nil {
		t.Fatalf("calibrated before the reference: %v", v.RelativeSpeed)
	}
	e.calibrated(ref, clipA, map[string]float64{k4slower: 2.5, hd: 100})
	v := must(e.s.GetWorker(mac))
	// The median of 6/2.5 = 2.4 and 180/100 = 1.8.
	if v.RelativeSpeed == nil || math.Abs(*v.RelativeSpeed-2.1) > 1e-9 || *v.RelativeSpeedSource != "calibrated" {
		t.Fatalf("calibrated: %v %v", v.RelativeSpeed, v.RelativeSpeedSource)
	}
	if v.SpeedEstimate != nil {
		t.Fatal("an estimate beside a calibrated speed")
	}
	var k4 *SpeedClass
	for i := range v.SpeedClasses {
		if v.SpeedClasses[i].Class == k4slower {
			k4 = &v.SpeedClasses[i]
		}
	}
	if k4 == nil || k4.Source != "calibrated" || k4.FPS != 6 || *k4.ReferenceFPS != 2.5 || k4.Width != 3840 || k4.Preset != "slower" {
		t.Fatalf("the 4K class: %+v", k4)
	}
	if r := must(e.s.GetWorker(ref)); r.RelativeSpeed == nil || *r.RelativeSpeed != 1 || *r.RelativeSpeedSource != "calibrated" {
		t.Fatalf("the reference: %v %v", r.RelativeSpeed, r.RelativeSpeedSource)
	}
	// The forecast takes the worker's own calibrated fps on the class.
	if f := e.s.fpsOn(e.s.st.Workers[mac], k4slower); f != 6 {
		t.Fatalf("fps on 4K slower %v, want 6", f)
	}
	// A measured result overrides the calibration.
	e.encode(ref, "r1.archive", 10)
	e.encode(mac, "m1.archive", 30)
	if v := must(e.s.GetWorker(mac)); math.Abs(*v.RelativeSpeed-3) > 1e-9 || *v.RelativeSpeedSource != "measured" {
		t.Fatalf("measured: %v %v", *v.RelativeSpeed, *v.RelativeSpeedSource)
	}
}

func TestCalibrationOfAnotherClip(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.calibrated(ref, clipA, map[string]float64{hd: 100})
	e.calibrated(mac, strings.Repeat("b", 64), map[string]float64{hd: 180})
	if v := must(e.s.GetWorker(mac)); v.RelativeSpeed != nil {
		t.Fatalf("compared clips that differ: %v", *v.RelativeSpeed)
	}
}

func TestCalibrationChecks(t *testing.T) {
	e := newEnv(t, "encoder")
	w := e.worker("mac", reference)
	put := func(cal *Calibration) *Calibration {
		e.s.PutCapabilities(w, Capabilities{AppVersion: "0.2.5", OS: "test", Tools: reference, Calibration: cal})
		return e.s.st.Workers[w].Caps.Calibration
	}
	good := func() *Calibration {
		return &Calibration{ClipSHA256: clipA, Tools: reference[:3], At: time.Now(), Classes: []CalibratedClass{{Class: hd, Frames: 150, FPS: 180}}}
	}
	if put(good()) == nil {
		t.Fatal("a good calibration dropped")
	}
	// Made with tools the worker no longer runs: a tools change calibrates again.
	old := good()
	old.Tools = []Tool{{"ffmpeg", "7.1.1"}}
	if put(old) != nil {
		t.Fatal("kept a calibration of other tools")
	}
	bad := good()
	bad.Classes = append(bad.Classes, CalibratedClass{Class: k4slower, FPS: math.Inf(1)}, CalibratedClass{Class: "x", FPS: -1})
	if c := put(bad); c == nil || len(c.Classes) != 1 {
		t.Fatalf("bad classes: %+v", c)
	}
	short := good()
	short.ClipSHA256 = "abc"
	if put(short) != nil {
		t.Fatal("kept a clip without a sha256")
	}
	b, _ := json.Marshal(must(e.s.GetWorker(w)))
	if !strings.Contains(string(b), `"relative_speed_source":null`) {
		t.Fatalf("relative_speed_source missing: %s", b)
	}
}

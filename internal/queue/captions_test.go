package queue

import (
	"encoding/json"
	"strings"
	"testing"
)

// Burnt-in captions (the #one preview's Loop version) need libass, and the reference's version
// exactly, so a Mac on toolchain 3 renders what encoder renders (v0.2.7).
func TestCaptionJobsNeedTheReferencesLibass(t *testing.T) {
	e := newEnv(t, "encoder")
	withAss := append(append([]Tool(nil), reference...), Tool{"libass", "0.17.5"})
	ref := e.worker("encoder", withAss)
	mac := e.worker("mac", append([]Tool(nil), withAss...))
	old := e.worker("old-mac", reference) // toolchain 2: no libass
	for _, id := range []string{ref, mac, old} {
		c := e.s.st.Workers[id].Caps
		c.Fonts = []string{"Inter ExtraBold"}
	}
	f := e.source("v/master.mov", "master")
	ass := e.source("v/captions.ass", "[Script Info]")
	params := `{"rung":"loop","codec":"libx264","preset":"veryfast","crf":20,"width":1080,"height":1080,"crop":{"width":2160,"height":2160,"x":840,"y":0},"captions":{"input":"captions","font":"Inter ExtraBold"}}`
	if _, _, err := e.s.Submit("v.loop", JobSubmit{Kind: KindRung, Priority: PriorityNow, Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, Width: 3840, Height: 2160}, Inputs: map[string]FileIn{"captions": ass}, Result: ResultSpec{Name: "loop.mp4", MaxBytes: 1 << 30}}); err != nil {
		t.Fatal(err)
	}
	j := e.s.st.Jobs["v.loop"]
	if why := e.s.mismatch(e.s.st.Workers[old], j); why != "libass missing" {
		t.Fatalf("a worker without libass: %q", why)
	}
	if why := e.s.mismatch(e.s.st.Workers[mac], j); why != "" {
		t.Fatalf("a worker with the reference's libass: %q", why)
	}
	e.s.st.Workers[mac].Caps.Tools[len(withAss)-1].Version = "0.17.3"
	if why := e.s.mismatch(e.s.st.Workers[mac], j); !strings.HasPrefix(why, "libass differs") {
		t.Fatalf("another libass: %q", why)
	}
	// A rung without captions does not ask for libass.
	e.rung("a.archive", "archive")
	if why := e.s.mismatch(e.s.st.Workers[old], e.s.st.Jobs["a.archive"]); why != "" {
		t.Fatalf("a plain rung on a worker without libass: %q", why)
	}
}

// encoder on toolchain 3 (libass 0.17.5 added; ffmpeg, libx265, libx264 and whisper.cpp read the
// same as toolchain 2, the releases' versions-darwin-arm64.txt diffed on 2026-10-27), a Mac still
// on toolchain 2 (Slipstream 1.2.0, worker v0.2.4): rung and whisper jobs go on reaching the Mac;
// only caption jobs need libass. The toolchain's own number is never a tool.
func TestAToolchain2WorkerAgainstAToolchain3Reference(t *testing.T) {
	e := newEnv(t, "encoder")
	tc2 := []Tool{{"ffmpeg", "9.0.2"}, {"libx265", "4.3+1-e9b8812"}, {"libx264", "core 165 r3222 b35605a"}, {"whisper.cpp", "v1.9.5"}}
	tc3 := append(append([]Tool(nil), tc2...), Tool{"libass", "0.17.5"})
	ref := e.worker("encoder", tc3)
	mac := e.worker("mac", tc2)
	e.s.st.Workers[ref].Caps.Fonts = []string{"Inter ExtraBold"}
	e.s.st.Workers[mac].Caps.AppVersion = "0.2.4"
	rep := must(e.s.GetCapabilities(mac))
	if !contains(rep.Accepted, KindRung) || !contains(rep.Accepted, KindWhisper) || len(rep.Refused) != 0 {
		t.Fatalf("the Mac's kinds: accepted %v refused %+v", rep.Accepted, rep.Refused)
	}
	e.rung("a.2160-archive", "archive")
	if why := e.s.mismatch(e.s.st.Workers[mac], e.s.st.Jobs["a.2160-archive"]); why != "" {
		t.Fatalf("a rung: %q", why)
	}
	f := e.source("w/sound.wav", "RIFF")
	if _, _, err := e.s.Submit("w.whisper", JobSubmit{Kind: KindWhisper, Priority: "stage1", Params: json.RawMessage(`{"model_sha256":"` + modelSHA + `","language":"de"}`),
		Source: SourceIn{FileIn: f, Container: "wav"}, Result: ResultSpec{Name: "w.json", MaxBytes: 1 << 20}}); err != nil {
		t.Fatal(err)
	}
	if why := e.s.mismatch(e.s.st.Workers[mac], e.s.st.Jobs["w.whisper"]); why != "" {
		t.Fatalf("whisper: %q", why)
	}
	ass := e.source("v/captions.ass", "[Script Info]")
	params := `{"rung":"loop","codec":"libx264","preset":"veryfast","crf":20,"width":1080,"height":1080,"captions":{"input":"captions","font":"Inter ExtraBold"}}`
	if _, _, err := e.s.Submit("v.loop", JobSubmit{Kind: KindRung, Priority: PriorityNow, Params: json.RawMessage(params),
		Source: SourceIn{FileIn: e.source("v/master.mov", "m"), Width: 3840, Height: 2160}, Inputs: map[string]FileIn{"captions": ass}, Result: ResultSpec{Name: "loop.mp4", MaxBytes: 1 << 30}}); err != nil {
		t.Fatal(err)
	}
	if why := e.s.mismatch(e.s.st.Workers[mac], e.s.st.Jobs["v.loop"]); why == "" {
		t.Fatal("a caption job reached a worker without libass")
	}
	if why := e.s.mismatch(e.s.st.Workers[ref], e.s.st.Jobs["v.loop"]); why != "" {
		t.Fatalf("the caption job on the reference: %q", why)
	}
	if l := e.claim(mac); l == nil || l.Job.ID == "v.loop" {
		t.Fatalf("the Mac's claim: %+v", l)
	}
}

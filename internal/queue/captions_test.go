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

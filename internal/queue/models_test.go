package queue

import (
	"encoding/json"
	"testing"
)

// Thorsten, 2026-10-27: large-v3-turbo where a worker runs it, medium on Linux as the fallback.
// A whisper job lists its models best first; a worse model runs only when no worker with a
// better one is connected; the lease names the model chosen.

// whisperModels submits captions from the audio that accept these models, best first.
func (e *env) whisperModels(id string, models ...string) {
	e.t.Helper()
	f := e.source(id+"/sound.wav", "RIFF "+id)
	params, _ := json.Marshal(map[string]any{"models": models, "language": "de"})
	if _, _, err := e.s.Submit(id, JobSubmit{Kind: KindWhisper, Priority: PriorityNow, Params: params,
		Source: SourceIn{FileIn: f, Container: "wav"}, Result: ResultSpec{Name: "whisper.json", MaxBytes: 1 << 20}}); err != nil {
		e.t.Fatal(err)
	}
}

// holds sets the models a worker has.
func (e *env) holds(wid string, shas ...string) {
	var ms []Model
	for _, s := range shas {
		ms = append(ms, Model{Name: catalogue[s].Name, SHA256: s})
	}
	e.s.st.Workers[wid].Caps.Models = ms
}

func TestWhisperPrefersTheBetterModel(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.holds(enc, SHASmall, SHAMedium)
	e.holds(mac, SHASmall, SHALargeV3Turbo)
	e.whisperModels("a.whisper", SHALargeV3Turbo, SHAMedium)
	// encoder asks first: the Mac, connected, holds the better model, so encoder leaves it.
	if l := e.claim(enc); l != nil {
		t.Fatalf("encoder took a job the Mac runs with large-v3-turbo: %+v", l)
	}
	l := e.claim(mac)
	if l == nil || l.Job.ID != "a.whisper" {
		t.Fatalf("the Mac: %+v", l)
	}
	var p WhisperParams
	json.Unmarshal(l.Params, &p)
	if p.ModelSHA256 != SHALargeV3Turbo || p.Model != "ggml-large-v3-turbo.bin" || p.Models != nil || p.Language != "de" {
		t.Fatalf("the lease's params: %+v", p)
	}
	if v := must(e.s.GetJob("a.whisper")); v.Attempts[0].ModelSHA256 == nil || *v.Attempts[0].ModelSHA256 != SHALargeV3Turbo {
		t.Fatalf("the attempt does not name its model: %+v", v.Attempts[0])
	}
	// With the Mac gone, encoder takes the next one with medium.
	e.whisperModels("b.whisper", SHALargeV3Turbo, SHAMedium)
	e.s.st.Workers[mac].LastSeen = nil
	l = e.claim(enc)
	if l == nil || l.Job.ID != "b.whisper" {
		t.Fatalf("encoder as the fallback: %+v", l)
	}
	json.Unmarshal(l.Params, &p)
	if p.ModelSHA256 != SHAMedium {
		t.Fatalf("encoder's model %s", p.ModelSHA256)
	}
}

func TestWhisperModelsAreChecked(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.holds(w, SHASmall)
	f := e.source("x/sound.wav", "RIFF")
	sub := func(params string) error {
		_, _, err := e.s.Submit("x.whisper", JobSubmit{Kind: KindWhisper, Priority: "stage1", Params: json.RawMessage(params),
			Source: SourceIn{FileIn: f, Container: "wav"}, Result: ResultSpec{Name: "w.json", MaxBytes: 1 << 20}})
		return err
	}
	for _, bad := range []string{`{"language":"de"}`, `{"models":["nope"],"language":"de"}`, `{"models":["` + SHASmall + `"]}`} {
		if sub(bad) == nil {
			t.Fatalf("took %s", bad)
		}
	}
	if err := sub(`{"models":["` + SHAMedium + `"],"language":"de"}`); err != nil {
		t.Fatal(err)
	}
	if why := e.s.mismatch(e.s.st.Workers[w], e.s.st.Jobs["x.whisper"]); why != "model "+SHAMedium[:12]+" missing" {
		t.Fatalf("without the model: %q", why)
	}
	if v := must(e.s.GetJob("x.whisper")); v.Quality == nil || *v.Quality != "ggml-medium.bin" {
		t.Fatalf("quality %v", v.Quality)
	}
}

// timed gives a worker a calibration with whisper speeds by model and its RAM.
func (e *env) timed(wid string, memory int64, speeds map[string]float64) {
	e.t.Helper()
	hw, _ := json.Marshal(map[string]any{"memory_bytes": memory})
	cal := &Calibration{ClipSHA256: clipA, Tools: reference[:3], Classes: []CalibratedClass{{Class: hd, Frames: 100, FPS: 100}}}
	var models []Model
	for sha, v := range speeds {
		cal.Whisper = append(cal.Whisper, CalibratedModel{SHA256: sha, Name: catalogue[sha].Name, Whisper: "v1.9.5", AudioS: 21.6, Speed: v})
		models = append(models, Model{Name: catalogue[sha].Name, SHA256: sha})
	}
	if _, err := e.s.PutCapabilities(wid, Capabilities{AppVersion: "0.2.7", OS: "test", Tools: reference, Models: models, Hardware: hw, Calibration: cal}); err != nil {
		e.t.Fatal(err)
	}
}

func names(ms []CatalogueModel) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Name)
	}
	return out
}

// Thorsten: "Choose the whisper model based on measured performance at first launch." The
// measurement decides, not the platform.
func TestWantedModelsFromTheMeasuredSpeed(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	wanted := func() []string { return names(must(e.s.GetCapabilities(w)).WantedModels) }
	if got := wanted(); len(got) != 1 || got[0] != "ggml-small.bin" {
		t.Fatalf("before any whisper timing: %v", got)
	}
	// The Mac on Metal: small at 12× real time, large-v3-turbo guessed at 6×.
	e.timed(w, 32<<30, map[string]float64{SHASmall: 12})
	if got := wanted(); len(got) != 2 || got[1] != "ggml-large-v3-turbo.bin" {
		t.Fatalf("a fast Mac: %v", got)
	}
	rep := must(e.s.GetCapabilities(w))
	if u := rep.WantedModels[1]; u.Bytes != 1624555275 || u.SHA256 != SHALargeV3Turbo || u.URL != "https://huggingface.co/ggerganov/whisper.cpp/resolve/"+WhisperRevision+"/ggml-large-v3-turbo.bin" {
		t.Fatalf("the wanted model: %+v", u)
	}
	// encoder's CPU: large-v3-turbo measured at 0.8×, so medium (guessed 1.0×) instead.
	e.timed(w, 8<<30, map[string]float64{SHASmall: 2.5, SHALargeV3Turbo: 0.8})
	if got := wanted(); len(got) != 2 || got[1] != "ggml-medium.bin" {
		t.Fatalf("a slow CPU: %v", got)
	}
	// Too little RAM for either: small only.
	e.timed(w, 3<<30, map[string]float64{SHASmall: 12})
	if got := wanted(); len(got) != 1 {
		t.Fatalf("3 GB of RAM: %v", got)
	}
	// The threshold is EQ_WHISPER_MIN_SPEED_PCT.
	e.s.cfg.WhisperMinSpeed = 7
	e.timed(w, 32<<30, map[string]float64{SHASmall: 12})
	if got := wanted(); len(got) != 1 {
		t.Fatalf("below 7×: %v", got)
	}
}

// encoder on v0.2.7, 2026-10-27 18:17: small 2.87×, medium 0.96×, and the queue wanted
// large-v3-turbo from a guess (2.87 / 2 = 1.4×). A guess does not win over a measured model below
// it that is too slow, and the best model held stays the fallback.
func TestEncodersMeasuredSpeedsWantNoGuessedModel(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.timed(w, 8<<30, map[string]float64{SHASmall: 2.87, SHAMedium: 0.96})
	got := names(must(e.s.GetCapabilities(w)).WantedModels)
	if len(got) != 2 || got[0] != "ggml-small.bin" || got[1] != "ggml-medium.bin" {
		t.Fatalf("encoder wants %v, want small and its medium as the fallback", got)
	}
	// The same small without a medium measured: the guess for large-v3-turbo stands (a fast Mac).
	e.timed(w, 32<<30, map[string]float64{SHASmall: 12})
	if got := names(must(e.s.GetCapabilities(w)).WantedModels); len(got) != 2 || got[1] != "ggml-large-v3-turbo.bin" {
		t.Fatalf("a fast Mac wants %v", got)
	}
	// large-v3-turbo measured fast beside a slow medium: measured, so wanted.
	e.timed(w, 32<<30, map[string]float64{SHASmall: 2.87, SHAMedium: 0.96, SHALargeV3Turbo: 1.2})
	if got := names(must(e.s.GetCapabilities(w)).WantedModels); len(got) != 2 || got[1] != "ggml-large-v3-turbo.bin" {
		t.Fatalf("a measured turbo: %v", got)
	}
}

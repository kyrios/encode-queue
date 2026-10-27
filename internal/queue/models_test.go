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

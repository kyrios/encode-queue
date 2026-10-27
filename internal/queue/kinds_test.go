package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// v0.2.8 (ADR-0007): every encode and every decode on a worker. The queue takes cut, remux and
// check jobs, gives them only to workers that run them, and keeps the check's figures for the
// pipeline to judge.

// v028 makes a worker one of 0.2.8.
func (e *env) v028(wid string) { e.s.st.Workers[wid].Caps.AppVersion = "0.2.8" }

func (e *env) submit(id, kind, params string, inputs map[string]FileIn) error {
	f := e.source(strings.Split(id, ".")[0]+"/master.mov", "master "+id)
	_, _, err := e.s.Submit(id, JobSubmit{Kind: kind, Priority: "stage1", Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, Width: 3840, Height: 2160}, Inputs: inputs, Result: ResultSpec{Name: "out.mp4", MaxBytes: 1 << 30}})
	return err
}

const (
	cutParams   = `{"start_s":2.5,"length_s":5,"fade_s":1,"preset":"veryfast","crf":14,"bit_depth":10,"colour":{"primaries":"bt709","transfer":"bt709","space":"bt709"},"audio":{"bitrate_kbps":256},"check":{"decode":true}}`
	remuxParams = `{"hvc1_tag":true,"check":{"decode":true,"framemd5":true}}`
	checkParams = `{"candidate":"candidate","width":1920,"height":1080,"check":{"decode":true,"compare":{"every":5,"metrics":["ssim","psnr_y","xpsnr"],"vmaf":{"model":"vmaf_v0.6.1"}}}}`
	rungChecked = `{"rung":"1080","codec":"libx265","preset":"slow","crf":30,"width":1920,"height":1080,"audio":{"mode":"aac","bitrate_kbps":64,"mono":"measure","mono_side_below_db":40},"check":{"decode":true,"compare":{"every":5}}}`
)

func TestTheNewKindsAreChecked(t *testing.T) {
	e := newEnv(t, "")
	cand := map[string]FileIn{"candidate": e.source("c/2160.mp4", "a file")}
	for _, ok := range []struct{ id, kind, params string }{
		{"a.cut", KindCut, cutParams}, {"b.remux", KindRemux, remuxParams}, {"d.2160-stage1", KindRung, rungChecked},
	} {
		if err := e.submit(ok.id, ok.kind, ok.params, nil); err != nil {
			t.Fatalf("%s: %v", ok.id, err)
		}
	}
	if err := e.submit("c.check", KindCheck, checkParams, cand); err != nil {
		t.Fatal(err)
	}
	if r := e.s.st.Jobs["d.2160-stage1"].rung; !r.Audio.Mono.Measure || r.Check == nil {
		t.Fatalf("the rung's mono and check: %+v", r)
	}
	for _, bad := range []struct{ kind, params string }{
		{KindCut, `{"start_s":-1,"length_s":5,"preset":"veryfast","crf":14,"bit_depth":8}`},
		{KindCut, `{"start_s":0,"length_s":5,"fade_s":6,"preset":"veryfast","crf":14,"bit_depth":8}`},
		{KindCut, `{"start_s":0,"length_s":5,"preset":"fastest","crf":14,"bit_depth":8}`},
		{KindCut, `{"start_s":0,"length_s":5,"preset":"veryfast","crf":14,"bit_depth":12}`},
		{KindCut, `{"start_s":0,"length_s":5,"preset":"veryfast","crf":14,"bit_depth":8,"colour":{"space":"bt709; rm"}}`},
		{KindRemux, `{"check":{"compare":{"metrics":["mse"]}}}`},
		{KindRemux, `{"hvc1":true}`},
		{KindCheck, `{"check":{"decode":true},"candidate":"other"}`},
		{KindCheck, `{"candidate":"candidate"}`},
		{KindCheck, `{"check":{"compare":{"every":5}}}`},
		{KindRung, `{"codec":"libx265","preset":"slow","crf":30,"width":1920,"height":1080,"audio":{"mode":"aac","bitrate_kbps":64,"mono":"maybe"}}`},
	} {
		if err := e.submit("x.y", bad.kind, bad.params, cand); err == nil {
			t.Fatalf("took %s %s", bad.kind, bad.params)
		}
	}
}

func TestTheNewKindsGoOnlyToWorkersThatRunThem(t *testing.T) {
	e := newEnv(t, "")
	old := e.worker("mac", reference)
	enc := e.worker("encoder", reference)
	e.v028(enc)
	e.submit("a.cut", KindCut, cutParams, nil)
	e.submit("d.2160-stage1", KindRung, rungChecked, nil)
	e.rung("p.2160-archive", "archive")
	for _, id := range []string{"a.cut", "d.2160-stage1"} {
		if why := e.s.mismatch(e.s.st.Workers[old], e.s.st.Jobs[id]); !strings.Contains(why, "(0.2.8)") {
			t.Fatalf("%s on a worker of 0.1.0: %q", id, why)
		}
		if why := e.s.mismatch(e.s.st.Workers[enc], e.s.st.Jobs[id]); why != "" {
			t.Fatalf("%s on a worker of 0.2.8: %q", id, why)
		}
	}
	if why := e.s.mismatch(e.s.st.Workers[old], e.s.st.Jobs["p.2160-archive"]); why != "" {
		t.Fatalf("a plain rung on an old worker: %q", why)
	}
}

const figures = `{"decode":{"errors":0,"lines":[]},"frames":7200,"source_frames":7200,"compare":{"every":5,"pairs":1440,"ssim":0.9871,"psnr_y":44.21,"xpsnr":41.07,"vmaf":null},"audio_levels":{"mid_db":-23.1,"side_db":"-inf","mono":true},"check_s":41.2}`

func TestTheFiguresGoToThePipeline(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.v028(w)
	// A rung's file with the figures of its check step.
	e.submit("d.2160-stage1", KindRung, rungChecked, nil)
	l := e.claim(w)
	if _, err := e.s.Renew(w, l.ID, Progress{Phase: "checking"}); err != nil {
		t.Fatalf("the checking phase: %v", err)
	}
	content := "the 1080 rung"
	fid := e.upload(w, l.ID, content)
	in := ResultIn{Outcome: "file", File: "/v1/files/" + fid, SHA256: sha(content), Check: json.RawMessage(figures)}
	if _, err := e.s.Deliver(w, l.ID, in); err != nil {
		t.Fatal(err)
	}
	a := must(e.s.GetJob("d.2160-stage1")).Attempts[0]
	if a.Result == nil || string(a.Result.Check) != figures || a.Result.Verdict != VerdictChecking {
		t.Fatalf("the attempt's figures: %+v", a.Result)
	}
	// A check job: figures and no file; the pipeline accepts it like a file.
	e.submit("c.check", KindCheck, checkParams, map[string]FileIn{"candidate": e.source("c/2160.mp4", "a file")})
	l = e.claim(w)
	if _, err := e.s.Deliver(w, l.ID, ResultIn{Outcome: "file", File: "/v1/files/x", SHA256: sha("x")}); err == nil {
		t.Fatal("a check job delivered a file")
	}
	if _, err := e.s.Deliver(w, l.ID, ResultIn{Outcome: "figures"}); err == nil {
		t.Fatal("figures without check")
	}
	if _, err := e.s.Deliver(w, l.ID, ResultIn{Outcome: "figures", Check: json.RawMessage(`[1,2]`)}); err == nil {
		t.Fatal("figures that are no object")
	}
	if _, err := e.s.Deliver(w, l.ID, ResultIn{Outcome: "figures", Check: json.RawMessage(figures), EncodeS: 41.2, Tools: reference[:1]}); err != nil {
		t.Fatal(err)
	}
	if v, err := e.s.PutVerdict("c.check", 1, Verdict{Verdict: VerdictAccepted}); err != nil || v.State != "done" {
		t.Fatalf("the verdict: %+v %v", v.State, err)
	}
	if a := must(e.s.GetJob("c.check")).Attempts[0]; a.Result.Outcome != "figures" || a.Result.Path != nil || string(a.Result.Check) != figures {
		t.Fatalf("the check job's result: %+v", a.Result)
	}
	// Figures alone only from a check job.
	e.rung("p.2160-archive", "archive")
	l = e.claim(w)
	if _, err := e.s.Deliver(w, l.ID, ResultIn{Outcome: "figures", Check: json.RawMessage(figures)}); err == nil {
		t.Fatal("a rung delivered figures without a file")
	}
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

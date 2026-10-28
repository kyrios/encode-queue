package queue

import (
	"encoding/json"
	"strings"
	"testing"
)

// mediaWorker is encoder at 0.2.22: 8 GB, a 4K slower encode's 5.3 GB in use.
func (e *env) mediaWorker(name, version string) string {
	e.t.Helper()
	w := e.worker(name, reference)
	e.machine(w, 8*gib, 53*gib/10)
	e.s.st.Workers[w].Caps.AppVersion = version
	return w
}

func (e *env) media(id, kind, params, prio string, maxBytes int64) (JobView, error) {
	f := e.source(strings.Split(id, ".")[0]+"/h264.mp4", "h264 of "+id)
	_, _, err := e.s.Submit(id, JobSubmit{Kind: kind, Priority: prio, Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, DurationS: 7200}, Result: ResultSpec{Name: kind + ".out", MaxBytes: maxBytes}})
	if err != nil {
		return JobView{}, err
	}
	return e.s.GetJob(id)
}

// TestMediaJobsAreRealtime: a peaks or strip job without a priority is now; one may say otherwise.
func TestMediaJobsAreRealtime(t *testing.T) {
	e := newEnv(t, "")
	for _, kind := range []string{KindPeaks, KindStrip} {
		v, err := e.media("rec"+kind+".x", kind, `{}`, "", 1<<20)
		if err != nil || v.Priority != PriorityNow {
			t.Errorf("%s: %+v %v", kind, v.Priority, err)
		}
	}
	if v, err := e.media("stage2strip.x", KindStrip, `{}`, "stage2", 1<<20); err != nil || v.Priority != "stage2" {
		t.Errorf("a strip at stage2: %+v %v", v.Priority, err)
	}
	// Other kinds still need one.
	if _, _, err := e.s.Submit("r.x", JobSubmit{Kind: KindRemux, Params: json.RawMessage(`{}`),
		Source: SourceIn{FileIn: e.source("r/m.mov", "m")}, Result: ResultSpec{Name: "r.mp4", MaxBytes: 1}}); code(err) != "invalid" {
		t.Errorf("a remux without a priority: %v", err)
	}
}

func TestMediaParams(t *testing.T) {
	e := newEnv(t, "")
	for _, c := range []struct {
		kind, params string
		max          int64
		ok           bool
	}{
		{KindPeaks, `{}`, 4 << 20, true},
		{KindPeaks, `{"rate":16000}`, 4 << 20, false}, // unknown field
		{KindPeaks, `{}`, 65 << 20, false},            // more than 64 MiB
		{KindStrip, `{"height":90,"fine_every_s":4,"scenes":32,"from":"h264.mp4"}`, 8 << 20, true},
		{KindStrip, `{"height":91}`, 8 << 20, false},
		{KindStrip, `{"height":720}`, 8 << 20, false},
		{KindStrip, `{"fine_every_s":0.1}`, 8 << 20, false},
		{KindStrip, `{"scenes":33}`, 8 << 20, false},
		{KindStrip, `{"from":"../h264.mp4"}`, 8 << 20, false},
	} {
		_, err := e.media("p"+strings.NewReplacer(`"`, "", "{", "", "}", "", ":", "", ",", "", ".", "", "/", "").Replace(c.params)+".x",
			c.kind, c.params, "", c.max)
		if (err == nil) != c.ok {
			t.Errorf("%s %s max %d: %v", c.kind, c.params, c.max, err)
		}
	}
}

// TestMediaNeedsV0222: a worker before 0.2.22 neither takes nor refuses them; one of 0.2.22
// with the reference's ffmpeg takes both.
func TestMediaNeedsV0222(t *testing.T) {
	e := newEnv(t, "")
	old := e.mediaWorker("old", "0.2.20")
	cur := e.mediaWorker("encoder", "0.2.22")
	if _, err := e.media("a.peaks", KindPeaks, `{}`, "", 1<<20); err != nil {
		t.Fatal(err)
	}
	j := e.s.st.Jobs["a.peaks"]
	if why := e.s.mismatch(e.s.st.Workers[old], j); !strings.Contains(why, "no peaks or strip jobs (0.2.22)") {
		t.Errorf("0.2.20: %q", why)
	}
	if why := e.s.mismatch(e.s.st.Workers[cur], j); why != "" {
		t.Errorf("0.2.22: %q", why)
	}
	acc, ref := e.s.kinds(e.s.st.Workers[old])
	for _, r := range ref {
		if r.Kind == KindPeaks || r.Kind == KindStrip {
			t.Errorf("0.2.20 refuses %+v", r)
		}
	}
	if contains(acc, KindPeaks) || contains(acc, KindStrip) {
		t.Errorf("0.2.20 kinds %v", acc)
	}
	if acc, _ := e.s.kinds(e.s.st.Workers[cur]); !contains(acc, KindPeaks) || !contains(acc, KindStrip) {
		t.Errorf("0.2.22 kinds %v", acc)
	}
}

// TestMediaBesideA4KEncode: encoder runs a 4K slower archive encode in its one regular slot; a
// peaks job and then a strip job, realtime, each take the high-priority slot at once (512 MiB
// fit beside the encode's 5.3 GB in 8 GB), never preempt the encode, and the regular slot stays
// the encode's.
func TestMediaBesideA4KEncode(t *testing.T) {
	e := newEnv(t, "")
	w := e.mediaWorker("encoder", "0.2.22")
	e.rung("old.2160-archive", "archive")
	enc := e.claim(w)
	if enc == nil {
		t.Fatal("no encode")
	}
	for _, kind := range []string{KindPeaks, KindStrip} {
		id := "rec" + kind + ".x"
		if _, err := e.media(id, kind, `{}`, "", 4<<20); err != nil {
			t.Fatal(err)
		}
		if e.s.preempt(e.clock.Now()) {
			t.Fatalf("%s preempted the encode", kind)
		}
		x, err := e.extra(w)
		if err != nil || x == nil || x.Job.ID != id {
			t.Fatalf("%s: the high-priority slot gave %+v %v", kind, x, err)
		}
		if ect, ok := e.s.runSeconds(e.s.st.Workers[w], e.s.st.Jobs[id]); !ok || ect > 20 {
			t.Errorf("%s: %v s for two hours, known %v", kind, ect, ok)
		}
		if err := e.s.Release(w, x.ID, "user"); err != nil {
			t.Fatal(err)
		}
		e.s.CancelJob(id)
	}
	if v := must(e.s.GetJob("old.2160-archive")); v.State != "leased" || v.Attempts[0].LeaseID != enc.ID {
		t.Fatalf("the encode: %+v", v)
	}
}

// TestMediaTypes: Funkhaus's Typ column names them "Peaks" and "Filmstrip".
func TestMediaTypes(t *testing.T) {
	e := newEnv(t, "")
	for _, c := range []struct{ kind, params, typ, detail string }{
		{KindPeaks, `{}`, "Peaks", ""},
		{KindStrip, `{}`, "Filmstrip", "90p · every 4 s"},
		{KindStrip, `{"height":120,"fine_every_s":2.5}`, "Filmstrip", "120p · every 2.5 s"},
	} {
		v, err := e.media("t"+c.kind+strings.NewReplacer(`"`, "", "{", "", "}", "", ":", "", ",", "", ".", "").Replace(c.params)+".x",
			c.kind, c.params, "", 1<<20)
		if err != nil || v.Type != c.typ || v.Detail != c.detail {
			t.Errorf("%s %s: %q %q %v", c.kind, c.params, v.Type, v.Detail, err)
		}
	}
}

// TestMediaSecondsByTheSource: a 4K upload's strip takes longer than an h264.mp4's.
func TestMediaSecondsByTheSource(t *testing.T) {
	j := &Job{strip: &StripParams{}}
	j.Spec.Source = SourceIn{DurationS: 7200, Width: 1280, Height: 640}
	small, _ := mediaSeconds(j)
	j.Spec.Source.Width, j.Spec.Source.Height = 3840, 2160
	big, _ := mediaSeconds(j)
	if small > 20 || big < 250 || big > 350 {
		t.Errorf("two hours: h264.mp4 %v s, 4K %v s", small, big)
	}
}

// TestLoudnessJobs (v0.2.24): realtime by default, `{}` only, "Lautheit" on the Encoder page,
// and only for a worker of 0.2.24.
func TestLoudnessJobs(t *testing.T) {
	e := newEnv(t, "")
	v, err := e.media("recl.loudness", KindLoudness, `{}`, "", 1<<20)
	if err != nil || v.Priority != PriorityNow || v.Type != "Lautheit" || v.Detail != "EBU R 128" {
		t.Fatalf("%+v %v", v, err)
	}
	if _, err := e.media("recm.loudness", KindLoudness, `{"target":-16}`, "", 1<<20); err == nil {
		t.Error("an unknown param was taken")
	}
	old := e.mediaWorker("old", "0.2.23")
	cur := e.mediaWorker("encoder", "0.2.24")
	j := e.s.st.Jobs["recl.loudness"]
	if why := e.s.mismatch(e.s.st.Workers[old], j); !strings.Contains(why, "no loudness jobs (0.2.24)") {
		t.Errorf("0.2.23: %q", why)
	}
	if why := e.s.mismatch(e.s.st.Workers[cur], j); why != "" {
		t.Errorf("0.2.24: %q", why)
	}
	if acc, _ := e.s.kinds(e.s.st.Workers[old]); contains(acc, KindLoudness) || !contains(acc, KindPeaks) {
		t.Errorf("0.2.23 kinds %v", acc)
	}
	if acc, _ := e.s.kinds(e.s.st.Workers[cur]); !contains(acc, KindLoudness) {
		t.Errorf("0.2.24 kinds %v", acc)
	}
	if s, ok := e.s.runSeconds(e.s.st.Workers[cur], j); !ok || s > 15 {
		t.Errorf("two hours' loudness: %v s", s)
	}
}

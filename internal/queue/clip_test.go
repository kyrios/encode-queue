package queue

import (
	"encoding/json"
	"strings"
	"testing"
)

// v0.2.13: a clip's rung (video-pipeline v0.2.10) sets the frame rate, the AAC's sample rate and
// channels, and an MP4 without edit lists. The queue checks them and gives such a rung only to a
// worker of 0.2.13 or later: an older one would drop the fields and make the file without them.

// clipParams is a clip's rung as video-pipeline sends it: the 9:16 window of a 3840x1920 master.
const clipParams = `{"rung":"mastodon","codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,` +
	`"crop":{"width":1080,"height":1920,"x":1380,"y":0},"vbv_maxrate_kbps":3992,"vbv_bufsize_kbit":7984,` +
	`"audio":{"mode":"aac","bitrate_kbps":128,"sample_rate":48000,"channels":2},"check":{"decode":true,"frames":true},` +
	`"fps":60,"edit_list":false}`

// rungWith is a plain libx264 rung with these fields added.
func rungWith(fields string) string {
	return `{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128}` + fields + `}`
}

func TestAClipRungIsChecked(t *testing.T) {
	e := newEnv(t, "")
	if err := e.submit("c.mastodon-s2", KindRung, clipParams, nil); err != nil {
		t.Fatal(err)
	}
	r := e.s.st.Jobs["c.mastodon-s2"].rung
	if r.FPS == nil || *r.FPS != "60" || r.EditList == nil || *r.EditList || r.Audio.SampleRate != 48000 || r.Audio.Channels != 2 {
		t.Fatalf("the clip's settings: %+v %+v", r, r.Audio)
	}
	for i, ok := range []string{
		rungWith(`,"fps":"30000/1001"`),
		rungWith(`,"fps":29.97`),
		rungWith(`,"fps":null`),
		rungWith(`,"edit_list":true`),
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"sample_rate":44100}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":64,"mono":true,"sample_rate":48000}}`,
	} {
		if err := e.submit("ok.r"+string(rune('a'+i)), KindRung, ok, nil); err != nil {
			t.Errorf("refused %s: %v", ok, err)
		}
	}
	for _, bad := range []string{
		rungWith(`,"fps":0`),
		rungWith(`,"fps":241`),
		rungWith(`,"fps":-30`),
		rungWith(`,"fps":"30/0"`),
		rungWith(`,"fps":"ntsc"`),
		rungWith(`,"fps":"60,scale=1:1"`),
		rungWith(`,"fps":1e2`),
		rungWith(`,"fps":true`),
		rungWith(`,"edit_list":"no"`),
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"sample_rate":47000}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"channels":9}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":128,"channels":-1}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"copy","sample_rate":48000}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"none","channels":2}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":64,"mono":true,"channels":2}}`,
		`{"codec":"libx264","preset":"slow","crf":23,"width":1080,"height":1920,"audio":{"mode":"aac","bitrate_kbps":64,"mono":"measure","channels":1}}`,
	} {
		err := e.submit("x.y", KindRung, bad, nil)
		if qe, ok := err.(*Error); !ok || qe.Status != 422 {
			t.Errorf("took %s: %v", bad, err)
		}
	}
}

func TestAClipRungGoesOnlyToAWorkerOf0213(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	vimes := e.worker("vimes", reference)
	e.s.st.Workers[enc].Caps.AppVersion = "0.2.12"
	e.s.st.Workers[vimes].Caps.AppVersion = "0.2.8"
	if err := e.submit("c.mastodon-s2", KindRung, clipParams, nil); err != nil {
		t.Fatal(err)
	}
	e.rung("p.2160-archive", "archive")
	e.submit("k.1080-s1", KindRung, rungWith(`,"edit_list":true`), nil) // true is ffmpeg's own: any worker
	clip := e.s.st.Jobs["c.mastodon-s2"]
	for _, w := range []string{enc, vimes} {
		why := e.s.mismatch(e.s.st.Workers[w], clip)
		if want := "would make the file without fps, audio.sample_rate, audio.channels, edit_list (0.2.13)"; !strings.HasSuffix(why, want) {
			t.Fatalf("the clip on %s: %q", e.s.st.Workers[w].Name, why)
		}
		for _, id := range []string{"p.2160-archive", "k.1080-s1"} {
			if why := e.s.mismatch(e.s.st.Workers[w], e.s.st.Jobs[id]); why != "" {
				t.Fatalf("%s on %s: %q", id, e.s.st.Workers[w].Name, why)
			}
		}
	}
	// The clip is first in line (stage1); the old workers take what follows it.
	if l := e.claim(enc); l == nil || l.Job.ID != "k.1080-s1" {
		t.Fatalf("encoder 0.2.12 took %+v", l)
	}
	if l := e.claim(vimes); l == nil || l.Job.ID != "p.2160-archive" {
		t.Fatalf("vimes 0.2.8 took %+v", l)
	}
	// The Encoder page says what the clip waits for, and moving it to an old worker is refused.
	v := must(e.s.GetJob("c.mastodon-s2"))
	want := "it waits for a worker of 0.2.13 or later, which sets fps, audio.sample_rate, audio.channels, edit_list (paired: encoder 0.2.12, vimes 0.2.8)"
	if v.State != StateQueued || v.Waiting == nil || v.Waiting.Code != WaitNoEligibleWorker || v.Waiting.Detail != want {
		t.Fatalf("the clip's waiting: %s %+v", v.State, v.Waiting)
	}
	if _, err := e.s.AssignJob("c.mastodon-s2", Assign{WorkerID: enc}); err == nil || !strings.Contains(err.Error(), "(0.2.13)") {
		t.Fatalf("moved to encoder 0.2.12: %v", err)
	}
	cands := must(e.s.JobCandidates("c.mastodon-s2"))
	for _, c := range cands.Items {
		if c.Available || c.Reason == nil || !strings.Contains(*c.Reason, "(0.2.13)") {
			t.Fatalf("the move menu's %s: %v %v", c.Worker.Name, c.Available, c.Reason)
		}
	}
	// A worker of 0.2.13 takes it.
	mac := e.worker("mac", reference)
	e.s.st.Workers[mac].Caps.AppVersion = "0.2.13"
	if v := must(e.s.GetJob("c.mastodon-s2")); v.Waiting != nil {
		t.Fatalf("waiting beside a worker of 0.2.13: %+v", v.Waiting)
	}
	l := e.claim(mac)
	if l == nil || l.Job.ID != "c.mastodon-s2" {
		t.Fatalf("mac 0.2.13 took %+v", l)
	}
	// The lease hands the worker the params as the pipeline sent them, byte for byte.
	if string(l.Params) != clipParams {
		t.Fatalf("the lease's params:\n %s\nnot\n %s", l.Params, clipParams)
	}
}

func TestAClipRungsFramesAreTheRates(t *testing.T) {
	j := &Job{Spec: JobSubmit{Kind: KindRung, Params: json.RawMessage(rungWith(`,"fps":"60000/1001"`)),
		Source: SourceIn{DurationS: 60, Frames: 14386}}}
	if err := j.parse(); err != nil {
		t.Fatal(err)
	}
	if n := j.outFrames(); n != 3596 {
		t.Fatalf("60 s of a 239.76 fps source at 59.94: %d frames", n)
	}
	if n := eta(j, &Progress{Phase: "encoding", FPS: f64(100), FramesDone: i64(596)}); n == nil || *n != 30 {
		t.Fatalf("the eta of 3000 frames left at 100 fps: %v", n)
	}
	j.Spec.Source.DurationS = 0 // the length unknown: the source's frames
	if n := j.outFrames(); n != 14386 {
		t.Fatalf("without the length: %d", n)
	}
	// The rate keeps its form: a ratio as text, a number as a number.
	for in, want := range map[FPS]string{"60000/1001": `"60000/1001"`, "60": `60`, "29.97": `29.97`} {
		if b := must(json.Marshal(in)); string(b) != want {
			t.Errorf("%s as JSON: %s", in, b)
		}
	}
}

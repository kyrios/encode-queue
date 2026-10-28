package queue

import "testing"

// What a job makes, as Funkhaus's Encoder page shows it (v0.2.19): a type and one line of detail.
func TestEveryJobSaysItsTypeAndDetail(t *testing.T) {
	e := newEnv(t, "")
	e.rung("a.2160-archive", "archive")
	e.smallRung("b.720-stage2", "stage2")
	e.captions("rec-1.whisper-1", 64<<20)
	if err := e.alignJob("c.align", alignParams, "mp4"); err != nil {
		t.Fatal(err)
	}
	cand := map[string]FileIn{"candidate": e.source("c/2160.mp4", "a file")}
	for _, k := range []struct{ id, kind, params string }{
		{"d.cut", KindCut, cutParams}, {"e.remux", KindRemux, remuxParams}, {"f.check", KindCheck, checkParams},
	} {
		in := map[string]FileIn(nil)
		if k.kind == KindCheck {
			in = cand
		}
		if err := e.submit(k.id, k.kind, k.params, in); err != nil {
			t.Fatalf("%s: %v", k.id, err)
		}
	}
	for id, want := range map[string][2]string{
		"a.2160-archive":  {TypeVideo, "2160p · slower CRF 29"},
		"b.720-stage2":    {TypeVideo, "720p H.264 · veryfast CRF 23"},
		"rec-1.whisper-1": {TypeCaptions, "turbo"},
		"c.align":         {TypeAlign, "Qwen3-ForcedAligner-0.6B · de"},
		"d.cut":           {TypeCut, "2.5–7.5 s"},
		"e.remux":         {TypeRemux, "hvc1"},
		"f.check":         {TypeCheck, "SSIM, PSNR-Y, XPSNR, VMAF · every 5th frame · decode"},
	} {
		v, err := e.s.GetJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if [2]string{v.Type, v.Detail} != want {
			t.Errorf("%s: %q %q, want %q %q", id, v.Type, v.Detail, want[0], want[1])
		}
	}
	// A kind from a later queue the page does not know is its own type.
	if typ, _ := jobType(&Job{Spec: JobSubmit{Kind: "peaks"}}); typ != "Peaks" {
		t.Fatalf("an unknown kind: %q", typ)
	}
	for sha, want := range map[string]string{SHALargeV3Turbo: "turbo", SHAMedium: "medium", SHASmall: "small"} {
		if got := shortModel(sha, ""); got != want {
			t.Errorf("%s: %q", want, got)
		}
	}
}

// The order and the priority on the Encoder page (v0.2.19): a drag before or after a job, To
// Top, and a new priority at the end of its class, Realtime too; only a queued job moves.
func TestTheEncoderPageMovesJobsAndChangesTheirPriority(t *testing.T) {
	e := newEnv(t, "")
	for _, id := range []string{"a.x", "b.x", "c.x"} {
		e.rung(id, "stage2")
	}
	e.captions("rec-1.whisper-1", 64<<20)
	order := func(prio string) []string {
		var out []string
		for _, j := range e.s.queued() {
			if j.Priority == prio {
				out = append(out, j.ID)
			}
		}
		return out
	}
	same := func(got []string, want ...string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}
	if _, err := e.s.MoveJob("c.x", Move{To: "top"}); err != nil || !same(order("stage2"), "c.x", "a.x", "b.x") {
		t.Fatalf("To Top: %v %v", order("stage2"), err)
	}
	if _, err := e.s.MoveJob("c.x", Move{After: "b.x"}); err != nil || !same(order("stage2"), "a.x", "b.x", "c.x") {
		t.Fatalf("after b: %v %v", order("stage2"), err)
	}
	// The Prio dropdown: Realtime, at the end of its class, so first come first served holds.
	v, err := e.s.MoveJob("a.x", Move{To: "bottom", Priority: PriorityNow})
	if err != nil || v.Priority != PriorityNow || !same(order(PriorityNow), "rec-1.whisper-1", "a.x") {
		t.Fatalf("to Realtime: %+v %v %v", v.Priority, order(PriorityNow), err)
	}
	if _, err := e.s.MoveJob("b.x", Move{To: "bottom", Priority: "Stufe 1"}); code(err) != "invalid" {
		t.Fatalf("an unknown priority: %v", err)
	}
	// A job a worker holds does not move.
	w := e.worker("mac", reference)
	l := e.claim(w)
	if _, err := e.s.MoveJob(l.Job.ID, Move{To: "top"}); code(err) != "not_queued" {
		t.Fatalf("a leased job: %v", err)
	}
}

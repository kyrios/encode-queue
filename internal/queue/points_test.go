package queue

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

// Points (v0.2.15; Thorsten, 2026-10-28): one point is a minute of the Mac Studio's work, by
// Punkte-Tabelle v1, the same on every machine.

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestPunkteTabelleV1(t *testing.T) {
	rung := func(codec string, w, h int, preset string) *RungParams {
		return &RungParams{Codec: codec, Width: w, Height: h, Preset: preset}
	}
	for _, c := range []struct {
		p   *RungParams
		fps float64 // the Mac Studio's, as the frame's price
	}{
		{rung("libx265", 3840, 2160, "slower"), 2.2835275723167556},
		{rung("libx265", 3840, 2160, "ultrafast"), 48.42392630208087},
		{rung("libx264", 1280, 720, "veryfast"), 271.00387277325575},
		// A 2:1 master's top rung: the 4K class, scaled by the pixels.
		{rung("libx265", 3840, 1920, "slower"), 2.2835275723167556 * 2160 / 1920},
		// The Clip, 1080x1920 at x264 slow: from 1280x720, 2.25 times the pixels.
		{rung("libx264", 1080, 1920, "slow"), 188.31159354449025 / 2.25},
		// 1440p x265 slow is nearer 1080 than 720.
		{rung("libx265", 2560, 1440, "slow"), 29.169180609459037 * 1920 * 1080 / (2560 * 1440)},
	} {
		if got := rungPointsPerFrame(c.p); !near(got, 1/(c.fps*60)) {
			t.Errorf("%s: %v points a frame, want %v", rungClass(c.p), got, 1/(c.fps*60))
		}
	}
	// A codec and preset the table lacks earns nothing.
	if got := rungPointsPerFrame(rung("libx265", 1920, 1080, "medium")); got != 0 {
		t.Errorf("x265 medium: %v", got)
	}
	// A 2-hour 4K slower archive rung at 25 fps: 180000 frames, about 1314 points (22 hours).
	if got := 180000 * rungPointsPerFrame(rung("libx265", 3840, 2160, "slower")); math.Abs(got-1313.7) > 0.1 {
		t.Errorf("the archive rung: %v points", got)
	}
	// Whisper by the model the attempt ran: an hour of turbo is about 4.7 points, of medium (2.5
	// times small's time) about 4.6.
	j := &Job{whisper: &WhisperParams{Models: []string{SHALargeV3Turbo, SHAMedium}}}
	j.Spec.Source.DurationS = 3600
	if got := jobPoints(j, &Lease{Model: SHALargeV3Turbo}); !near(got, 3600/(12.761687001891676*60)) {
		t.Errorf("an hour of turbo: %v", got)
	}
	if got := jobPoints(j, &Lease{Model: SHAMedium}); !near(got, 3600*2.5/(32.336355149617205*60)) {
		t.Errorf("an hour of medium: %v", got)
	}
	// Without the audio's length the queue cannot price it.
	j.Spec.Source.DurationS = 0
	if got := jobPoints(j, &Lease{Model: SHALargeV3Turbo}); got != 0 {
		t.Errorf("whisper without duration_s: %v", got)
	}
	// Cut, remux and check are not in the table.
	if got := jobPoints(&Job{remux: &RemuxParams{}}, &Lease{}); got != 0 {
		t.Errorf("remux: %v", got)
	}
}

// accept delivers a claimed rung and has the pipeline accept it.
func (e *env) accept(wid string, l *LeaseView) {
	e.t.Helper()
	e.result(wid, l.ID, 3000, 100)
	if _, err := e.s.PutVerdict(l.Job.ID, l.Attempt, Verdict{Verdict: VerdictAccepted}); err != nil {
		e.t.Fatal(err)
	}
}

func TestAnAcceptedJobEarnsTheSameOnEveryMachine(t *testing.T) {
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	want := 3000 / (2.2835275723167556 * 60) // 3000 frames of 4K slower: 21.9 points
	for i, w := range []string{enc, mac} {
		id := []string{"a.2160-archive", "b.2160-archive"}[i]
		e.rung(id, "archive")
		e.sized(id, 1e6)
		l := e.claim(w)
		if l == nil || l.Job.ID != id {
			t.Fatalf("%s claimed %+v", w, l)
		}
		e.accept(w, l)
		e.s.st.Workers[enc].LastSeen = nil // the Mac gets the next one
		v := must(e.s.GetWorker(w))
		if !near(v.Points.Today, want) || !near(v.Points.Week, want) || !near(v.Points.Total, want) || v.Points.Table != "v1" {
			t.Fatalf("%s's points: %+v, want %v", v.Name, v.Points, want)
		}
		if a := must(e.s.GetJob(id)).Attempts[0]; !near(a.Points, want) {
			t.Fatalf("the attempt's points: %v", a.Points)
		}
	}
	if v := must(e.s.GetWorker(enc)); !v.House {
		t.Fatal("encoder's own worker has no Haus badge")
	}
	if v := must(e.s.GetWorker(mac)); v.House {
		t.Fatal("the Mac has the Haus badge")
	}
	var b bytes.Buffer
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), `encode_queue_worker_points_total{worker_id="`+mac+`",worker="mac"} 21.89`) {
		t.Fatalf("no points counter for the Mac:\n%s", grepLines(b.String(), "points"))
	}
	// The same table on every machine: a second accept on the same job earns nothing more.
	if _, err := e.s.PutVerdict("b.2160-archive", 1, Verdict{Verdict: VerdictAccepted}); err == nil {
		t.Fatal("accepted twice")
	}
	if v := must(e.s.GetWorker(mac)); !near(v.Points.Total, want) {
		t.Fatalf("points after a second verdict: %v", v.Points.Total)
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestOnlyAnAcceptedResultOfAHeldLeaseEarns(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	other := e.worker("encoder", reference)
	// Rejected.
	e.rung("a.2160-archive", "archive")
	e.sized("a.2160-archive", 1e6)
	l := e.claim(w)
	e.result(w, l.ID, 3000, 100)
	if _, err := e.s.PutVerdict("a.2160-archive", 1, Verdict{Verdict: VerdictRejected}); err != nil {
		t.Fatal(err)
	}
	// Failed.
	e.rung("b.2160-archive", "archive")
	e.sized("b.2160-archive", 1e6)
	l = e.claim(w)
	if _, err := e.s.Deliver(w, l.ID, ResultIn{Outcome: "failed", Code: "ffmpeg", Message: "exit 1"}); err != nil {
		t.Fatal(err)
	}
	// Moved to another worker while its upload ran, and its late result accepted anyway: the
	// work was reassigned.
	e.rung("c.2160-archive", "archive")
	e.sized("c.2160-archive", 1e6)
	l = e.claim(w)
	fid := e.upload(w, l.ID, "file of c")
	if _, err := e.s.AssignJob("c.2160-archive", Assign{WorkerID: other}); err != nil {
		t.Fatal(err)
	}
	if err := e.finish(w, l.ID, fid, "file of c"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.PutVerdict("c.2160-archive", l.Attempt, Verdict{Verdict: VerdictAccepted}); err != nil {
		t.Fatal(err)
	}
	if a := must(e.s.GetJob("c.2160-archive")).Attempts[0]; a.Result == nil || a.Result.Verdict != VerdictAccepted || a.Points != 0 {
		t.Fatalf("the moved attempt: %+v", a)
	}
	if v := must(e.s.GetWorker(w)); v.Points.Total != 0 || v.Points.Today != 0 {
		t.Fatalf("points for nothing accepted while held: %+v", v.Points)
	}
}

func TestPointsByDayAndWeekInBerlin(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	earn := func(id string) {
		e.rung(id, "archive")
		e.sized(id, 1e6)
		e.accept(w, e.claim(w))
	}
	one := 3000 / (2.2835275723167556 * 60)
	// Sunday 1 November 2026, 23:30 in Berlin (CET): the week's last day.
	e.clock.Add(time.Date(2026, 11, 1, 22, 30, 0, 0, time.UTC).Sub(e.clock.Now()))
	e.presence(w)
	earn("a.2160-archive")
	if v := must(e.s.GetWorker(w)).Points; !near(v.Today, one) || !near(v.Week, one) {
		t.Fatalf("Sunday: %+v", v)
	}
	// An hour later it is Monday in Berlin, though still Sunday in UTC: a new day and week.
	e.clock.Add(time.Hour)
	e.presence(w)
	if v := must(e.s.GetWorker(w)).Points; v.Today != 0 || v.Week != 0 || !near(v.Total, one) {
		t.Fatalf("Monday 00:30: %+v", v)
	}
	earn("b.2160-archive")
	// Wednesday: Monday's points are this week's, not today's.
	e.clock.Add(48 * time.Hour)
	e.presence(w)
	if v := must(e.s.GetWorker(w)).Points; v.Today != 0 || !near(v.Week, one) || !near(v.Total, 2*one) {
		t.Fatalf("Wednesday: %+v", v)
	}
	// Three weeks on, the days are gone and the total is kept.
	e.clock.Add(21 * 24 * time.Hour)
	e.presence(w)
	earn("c.2160-archive")
	if l := e.s.st.Workers[w].Points; len(l.Days) != 1 || !near(l.Total, 3*one) {
		t.Fatalf("the ledger: %+v", l)
	}
}

func TestThePresenceSaysTheWorkersPlace(t *testing.T) {
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	vimes := e.worker("vimes", reference)
	e.rung("a.2160-archive", "archive")
	e.sized("a.2160-archive", 1e6)
	e.accept(enc, e.claim(enc))
	ack := must(e.s.PutPresence(enc, Presence{State: "idle"}))
	if p := ack.Points; p.Rank != 1 || p.Of != 3 || p.Today <= 0 || p.Table != "v1" {
		t.Fatalf("encoder's place: %+v", p)
	}
	// Equal points share a place: the Mac and Vimes both have none.
	for _, w := range []string{mac, vimes} {
		if p := must(e.s.PutPresence(w, Presence{State: "idle"})).Points; p.Rank != 2 || p.Of != 3 {
			t.Fatalf("a worker without points: %+v", p)
		}
	}
}

// Points are counted and shown, never scored: a worker with many points gets no job sooner.
func TestPointsDoNotSteerTheScoring(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("h264.2160-archive", "archive")
	e.sized("h264.2160-archive", 400e6)
	e.s.st.Workers[enc].Points = &PointsLedger{Total: 1e9, Days: map[string]float64{e.clock.Now().In(berlin).Format(dayLayout): 1e9}}
	if l := e.claim(enc); l != nil {
		t.Fatalf("encoder took a job the Mac finishes in a third of the time: %+v", l)
	}
	if l, err := e.s.Claim(context.Background(), mac, LeaseRequest{}, 0); err != nil || l == nil {
		t.Fatalf("the Mac: %+v %v", l, err)
	}
}

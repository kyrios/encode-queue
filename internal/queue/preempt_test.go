package queue

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Nearly done is never taken back, and of the rest the lease that ran the shortest goes
// (v0.2.15; Thorsten, 2026-10-28: queue v0.2.12 threw away a 2160 archive encode 95 % done after
// five hours for the smoke test's stage 2 job).

// busyWith gives encoder one archive lease with its progress, and no high-priority slot, so a new
// stage 1 job can only take that lease back.
func busyWith(t *testing.T, p Progress) (*env, string, *LeaseView) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.limits(w, 1, 0)
	e.rung("old.2160-archive", "archive")
	e.sized("old.2160-archive", 1e6) // 3000 frames
	l := e.claim(w)
	if _, err := e.s.Renew(w, l.ID, p); err != nil {
		t.Fatal(err)
	}
	e.rung("new.2160-stage1", "stage1")
	return e, w, l
}

func TestANearlyDoneLeaseIsSpared(t *testing.T) {
	for _, c := range []struct {
		name  string
		p     Progress
		spare bool
	}{
		{"95 % done, as on 2026-10-28", Progress{Phase: "encoding", Fraction: f64(0.95)}, true},
		{"81 % of its frames", Progress{Phase: "encoding", FramesDone: i64(810), FramesTotal: i64(1000)}, true},
		// 1000 of 3000 frames at 2 fps: 16.7 minutes to go.
		{"under 20 minutes to go", Progress{Phase: "encoding", FPS: f64(2), FramesDone: i64(1000), Fraction: f64(0.33)}, true},
		// The same at 1 fps: 33 minutes to go.
		{"a third done, 33 minutes to go", Progress{Phase: "encoding", FPS: f64(1), FramesDone: i64(1000), Fraction: f64(0.33)}, false},
		{"still downloading", Progress{Phase: "downloading"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, w, l := busyWith(t, c.p)
			if got := e.s.preempt(e.clock.Now()); got == c.spare {
				t.Fatalf("preempted %v, want spared %v", got, c.spare)
			}
			_, err := e.s.Renew(w, l.ID, c.p)
			if c.spare && err != nil {
				t.Fatalf("the spared lease: %v", err)
			}
			if !c.spare && code(err) != "preempted" {
				t.Fatalf("the lease: %v", err)
			}
		})
	}
}

func TestTheLeaseThatRanTheShortestGoes(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.limits(enc, 1, 0)
	e.limits(mac, 1, 0)
	// encoder's archive encode has run two hours; the Mac's stage 2 encode one minute.
	e.rung("old.2160-archive", "archive")
	e.sized("old.2160-archive", 1e6)
	archive := e.claim(enc)
	e.s.st.Workers[mac].LastSeen = nil // only encoder may claim the archive job
	e.clock.Add(2 * time.Hour)
	e.presence(enc, mac)
	e.rung("mid.2160-stage2", "stage2")
	e.sized("mid.2160-stage2", 1e6)
	stage2 := e.claim(mac)
	e.clock.Add(time.Minute)
	e.presence(enc, mac)
	for _, x := range []struct {
		w string
		l *LeaseView
	}{{enc, archive}, {mac, stage2}} {
		if _, err := e.s.Renew(x.w, x.l.ID, Progress{Phase: "encoding", FPS: f64(0.1), FramesDone: i64(12), Fraction: f64(0.004)}); err != nil {
			t.Fatal(err)
		}
	}
	var log bytes.Buffer
	e.s.cfg.Log = slog.New(slog.NewTextHandler(&log, nil))
	e.rung("new.2160-stage1", "stage1")
	if !e.s.preempt(e.clock.Now()) {
		t.Fatal("nothing taken back")
	}
	// Before v0.2.15 the lowest priority went first: the two-hour archive encode.
	if _, err := e.s.Renew(enc, archive.ID, Progress{Phase: "encoding"}); err != nil {
		t.Fatalf("the two-hour archive encode: %v", err)
	}
	if _, err := e.s.Renew(mac, stage2.ID, Progress{Phase: "encoding"}); code(err) != "preempted" {
		t.Fatalf("the one-minute stage 2 encode: %v", err)
	}
	if line := log.String(); !strings.Contains(line, "msg=preempt") || !strings.Contains(line, "job=mid.2160-stage2") ||
		!strings.Contains(line, "lost_minutes=1.0") || !strings.Contains(line, "lost_frames=12") {
		t.Fatalf("the preempt line: %s", line)
	}
	var b bytes.Buffer
	e.s.WriteMetrics(&b, "test")
	l := `worker_id="` + mac + `",worker="mac"`
	for _, want := range []string{`encode_queue_worker_preempted_work_seconds_total{` + l + `} 60`, `encode_queue_worker_preempted_frames_total{` + l + `} 12`} {
		if !strings.Contains(b.String(), want+"\n") {
			t.Errorf("metrics lack %s", want)
		}
	}
}

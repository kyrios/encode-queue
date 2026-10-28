package e2e

import (
	"os"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/queue"
	"github.com/kyrios/encode-queue/internal/worker"
)

// Pause (v0.2.18; Thorsten, 2026-10-28: "Pause also wasn't working"), through the tunnel: the
// harness gives a paused lease back after 2 s and 2 s of grace.

// encoding waits until the job's tool has done some frames.
func (h *harness) encoding(id string) {
	h.t.Helper()
	h.waitFor(id+" encoding", 30*time.Second, func() bool {
		p := h.job(id).Progress
		return p != nil && p.Phase == "encoding" && p.FramesDone != nil && *p.FramesDone > 5
	})
}

func (h *harness) workerState(id string) string {
	var w queue.WorkerView
	h.call("GET", "/v1/workers/"+id, adminToken, nil, &w)
	return w.State
}

// "Sofort pausieren" on an archive job: the queue sees it suspended and keeps it past the give-back
// time, its frames stand still, and it goes on where it stood when the pause ends.
func TestAPauseHoldsAnArchiveJobAndItGoesOn(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "300")
	t.Setenv("FAKE_FRAME_MS", "10")
	tw := h.pair("mac")
	h.rungJob("p_1.2160-archive", "archive", map[string]any{"source_frames": 300})
	h.encoding("p_1.2160-archive")
	tw.w.PauseWith(time.Now().Add(time.Hour), worker.PauseNow)
	h.waitFor("the queue to see the job suspended", 5*time.Second, func() bool {
		p := h.job("p_1.2160-archive").Progress
		return h.workerState(tw.id) == "paused" && p != nil && p.Phase == "paused"
	})
	frames := *h.job("p_1.2160-archive").Progress.FramesDone
	time.Sleep(6 * time.Second) // past the give-back time and its grace
	j := h.job("p_1.2160-archive")
	if j.State != "leased" || len(j.Attempts) != 1 || j.Attempts[0].State != "active" || j.Progress.Phase != "paused" {
		t.Fatalf("the held job: %s %+v %+v", j.State, j.Attempts, j.Progress)
	}
	if got := *j.Progress.FramesDone; got != frames {
		t.Fatalf("suspended, the tool went on from %d to %d frames", frames, got)
	}
	if st := tw.w.Status(); st.State != "paused" || st.PauseMode != worker.PauseNow || len(st.Jobs) != 1 || st.Jobs[0].Progress.Phase != "paused" {
		t.Fatalf("the worker's status: %+v", st)
	}
	tw.w.Resume()
	j = h.waitState("p_1.2160-archive", "done", 30*time.Second)
	if len(j.Attempts) != 1 {
		t.Fatalf("attempts after the pause: %+v", j.Attempts)
	}
}

// "Nach diesem Auftrag": the running job finishes at full speed and the queue leaves it alone,
// even a stage 1 job past the give-back time; the next job waits for the pause's end.
func TestAPauseAfterTheJobLetsItFinish(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "600") // 6 s, past the give-back time and its grace
	t.Setenv("FAKE_FRAME_MS", "10")
	tw := h.pair("mac")
	h.rungJob("a_1.2160", "stage1", nil)
	h.encoding("a_1.2160")
	tw.w.PauseWith(time.Now().Add(time.Hour), worker.PauseAfterJob)
	h.waitFor("the queue to see the pause", 5*time.Second, func() bool { return h.workerState(tw.id) == "paused" })
	os.Setenv("FAKE_FRAMES", "5")
	h.rungJob("b_2.2160", "stage1", nil)
	a := h.waitState("a_1.2160", "done", 30*time.Second)
	if len(a.Attempts) != 1 {
		t.Fatalf("the job that ran through the pause: %+v", a.Attempts)
	}
	time.Sleep(1500 * time.Millisecond)
	if b := h.job("b_2.2160"); b.State != "queued" {
		t.Fatalf("a paused worker took the next job: %s", b.State)
	}
	if st := tw.w.Status(); st.PauseMode != worker.PauseAfterJob {
		t.Fatalf("the pause's mode: %q", st.PauseMode)
	}
	tw.w.Resume()
	h.waitState("b_2.2160", "done", 30*time.Second)
}

// A pause that runs out: the suspended job goes on by itself, on its first attempt.
func TestAPauseRunsOut(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "300")
	t.Setenv("FAKE_FRAME_MS", "10")
	tw := h.pair("mac")
	h.rungJob("r_1.2160-archive", "archive", nil)
	h.encoding("r_1.2160-archive")
	tw.w.PauseWith(time.Now().Add(3*time.Second), worker.PauseNow)
	h.waitFor("the job suspended", 5*time.Second, func() bool {
		p := h.job("r_1.2160-archive").Progress
		return p != nil && p.Phase == "paused"
	})
	j := h.waitState("r_1.2160-archive", "done", 30*time.Second)
	if len(j.Attempts) != 1 {
		t.Fatalf("attempts: %+v", j.Attempts)
	}
	if st := tw.w.Status(); st.State == "paused" {
		t.Fatal("still paused after the pause's end")
	}
}

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/worker"
)

// The peaks job end to end (v0.2.22): realtime by default, it starts in the worker's
// high-priority slot within seconds while a long 4K archive encode runs in the regular one, and
// the encode runs on.
func TestPeaksBesideA4KEncode(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "3000") // 30 s of encode
	t.Setenv("FAKE_FRAME_MS", "10")
	h.pairWith("encoder", func(c *worker.Config) { c.Version = "0.2.22" })
	h.rungJob("old_1.2160-archive", "archive", map[string]any{"source_frames": 3000})
	h.waitFor("old_1.2160-archive encoding", 30*time.Second, func() bool {
		p := h.job("old_1.2160-archive").Progress
		return p != nil && p.Phase == "encoding" && p.FramesDone != nil && *p.FramesDone > 0
	})
	src := h.source("rec_7/upload.mp4", "an upload with sound")
	submitted := time.Now()
	h.submit("rec_7.peaks", map[string]any{"kind": "peaks", "params": map[string]any{}, "upload_id": "rec_7",
		"source": map[string]any{"path": src.Path, "bytes": src.Bytes, "sha256": src.SHA256, "container": "mp4", "duration_s": 1},
		"result": map[string]any{"name": "upload.peaks", "max_bytes": 1 << 20}})
	j := h.waitState("rec_7.peaks", "done", 10*time.Second)
	if j.Priority != "now" || len(j.Attempts) != 1 || j.Attempts[0].Worker.Name != "encoder" {
		t.Fatalf("the peaks job: %s %+v", j.Priority, j.Attempts)
	}
	if took := j.Attempts[0].StartedAt.Sub(submitted); took > 3*time.Second {
		t.Errorf("the peaks job started %v after it came", took)
	}
	if got := h.fetch(j, 1); !strings.HasPrefix(got, "FKPK\x01\x05") || len(got) != 16+20+2*(100+25+7+2+1) {
		t.Errorf("the peaks file: %d bytes, % x", len(got), got[:min(len(got), 24)])
	}
	o := h.job("old_1.2160-archive")
	if o.State != "leased" || len(o.Attempts) != 1 || o.Attempts[0].State != "active" {
		t.Fatalf("the archive encode beside it: %s %+v", o.State, o.Attempts)
	}
}

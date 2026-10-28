package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// 2026-10-28: rec-624a5f5a9b0a.whisper-1793212944, priority now, queued for hours with `waiting:
// null`, while encoder's worker sat idle with both slots free and turbo at hand. The Mac Studio's
// worker, 0.2.12, encoded a 2160 archive rung, whose max_bytes the queue held for its result.

// as gives a worker its version, its whisper speeds by model and its RAM; used nil: a presence
// without memory_used_bytes.
func (e *env) as(wid, version string, memory int64, used *int64, speeds map[string]float64) {
	e.t.Helper()
	hw, _ := json.Marshal(map[string]any{"cpu_threads": 6, "memory_bytes": memory})
	cal := &Calibration{ClipSHA256: clipA, Tools: reference[:3], Classes: []CalibratedClass{{Class: hd, Frames: 100, FPS: 100}}}
	var models []Model
	for sha, v := range speeds {
		cal.Whisper = append(cal.Whisper, CalibratedModel{SHA256: sha, Name: catalogue[sha].Name, Whisper: "v1.9.5", AudioS: 21.6, Speed: v})
		models = append(models, Model{Name: catalogue[sha].Name, SHA256: sha})
	}
	if _, err := e.s.PutCapabilities(wid, Capabilities{AppVersion: version, OS: "test", Tools: reference, Models: models,
		Hardware: hw, Calibration: cal}); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.s.PutPresence(wid, Presence{State: "working", MemoryUsed: used}); err != nil {
		e.t.Fatal(err)
	}
}

// captions submits a `now` whisper job for turbo alone, as the pipeline sends a recording's.
func (e *env) captions(id string, maxBytes int64) {
	e.t.Helper()
	f := e.source(id+"/sound.wav", "RIFF "+id)
	params, _ := json.Marshal(map[string]any{"model_sha256": SHALargeV3Turbo, "model": "ggml-large-v3-turbo.bin", "language": "de"})
	if _, _, err := e.s.Submit(id, JobSubmit{Kind: KindWhisper, Priority: PriorityNow, Params: params,
		Source: SourceIn{FileIn: f, Container: "wav", DurationS: 600}, Result: ResultSpec{Name: "whisper.json", MaxBytes: maxBytes}}); err != nil {
		e.t.Fatal(err)
	}
}

// anySlot is what a worker of 0.2.14 or later gets when it asks in its normal slot and then in
// its high-priority one.

// later lets `now`'s max_wait pass, after which any eligible worker takes the job (ect.go): the
// Mac's free extra slot makes it the better worker until then.
func (e *env) later(ids ...string) {
	e.clock.Add(maxWait[PriorityNow] + time.Second)
	e.presence(ids...)
}
func (e *env) anySlot(wid string) *LeaseView {
	e.t.Helper()
	for _, slot := range []string{SlotNormal, SlotHigh} {
		l, err := e.s.Claim(context.Background(), wid, LeaseRequest{Slot: slot}, 0)
		if err != nil {
			e.t.Fatalf("%s slot: %v", slot, err)
		}
		if l != nil {
			return l
		}
	}
	return nil
}

// atNoon is the production scene: encoder, the Mac Studio (0.2.12) on a 2160 archive rung whose
// max_bytes is 22 GiB (half an hour of source at 12.5 MB/s), and Vimes with small only. The disk
// had 30 GiB free when the Mac claimed and has 18 GiB now; the reserve is EQ_RESULT_RESERVE_BYTES's
// 2 GiB.
func atNoon(t *testing.T) (e *env, enc, mac string, free *int64) {
	e = newEnv(t, "encoder")
	enc, mac = e.worker("encoder", reference), e.worker("mac-studio", reference)
	vimes := e.worker("vimes", reference)
	e.as(enc, "0.2.18", 14*gib, ptrInt64(3*gib), map[string]float64{SHASmall: 4, SHALargeV3Turbo: 1})
	e.as(mac, "0.2.12", 64*gib, ptrInt64(20*gib), map[string]float64{SHASmall: 40, SHALargeV3Turbo: 10})
	e.as(vimes, "0.2.18", 16*gib, ptrInt64(3*gib), map[string]float64{SHASmall: 8})
	e.limits(enc, 1, 1)
	e.limits(vimes, 1, 1)
	e.s.cfg.ResultReserve = 2 << 30
	free = new(int64)
	*free = 30 << 30
	e.s.cfg.FreeBytes = func(string) (int64, error) { return *free, nil }
	e.rung("old.2160-archive", "archive")
	e.s.st.Jobs["old.2160-archive"].Spec.Result.MaxBytes = 22 << 30
	if l := e.claim(mac); l == nil || l.Job.ID != "old.2160-archive" {
		t.Fatalf("the Mac's archive lease: %+v", l)
	}
	*free = 18 << 30
	return e, enc, mac, free
}

func ptrInt64(v int64) *int64 { return &v }

func TestANowJobDoesNotWaitForAnArchiveLeasesWorstCase(t *testing.T) {
	e, enc, mac, _ := atNoon(t)
	e.captions("rec-624a5f5a9b0a.whisper-1793212944", 64<<20)
	e.rung("next.2160-archive", "archive")
	e.s.st.Jobs["next.2160-archive"].Spec.Result.MaxBytes = 4 << 30
	// The archive job keeps the whole reservation: 18 GB less 22 and the reserve is no room.
	jv, err := e.s.GetJob("next.2160-archive")
	if err != nil {
		t.Fatal(err)
	}
	if jv.Waiting == nil || jv.Waiting.Code != WaitNoRoom || !strings.Contains(jv.Waiting.Detail, "may take 4.3 GB") {
		t.Fatalf("the archive job's waiting: %+v", jv.Waiting)
	}
	// Before v0.2.19 encoder got nothing, at any time.
	e.later(enc, mac)
	l := e.anySlot(enc)
	if l == nil || l.Job.ID != "rec-624a5f5a9b0a.whisper-1793212944" {
		t.Fatalf("encoder's claim: %+v", l)
	}
	// The archive job still waits for the room, and says so.
	if jv, _ := e.s.GetJob("next.2160-archive"); jv.State != StateQueued || jv.Waiting == nil || jv.Waiting.Code != WaitNoRoom {
		t.Fatalf("the archive job: %s %+v", jv.State, jv.Waiting)
	}
	// A stage 1 rung of a new upload is new before old too.
	e.rung("up1.2160-stage1", "stage1")
	e.s.st.Jobs["up1.2160-stage1"].Spec.Result.MaxBytes = 4 << 30
	if jv, _ := e.s.GetJob("up1.2160-stage1"); jv.Waiting != nil {
		t.Fatalf("the stage 1 job's waiting: %+v", jv.Waiting)
	}
}

func TestAHighPriorityResultStillNeedsTheDiskAsItIs(t *testing.T) {
	e, enc, mac, free := atNoon(t)
	// What the Mac's result still has to write counts in full, and so does the reserve.
	if _, err := e.s.CreateUpload(mac, e.s.activeLeases(mac)[0].ID, 15<<30); err != nil {
		t.Fatal(err)
	}
	*free = 17 << 30
	e.captions("rec-1.whisper-1", 64<<20)
	e.later(enc, mac)
	if l := e.anySlot(enc); l != nil {
		t.Fatalf("encoder took %s with 17 GB free, 15 GB to come and 2 GiB in reserve", l.Job.ID)
	}
	jv, _ := e.s.GetJob("rec-1.whisper-1")
	if jv.Waiting == nil || jv.Waiting.Code != WaitNoRoom {
		t.Fatalf("waiting: %+v", jv.Waiting)
	}
	*free = 18 << 30
	if l := e.anySlot(enc); l == nil {
		t.Fatal("encoder took nothing with room for the result")
	}
	var b bytes.Buffer
	e.s.WriteMetrics(&b, "test")
	for _, want := range []string{`encode_queue_result_room_bytes{for="below_stage1"} `, `encode_queue_result_room_bytes{for="now_stage1"} `} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

// The master's question, 2026-10-28: a `now` job never waits on a worker that cannot run it. The
// Mac Studio's 0.2.12 claims `now` jobs in its extra slot (v0.2.5), which the queue counts as its
// high-priority slot, so it is a better worker for one while its RAM holds the job; then encoder
// waits max_wait at most. Without a RAM figure, its extra slot takes nothing beside its archive
// lease, and encoder takes the job at once.
func TestANowJobNeverWaitsOnAWorkerThatCannotRunIt(t *testing.T) {
	e, enc, mac, free := atNoon(t)
	*free = 1 << 40
	e.s.st.Workers[mac].MemoryUsed = nil
	e.captions("rec-2.whisper-1", 64<<20)
	if l := e.anySlot(enc); l == nil {
		t.Fatal("encoder waited for the Mac, whose extra slot cannot take the job")
	}

	e, enc, mac, free = atNoon(t)
	*free = 1 << 40
	e.captions("rec-3.whisper-1", 64<<20)
	if l := e.anySlot(enc); l != nil {
		t.Fatalf("encoder took the job the Mac's free extra slot runs four times faster: %+v", l.Job.ID)
	}
	e.clock.Add(maxWait[PriorityNow] + time.Second)
	e.presence(enc, mac)
	if l := e.anySlot(enc); l == nil {
		t.Fatal("encoder still waits after max_wait")
	}
}

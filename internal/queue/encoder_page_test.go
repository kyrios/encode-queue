package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func i64(n int64) *int64     { return &n }
func f64(f float64) *float64 { return &f }

func TestETAFromFramesAndFPS(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("a.stage1", "stage1")
	if v, _ := e.s.GetJob("a.stage1"); v.ETAS != nil {
		t.Fatalf("a queued job has eta %d", *v.ETAS)
	}
	l := e.claim(w)
	e.s.Renew(w, l.ID, Progress{Phase: "downloading"})
	if v, _ := e.s.GetJob("a.stage1"); v.ETAS != nil {
		t.Fatalf("eta while downloading: %d", *v.ETAS)
	}
	e.s.Renew(w, l.ID, Progress{Phase: "encoding", FramesDone: i64(1000), FramesTotal: i64(10000), FPS: f64(7)})
	v, _ := e.s.GetJob("a.stage1")
	if v.ETAS == nil || *v.ETAS != 1286 { // 9000 / 7, rounded up
		t.Fatalf("eta %v, want 1286", v.ETAS)
	}
	// Without frames_total the source's frames count.
	e.s.st.Jobs["a.stage1"].Spec.Source.Frames = 2000
	e.s.Renew(w, l.ID, Progress{Phase: "encoding", FramesDone: i64(1000), FPS: f64(10)})
	if v, _ := e.s.GetJob("a.stage1"); v.ETAS == nil || *v.ETAS != 100 {
		t.Fatalf("eta from the source's frames %v, want 100", v.ETAS)
	}
	e.s.Renew(w, l.ID, Progress{Phase: "encoding", FramesDone: i64(1000), FPS: f64(0)})
	if v, _ := e.s.GetJob("a.stage1"); v.ETAS != nil {
		t.Fatalf("eta with fps 0: %d", *v.ETAS)
	}
}

// result delivers a rung result made at fps (frames over encode_s).
func (e *env) result(wid, lid string, frames int64, encodeS float64) {
	e.t.Helper()
	content := "file of " + lid
	fid := e.upload(wid, lid, content)
	sum := sha256.Sum256([]byte(content))
	if _, err := e.s.Deliver(wid, lid, ResultIn{Outcome: "file", File: "/v1/files/" + fid, SHA256: hex.EncodeToString(sum[:]),
		Frames: frames, EncodeS: encodeS, Tools: reference[:3]}); err != nil {
		e.t.Fatal(err)
	}
}

// encode runs one rung job on a worker at fps and delivers it.
func (e *env) encode(wid, id string, fps float64) {
	e.t.Helper()
	e.rung(id, "archive")
	l := e.claim(wid)
	if l == nil || l.Job.ID != id {
		e.t.Fatalf("claimed %+v, not %s", l, id)
	}
	e.result(wid, l.ID, 1000, 1000/fps)
}

func TestRelativeSpeed(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.encode(mac, "m1.archive", 20)
	if v := must(e.s.GetWorker(mac)); v.RelativeSpeed != nil || len(v.SpeedClasses) != 1 ||
		v.SpeedClasses[0].Class != "libx265 3840x2160 slower" || v.SpeedClasses[0].RelativeSpeed != nil {
		t.Fatalf("before the reference: %v %+v", v.RelativeSpeed, v.SpeedClasses)
	}
	e.encode(ref, "r1.archive", 10)
	if v := must(e.s.GetWorker(ref)); v.RelativeSpeed == nil || *v.RelativeSpeed != 1 {
		t.Fatalf("the reference itself: %v", v.RelativeSpeed)
	}
	if v := must(e.s.GetWorker(mac)); v.RelativeSpeed == nil || *v.RelativeSpeed != 2 || *v.SpeedClasses[0].ReferenceFPS != 10 {
		t.Fatalf("2x: %v %+v", v.RelativeSpeed, v.SpeedClasses)
	}
	// The median of the last five: an outlier does not move it, and the oldest drops out.
	for i, fps := range []float64{20, 200, 30, 30, 30} {
		e.encode(mac, fmt.Sprintf("m%d.archive", i+2), fps)
	}
	v := must(e.s.GetWorker(mac))
	if sc := v.SpeedClasses[0]; sc.Samples != 5 || math.Abs(sc.FPS-30) > 1e-9 || math.Abs(*sc.RelativeSpeed-3) > 1e-9 {
		t.Fatalf("the median %+v", sc)
	}
	// Another class: the headline is the median of the classes' ratios.
	e.s.Submit("h.archive", JobSubmit{Kind: KindRung, Priority: "archive", Source: SourceIn{FileIn: e.source("h/master.mov", "h")},
		Params: json.RawMessage(`{"codec":"libx264","preset":"veryfast","crf":20,"width":1920,"height":1080}`), Result: ResultSpec{Name: "h.mp4", MaxBytes: 1 << 20}})
	l := e.claim(mac)
	e.result(mac, l.ID, 1000, 10) // 100 fps
	e.s.Submit("hr.archive", JobSubmit{Kind: KindRung, Priority: "archive", Source: SourceIn{FileIn: e.source("hr/master.mov", "hr")},
		Params: json.RawMessage(`{"codec":"libx264","preset":"veryfast","crf":20,"width":1920,"height":1080}`), Result: ResultSpec{Name: "h.mp4", MaxBytes: 1 << 20}})
	l = e.claim(ref)
	e.result(ref, l.ID, 1000, 20) // 50 fps: the mac is 2x here
	v = must(e.s.GetWorker(mac))
	if v.SpeedClasses[0].Class != "libx264 1920x1080 veryfast" || math.Abs(*v.RelativeSpeed-2.5) > 1e-9 { // median of 2 and 3
		t.Fatalf("headline %v classes %+v", *v.RelativeSpeed, v.SpeedClasses)
	}
	var b strings.Builder
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), `encode_queue_worker_relative_speed{worker_id="`+mac+`",worker="mac"} 2.5`) && !strings.Contains(b.String(), `worker="mac"} 2.4999`) {
		t.Fatalf("no relative speed series:\n%s", b.String())
	}
	e.reopen()
	if v := must(e.s.GetWorker(mac)); v.RelativeSpeed == nil || math.Abs(*v.RelativeSpeed-2.5) > 1e-9 {
		t.Fatalf("after a restart: %v", v.RelativeSpeed)
	}
}

func TestForecastAndCandidates(t *testing.T) {
	e := newEnv(t, "encoder")
	ref := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.encode(ref, "r1.archive", 10)
	e.encode(mac, "m1.archive", 20)
	now := e.clock.Now()
	frames := func(id string, n int64) { e.s.st.Jobs[id].Spec.Source.Frames = n }
	// The encoder runs a: 600 frames left at 10 fps, 60 s.
	e.rung("a.stage1", "stage1")
	frames("a.stage1", 1000)
	la := e.claim(ref)
	e.s.Renew(ref, la.ID, Progress{Phase: "encoding", FramesDone: i64(400), FPS: f64(10)})
	// b and c wait: 2000 frames each, 200 s on the encoder, 100 s on the mac.
	e.rung("b.stage2", "stage2")
	frames("b.stage2", 2000)
	e.rung("c.stage2", "stage2")
	frames("c.stage2", 2000)
	e.rung("w.archive", "archive") // no frames: no estimate
	list := e.s.ListJobs(JobFilter{})
	at := map[string]*time.Time{}
	for _, j := range list.Items {
		at[j.ID] = j.FinishAt
	}
	want := map[string]time.Duration{"a.stage1": 60 * time.Second, "b.stage2": 100 * time.Second, "c.stage2": 200 * time.Second}
	for id, d := range want {
		if at[id] == nil || !at[id].Equal(now.Add(d)) {
			t.Errorf("%s finishes %v, want now+%v", id, at[id], d)
		}
	}
	if at["w.archive"] != nil {
		t.Errorf("a job without frames has an estimate: %v", at["w.archive"])
	}
	if d := must(e.s.GetJob("r1.archive")); d.FinishAt != nil { // checking: not finished, no active lease
		t.Errorf("a result waiting has %v", d.FinishAt)
	}
	// The encoder's free_at is when a finishes.
	if v := must(e.s.GetWorker(ref)); v.FreeAt == nil || !v.FreeAt.Equal(now.Add(60*time.Second)) {
		t.Errorf("free_at %v", v.FreeAt)
	}
	// The move menu for a: the encoder runs it now; the mac would start now and take 50 s.
	c := must(e.s.JobCandidates("a.stage1"))
	if len(c.Items) != 2 {
		t.Fatalf("%d candidates", len(c.Items))
	}
	enc, m := c.Items[0], c.Items[1]
	if !enc.Current || enc.FinishAt == nil || !enc.FinishAt.Equal(now.Add(60*time.Second)) {
		t.Errorf("the encoder %+v", enc)
	}
	if !m.Available || m.Current || m.FreeAt == nil || !m.FreeAt.Equal(now) || *m.EncodeS != 50 || !m.FinishAt.Equal(now.Add(50*time.Second)) || *m.RelativeSpeed != 2 {
		t.Errorf("the mac %+v", m)
	}
	// For b, the encoder is free when a finishes.
	c = must(e.s.JobCandidates("b.stage2"))
	if enc := c.Items[0]; enc.Current || !enc.FreeAt.Equal(now.Add(60*time.Second)) || !enc.FinishAt.Equal(now.Add(260*time.Second)) {
		t.Errorf("the encoder for b %+v", enc)
	}
	// A paused worker is greyed with its reason.
	until := now.Add(time.Hour)
	up := &until
	e.s.PatchWorker(mac, WorkerPatch{PausedUntil: &up})
	c = must(e.s.JobCandidates("b.stage2"))
	if m := c.Items[1]; m.Available || m.ReasonCode == nil || *m.ReasonCode != "worker_paused" || m.FinishAt != nil {
		t.Errorf("the paused mac %+v", m)
	}
}

func TestUnassign(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.rung("a.archive", "archive")
	e.s.AssignJob("a.archive", Assign{WorkerID: mac})
	if l := e.claim(enc); l != nil {
		t.Fatal("a pinned job went to another worker")
	}
	v, err := e.s.UnassignJob("a.archive")
	if err != nil || v.PinnedTo != nil {
		t.Fatalf("unassign %+v %v", v.PinnedTo, err)
	}
	if l := e.claim(enc); l == nil || l.Job.ID != "a.archive" {
		t.Fatalf("after unassign %+v", l)
	}
	if _, err := e.s.UnassignJob("a.archive"); err != nil {
		t.Fatalf("no pin: %v", err)
	}
}

func TestLiveTransferRate(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("a.stage1", "stage1")
	l := e.claim(w)
	e.s.Renew(w, l.ID, Progress{Phase: "downloading", BytesDone: i64(0)})
	e.clock.Add(10 * time.Second)
	e.s.Renew(w, l.ID, Progress{Phase: "downloading", BytesDone: i64(50 << 20)})
	if d := must(e.s.GetWorker(w)).Transfer.Download; d.CurrentBps != 5<<20 || d.Samples != 0 {
		t.Fatalf("live %+v", d)
	}
	e.clock.Add(2 * time.Minute)
	e.presence(w)
	if d := must(e.s.GetWorker(w)).Transfer.Download; d.CurrentBps != 0 {
		t.Fatalf("stale live rate %+v", d)
	}
}

func TestTransferRates(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.s.NoteTransfer(w, DirDownload, 1000, time.Second) // too small to count
	if v, _ := e.s.GetWorker(w); v.Transfer.Download.Samples != 0 || v.Transfer.Download.AvgBps != nil {
		t.Fatalf("a small request counted: %+v", v.Transfer.Download)
	}
	e.s.NoteTransfer(w, DirDownload, 100<<20, 2*time.Second)
	e.s.NoteTransfer(w, DirDownload, 100<<20, time.Second)
	d := must(e.s.GetWorker(w)).Transfer.Download
	if d.Samples != 2 || *d.LastBps != 100<<20 || *d.AvgBps != 50<<20+0.25*(50<<20) {
		t.Fatalf("download %+v (last %v avg %v)", d, *d.LastBps, *d.AvgBps)
	}
	e.s.NoteTransfer(w, DirUpload, 8<<20, time.Second)
	var b strings.Builder
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), `encode_queue_worker_transfer_bytes_per_second{worker_id="`+w+`",worker="encoder",direction="upload"} 8.388608e+06`) {
		t.Fatalf("no upload series:\n%s", b.String())
	}
}

func TestWorkerSystemAndEndpoint(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	hw := `{"chip":"Apple M2 Max","cpus":12,"cpu_cores":12,"cpu_threads":12,"memory_bytes":34359738368,"os_name":"macOS","os_version":"15.6","arch":"arm64","kernel":"24.6.0"}`
	if _, err := e.s.PutCapabilities(w, Capabilities{AppVersion: "0.2.0", OS: "macOS 15.6", Tools: reference, Hardware: json.RawMessage(hw)}); err != nil {
		t.Fatal(err)
	}
	threads := 8
	e.s.PatchMe(w, WorkerPatch{Limits: &Limits{CPUThreads: &threads, MaxParallelJobs: 1}})
	pub := e.s.st.Workers[w].PublicKey
	hs := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	e.s.PeerStatus = func() map[string]PeerStatus {
		return map[string]PeerStatus{pub: {Endpoint: "203.0.113.7:51820", LastHandshake: hs}}
	}
	v := must(e.s.GetWorker(w))
	sys := v.System
	if *sys.CPUModel != "Apple M2 Max" || *sys.CPUCores != 12 || *sys.CPUThreads != 12 || *sys.ThreadsInUse != 8 ||
		*sys.MemoryBytes != 34359738368 || *sys.OSName != "macOS" || *sys.OSVersion != "15.6" || *sys.Arch != "arm64" || *sys.Kernel != "24.6.0" {
		t.Fatalf("system %+v", sys)
	}
	if v.Endpoint == nil || *v.Endpoint != "203.0.113.7:51820" || v.LastHandshake == nil || !v.LastHandshake.Equal(hs) {
		t.Fatalf("endpoint %v handshake %v", v.Endpoint, v.LastHandshake)
	}
	// An older worker's hardware: only cpus, no limit.
	old := e.worker("old", reference)
	e.s.PutCapabilities(old, Capabilities{AppVersion: "0.1.0", OS: "test", Tools: reference, Hardware: json.RawMessage(`{"cpus":4}`)})
	ov := must(e.s.GetWorker(old))
	if *ov.System.CPUThreads != 4 || *ov.System.ThreadsInUse != 4 || ov.System.CPUModel != nil || ov.Endpoint != nil {
		t.Fatalf("an older worker %+v %v", ov.System, ov.Endpoint)
	}
	var b strings.Builder
	e.s.WriteMetrics(&b, "test")
	for _, want := range []string{
		`encode_queue_worker_info{worker_id="` + w + `",worker="mac",app_version="0.2.0",os="macOS 15.6",cpu_model="Apple M2 Max",arch="arm64",host_app="",host_app_version=""} 1`,
		`encode_queue_worker_cpu_threads_in_use{worker_id="` + w + `",worker="mac"} 8`,
		`encode_queue_worker_last_handshake_timestamp_seconds{worker_id="` + w + `",worker="mac"} ` + fmt.Sprint(float64(hs.Unix())),
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("no %s in\n%s", want, b.String())
		}
	}
}

func TestAssignMovesAndPins(t *testing.T) {
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.rung("a.archive", "archive")
	e.rung("b.archive", "archive")
	la := e.claim(enc) // the encoder holds a
	// a moves to the mac: the encoder's attempt is preempted, the job waits for the mac.
	v, err := e.s.AssignJob("a.archive", Assign{WorkerID: mac})
	if err != nil || v.State != StateQueued || v.PinnedTo == nil || v.PinnedTo.Name != "mac" {
		t.Fatalf("assign: %+v %v", v.PinnedTo, err)
	}
	if _, err := e.s.Renew(enc, la.ID, Progress{Phase: "encoding"}); code(err) != "preempted" {
		t.Fatalf("the old worker: %v", err)
	}
	if v.Attempts[0].EndCode == nil || *v.Attempts[0].EndCode != "assigned" {
		t.Fatalf("end code %v", v.Attempts[0].EndCode)
	}
	// The encoder does not get it back; it gets b.
	if l := e.claim(enc); l == nil || l.Job.ID != "b.archive" {
		t.Fatalf("the encoder claimed %+v", l)
	}
	// Preemption leaves a pinned job alone.
	e.s.Tick()
	// The mac takes it before anything else, and the pin is gone.
	e.rung("c.stage1", "stage1")
	if l := e.claim(mac); l == nil || l.Job.ID != "a.archive" {
		t.Fatalf("the mac claimed %+v", l)
	}
	if v := must(e.s.GetJob("a.archive")); v.PinnedTo != nil || v.State != StateLeased {
		t.Fatalf("after the lease: %+v %s", v.PinnedTo, v.State)
	}
	// Moving to the worker that holds it changes nothing.
	if v, err := e.s.AssignJob("a.archive", Assign{WorkerID: mac}); err != nil || v.PinnedTo != nil {
		t.Fatalf("assign to the holder: %+v %v", v.PinnedTo, err)
	}
}

func TestAssignRefusals(t *testing.T) {
	e := newEnv(t, "encoder")
	e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	odd := e.worker("odd", []Tool{{"ffmpeg", "8.0"}, {"libx265", "4.1+1-1d117be"}, {"libx264", "core 164 r3108 31e19f9"}})
	e.rung("a.stage1", "stage1")
	if _, err := e.s.AssignJob("a.stage1", Assign{WorkerID: odd}); code(err) != "worker_mismatch" {
		t.Fatalf("tools differ: %v", err)
	}
	if _, err := e.s.AssignJob("a.stage1", Assign{WorkerID: "wk_nonexist"}); code(err) != "not_found" {
		t.Fatalf("no worker: %v", err)
	}
	until := e.clock.Now().Add(time.Hour)
	up := &until
	e.s.PatchWorker(mac, WorkerPatch{PausedUntil: &up})
	if _, err := e.s.AssignJob("a.stage1", Assign{WorkerID: mac}); code(err) != "worker_paused" {
		t.Fatalf("paused: %v", err)
	}
	var none *time.Time
	e.s.PatchWorker(mac, WorkerPatch{PausedUntil: &none})
	e.clock.Add(5 * time.Minute) // nobody called: everyone is offline
	if _, err := e.s.AssignJob("a.stage1", Assign{WorkerID: mac}); code(err) != "worker_offline" {
		t.Fatalf("offline: %v", err)
	}
	e.presence(mac)
	e.s.CancelJob("a.stage1")
	if _, err := e.s.AssignJob("a.stage1", Assign{WorkerID: mac}); code(err) != "not_movable" {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestPinEndsWhenTheTargetLeaves(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.rung("a.archive", "archive")
	if _, err := e.s.AssignJob("a.archive", Assign{WorkerID: mac}); err != nil {
		t.Fatal(err)
	}
	if l := e.claim(enc); l != nil {
		t.Fatalf("the encoder got a job pinned to the mac: %s", l.Job.ID)
	}
	e.clock.Add(2 * time.Minute)
	e.presence(enc) // the mac stays silent and goes offline
	e.s.Tick()
	if v := must(e.s.GetJob("a.archive")); v.PinnedTo != nil {
		t.Fatalf("the pin outlived its offline worker")
	}
	if l := e.claim(enc); l == nil || l.Job.ID != "a.archive" {
		t.Fatalf("after the pin ended the encoder claimed %+v", l)
	}
}

// Whatever the job and the workers, a candidate that is not available says why.
func TestUnavailableCandidatesSayWhy(t *testing.T) {
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	e.worker("odd", []Tool{{"ffmpeg", "8.0"}, {"libx265", "4.0"}, {"libx264", "core 1"}})
	// Paired, but its first hello is still pending.
	if _, err := e.s.AddWorker(WorkerCreate{Name: "new", PublicKey: key(9)}); err != nil {
		t.Fatal(err)
	}
	check := func(id string, wantAvailable map[string]bool, wantCode map[string]string) {
		t.Helper()
		c := must(e.s.JobCandidates(id))
		for _, it := range c.Items {
			if it.Available != wantAvailable[it.Worker.Name] {
				t.Errorf("%s on %s: available %v", id, it.Worker.Name, it.Available)
			}
			if !it.Available && (it.ReasonCode == nil || it.Reason == nil || *it.Reason == "") {
				t.Errorf("%s on %s: unavailable without a reason %+v", id, it.Worker.Name, it)
				continue
			}
			if want := wantCode[it.Worker.Name]; want != "" && *it.ReasonCode != want {
				t.Errorf("%s on %s: reason_code %s, want %s", id, it.Worker.Name, *it.ReasonCode, want)
			}
		}
	}
	e.rung("a.stage1", "stage1")
	check("a.stage1", map[string]bool{"encoder": true}, map[string]string{"odd": "worker_mismatch", "new": "worker_offline"})
	l := e.claim(enc)
	if err := e.deliver(enc, l.ID, "file"); err != nil {
		t.Fatal(err)
	}
	all := map[string]string{"encoder": "not_movable", "odd": "not_movable", "new": "not_movable"}
	check("a.stage1", nil, all) // checking
	e.s.PutVerdict("a.stage1", 1, Verdict{Verdict: VerdictAccepted})
	check("a.stage1", nil, all) // done
}

func TestHostApp(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.s.cfg.MinAppVersion = "0.2.0"
	// host_app's version is not the worker's: 1.0.3 passes, and an old one does not fail.
	if _, err := e.s.PutCapabilities(w, Capabilities{AppVersion: "0.2.1", OS: "macOS 26.1", Tools: reference,
		HostApp: &HostApp{Name: "Slipstream", Version: "0.0.1"}}); err != nil {
		t.Fatal(err)
	}
	v := must(e.s.GetWorker(w))
	if v.HostApp == nil || v.HostApp.Name != "Slipstream" || v.HostApp.Version != "0.0.1" || *v.AppVersion != "0.2.1" {
		t.Fatalf("host_app %+v app_version %v", v.HostApp, v.AppVersion)
	}
	var b strings.Builder
	e.s.WriteMetrics(&b, "test")
	if !strings.Contains(b.String(), `host_app="Slipstream",host_app_version="0.0.1"} 1`) {
		t.Fatalf("no host_app labels:\n%s", b.String())
	}
	if _, err := e.s.PutCapabilities(w, Capabilities{AppVersion: "0.1.0", OS: "x", Tools: reference,
		HostApp: &HostApp{Name: "Slipstream", Version: "9.9.9"}}); code(err) != "app_too_old" {
		t.Fatalf("the worker's own version still counts: %v", err)
	}
	e.reopen()
	if v := must(e.s.GetWorker(w)); v.HostApp == nil || v.HostApp.Name != "Slipstream" {
		t.Fatalf("after a restart %+v", v.HostApp)
	}
}

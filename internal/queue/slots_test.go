package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Normal and high-priority slots (v0.2.14; Thorsten, 2026-10-28, after a `now` whisper job waited
// over 40 minutes behind a 2160 archive encode).

// limits sets a worker's slots as a worker of 0.2.14 says them.
func (e *env) limits(wid string, normal, high int) {
	e.t.Helper()
	if _, err := e.s.PatchMe(wid, WorkerPatch{Limits: &Limits{MaxParallelJobs: normal, HighPrioritySlots: &high}}); err != nil {
		e.t.Fatal(err)
	}
}

// high claims in one of the worker's high-priority slots.
func (e *env) high(wid string) (*LeaseView, error) {
	return e.s.Claim(context.Background(), wid, LeaseRequest{Slot: SlotHigh}, 0)
}

// smallRung submits a 720p rung, whose peak is 1 GB.
func (e *env) smallRung(id, prio string) {
	e.t.Helper()
	f := e.source(strings.Split(id, ".")[0]+"/master.mov", "master of "+id)
	if _, _, err := e.s.Submit(id, JobSubmit{Kind: KindRung, Priority: prio,
		Params: json.RawMessage(`{"rung":"720","codec":"libx264","preset":"veryfast","crf":23,"width":1280,"height":720,"audio":{"mode":"copy"}}`),
		Source: SourceIn{FileIn: f, Width: 1280, Height: 720}, Result: ResultSpec{Name: "720.mp4", MaxBytes: 1 << 30}}); err != nil {
		e.t.Fatal(err)
	}
}

func TestTheHighPrioritySlotsTakeOnlyNowAndStage1(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.machine(w, 64*gib, 8*gib)
	e.limits(w, 1, 2)
	e.rung("a.2160-archive", "archive")
	if l := e.claim(w); l == nil || l.Job.ID != "a.2160-archive" {
		t.Fatalf("the normal slot: %+v", l)
	}
	e.rung("b.2160-archive", "archive")
	e.rung("c.2160-stage2", "stage2")
	if l, err := e.high(w); err != nil || l != nil {
		t.Fatalf("a high-priority slot took an archive or stage 2 job: %+v %v", l, err)
	}
	e.rung("d.2160-stage1", "stage1")
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	for _, want := range []string{"k3v9x2m1q7pz.whisper", "d.2160-stage1"} {
		if l, err := e.high(w); err != nil || l == nil || l.Job.ID != want {
			t.Fatalf("a high-priority slot: %+v %v, want %s", l, err, want)
		}
	}
	if _, err := e.high(w); code(err) != "limit_reached" || !strings.Contains(err.Error(), "high-priority slots (2) are taken") {
		t.Fatalf("a third high-priority lease: %v", err)
	}
	if _, err := e.s.Claim(context.Background(), w, LeaseRequest{}, 0); code(err) != "limit_reached" {
		t.Fatalf("a second normal lease: %v", err)
	}
	// The archive and stage 2 jobs wait for the normal slot.
	if e.state("b.2160-archive") != StateQueued || e.state("c.2160-stage2") != StateQueued {
		t.Fatal("a lower job left the queue")
	}
	v := must(e.s.GetWorker(w))
	if v.Slots != (WorkerSlots{Normal: SlotCount{1, 1}, High: SlotCount{2, 2}}) {
		t.Fatalf("slots %+v", v.Slots)
	}
	slot := map[string]string{}
	for _, j := range v.Jobs {
		slot[j.JobID] = j.Slot
		if j.Extra != (j.Slot == SlotHigh) {
			t.Fatalf("extra and slot disagree: %+v", j)
		}
	}
	if slot["a.2160-archive"] != SlotNormal || slot["d.2160-stage1"] != SlotHigh || slot["k3v9x2m1q7pz.whisper"] != SlotHigh {
		t.Fatalf("the jobs' slots: %v", slot)
	}
	var b bytes.Buffer
	e.s.WriteMetrics(&b, "test")
	l := `worker_id="` + w + `",worker="mac"`
	for _, want := range []string{
		`encode_queue_worker_slots{` + l + `,slot="normal"} 1`, `encode_queue_worker_slots{` + l + `,slot="high"} 2`,
		`encode_queue_worker_slots_busy{` + l + `,slot="normal"} 1`, `encode_queue_worker_slots_busy{` + l + `,slot="high"} 2`,
	} {
		if !strings.Contains(b.String(), want+"\n") {
			t.Errorf("metrics lack %s", want)
		}
	}
}

func TestTheSlotsAWorkerAsksForAreCapped(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	// A worker before 0.2.14 says nothing of them: its one extra slot.
	if n := e.s.highSlots(e.s.st.Workers[w]); n != 1 {
		t.Fatalf("an old worker's high-priority slots: %d", n)
	}
	bad := -1
	if _, err := e.s.PatchMe(w, WorkerPatch{Limits: &Limits{MaxParallelJobs: 1, HighPrioritySlots: &bad}}); code(err) != "invalid" {
		t.Fatalf("high_priority_slots -1: %v", err)
	}
	e.limits(w, 1, 100)
	if n := e.s.highSlots(e.s.st.Workers[w]); n != 2 {
		t.Fatalf("100 high-priority slots give %d, want the queue's cap of 2", n)
	}
	e.limits(w, 1, 0)
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if _, err := e.high(w); code(err) != "limit_reached" || !strings.Contains(err.Error(), "no high-priority slot") {
		t.Fatalf("a high-priority claim without one: %v", err)
	}
	if _, err := e.s.Claim(context.Background(), w, LeaseRequest{Slot: "extra"}, 0); code(err) != "invalid" {
		t.Fatalf("slot extra: %v", err)
	}
	// The normal slot still takes it.
	if l := e.claim(w); l == nil || l.Job.ID != "k3v9x2m1q7pz.whisper" || e.s.st.Leases[l.ID].High {
		t.Fatalf("the normal slot: %+v", l)
	}
}

func TestAnOldWorkersExtraSlotTakesStage1Too(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference) // no high_priority_slots: one, its extra slot
	e.machine(w, 64*gib, 8*gib)
	e.rung("a.2160-archive", "archive")
	e.claim(w)
	e.rung("b.2160-stage2", "stage2")
	e.rung("c.2160-stage1", "stage1")
	if x, err := e.extra(w); err != nil || x == nil || x.Job.ID != "c.2160-stage1" || !e.s.st.Leases[x.ID].High {
		t.Fatalf("the extra slot: %+v %v", x, err)
	}
}

// The incident of 2026-10-28: encoder (8 GB) encodes a 2160 archive rung at `slower` in its one
// slot; captions at priority now, pinned to whisper medium, can run nowhere else (the Mac holds
// only large-v3-turbo). The RAM keeps them out of encoder's free high-priority slot, so they take
// the archive job back.
func TestANowJobTakesTheArchiveJobBackForTheRAM(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.machine(enc, 8*gib, 2*gib)
	e.machine(mac, 64*gib, 8*gib)
	e.holds(mac, SHALargeV3Turbo)
	e.limits(enc, 1, 1)
	e.limits(mac, 1, 1)
	e.rung("old.2160-archive", "archive")
	arch := e.claim(enc)
	if arch == nil {
		t.Fatal("no archive encode")
	}
	e.clock.Add(time.Second)
	used := 53 * gib / 10 // the encode in use: 8 - 5.3 - 1 GB headroom < 2.5 GB
	e.s.PutPresence(enc, Presence{State: "working", MemoryUsed: &used})
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	// A whisper job pinned to medium (model_sha256 alone) runs only where medium is.
	if why := e.s.mismatch(e.s.st.Workers[mac], e.s.st.Jobs["k3v9x2m1q7pz.whisper"]); why != "model "+SHAMedium[:12]+" missing" {
		t.Fatalf("the Mac with turbo: %q", why)
	}
	if x, err := e.high(enc); err != nil || x != nil {
		t.Fatalf("a high-priority lease without the RAM: %+v %v", x, err)
	}
	if !e.s.preempt(e.clock.Now()) {
		t.Fatal("the now job took nothing back")
	}
	if v := must(e.s.GetJob("old.2160-archive")); v.State != StateQueued || v.Attempts[0].State != LeasePreempted {
		t.Fatalf("the archive job: %s %+v", v.State, v.Attempts)
	}
	// Until encoder's next presence its memory still counts the encode, whose tool runs until
	// the worker's next renewal: neither slot takes the job, and nothing more is taken back.
	if x, err := e.high(enc); err != nil || x != nil {
		t.Fatalf("a high-priority lease beside the stopping encode: %+v %v", x, err)
	}
	if x, err := e.s.Claim(context.Background(), enc, LeaseRequest{}, 0); err != nil || x != nil {
		t.Fatalf("a normal lease beside the stopping encode: %+v %v", x, err)
	}
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("a second preemption before the RAM was measured")
	}
	e.clock.Add(30 * time.Second)
	used = 12 * gib / 10
	e.s.PutPresence(enc, Presence{State: "idle", MemoryUsed: &used})
	e.clock.Add(time.Second)
	if x, err := e.high(enc); err != nil || x == nil || x.Job.ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("the now job after the encode stopped: %+v %v", x, err)
	}
	// The archive job waits beside it: a normal claim beside a high-priority job needs the RAM,
	// and 8 - 1.2 - 2.5 (claimed since the presence) - 1 < 5.5.
	if x, err := e.s.Claim(context.Background(), enc, LeaseRequest{}, 0); err != nil || x != nil {
		t.Fatalf("the archive encode beside the captions: %+v %v", x, err)
	}
}

// An archive encode claimed after encoder's last presence is in no measured figure, only its
// expected peak is. Taken back, its tool runs until the worker's next renewal: the peak still
// counts until a presence measures the machine again (Codex on #21).
func TestATakenBackJobsRAMCountsUntilMeasured(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	e.machine(enc, 8*gib, 2*gib)
	e.limits(enc, 1, 1)
	e.clock.Add(time.Second)
	e.rung("old.2160-archive", "archive")
	if l := e.claim(enc); l == nil {
		t.Fatal("no archive encode")
	}
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	// 8 - 2 - 5.5 (claimed since the presence) - 1 < 2.5.
	if x, err := e.high(enc); err != nil || x != nil {
		t.Fatalf("captions beside the encode: %+v %v", x, err)
	}
	if !e.s.preempt(e.clock.Now()) {
		t.Fatal("nothing taken back")
	}
	if x, err := e.high(enc); err != nil || x != nil {
		t.Fatalf("captions while the taken-back encode may still run: %+v %v", x, err)
	}
	e.clock.Add(30 * time.Second)
	used := 12 * gib / 10
	e.s.PutPresence(enc, Presence{State: "idle", MemoryUsed: &used})
	e.clock.Add(time.Second)
	if x, err := e.high(enc); err != nil || x == nil || x.Job.ID != "k3v9x2m1q7pz.whisper" {
		t.Fatalf("captions once the encode is measured gone: %+v %v", x, err)
	}
}

// One lease at a time: until the RAM is measured again, a second lower job is not taken back.
func TestANowJobTakesOneJobBackAtATime(t *testing.T) {
	e := newEnv(t, "")
	mac := e.worker("mac", reference)
	e.machine(mac, 16*gib, 2*gib)
	e.limits(mac, 2, 1)
	e.rung("a.2160-archive", "archive")
	e.rung("b.2160-archive", "archive")
	e.claim(mac)
	e.clock.Add(time.Second)
	e.claim(mac)
	e.clock.Add(time.Second)
	used := 13 * gib // 16 - 13 - 1 < 2.5
	e.s.PutPresence(mac, Presence{State: "working", MemoryUsed: &used})
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if !e.s.preempt(e.clock.Now()) {
		t.Fatal("nothing taken back")
	}
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("a second job taken back before the RAM was measured")
	}
	if e.state("a.2160-archive") != StateLeased || e.state("b.2160-archive") != StateQueued {
		t.Fatalf("the youngest archive job goes: a %s, b %s", e.state("a.2160-archive"), e.state("b.2160-archive"))
	}
}

func TestANowJobTakesNothingBackWithoutAFreeHighPrioritySlot(t *testing.T) {
	setup := func(high int) (*env, string) {
		e := newEnv(t, "")
		enc := e.worker("encoder", reference)
		e.machine(enc, 8*gib, 2*gib)
		e.limits(enc, 1, high)
		e.rung("old.2160-archive", "archive")
		e.claim(enc)
		e.clock.Add(time.Second)
		used := 53 * gib / 10
		e.s.PutPresence(enc, Presence{State: "working", MemoryUsed: &used})
		return e, enc
	}
	// No high-priority slot at all: it waits first in line, as Sofort did before 0.2.14.
	e, _ := setup(0)
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("preempted without a high-priority slot")
	}
	// The high-priority slot holds a stage 1 job: nothing is taken back from it, nor from the
	// archive job.
	e, enc := setup(1)
	e.machine(enc, 64*gib, 8*gib)
	e.smallRung("s.720-stage1", "stage1")
	if x, err := e.high(enc); err != nil || x == nil {
		t.Fatalf("stage 1 in the high-priority slot: %+v %v", x, err)
	}
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("preempted while the high-priority slot is taken")
	}
	// Other programs hold the RAM: taking the job back would not free enough.
	e, enc = setup(1)
	e.s.st.Jobs["old.2160-archive"].Final = ""
	e.s.st.Jobs["old.2160-archive"].rung.Height, e.s.st.Jobs["old.2160-archive"].rung.Width = 720, 1280 // a 1 GB peak
	used := 79 * gib / 10
	e.s.PutPresence(enc, Presence{State: "working", MemoryUsed: &used})
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("preempted for RAM it would not free")
	}
}

// A stage 2 job that only the RAM beside a high-priority job keeps off a free normal slot waits
// for it, and takes no other worker's archive job back.
func TestALowerJobWaitsForTheHighPriorityJobsRAM(t *testing.T) {
	e := newEnv(t, "")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.machine(enc, 8*gib, 2*gib)
	e.machine(mac, 64*gib, 8*gib)
	e.limits(enc, 1, 1)
	e.limits(mac, 1, 0)
	e.rung("a.2160-archive", "archive")
	if l := e.claim(mac); l == nil {
		t.Fatal("the Mac took no archive job")
	}
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if x, err := e.high(enc); err != nil || x == nil {
		t.Fatalf("the captions on encoder: %+v %v", x, err)
	}
	e.clock.Add(time.Second)
	used := 45 * gib / 10
	e.s.PutPresence(enc, Presence{State: "working", MemoryUsed: &used})
	e.rung("b.2160-stage2", "stage2")
	if x, err := e.s.Claim(context.Background(), enc, LeaseRequest{}, 0); err != nil || x != nil {
		t.Fatalf("a 4K encode beside the captions on 8 GB: %+v %v", x, err)
	}
	if e.s.preempt(e.clock.Now()) {
		t.Fatal("the stage 2 job took the Mac's archive job back")
	}
}

// A high-priority job does not wait for a worker busy with a lower job, however fast (v0.2.14:
// "a high-priority job never waits for an archive job").
func TestAHighPriorityJobWaitsForNoArchiveJob(t *testing.T) {
	e, enc, mac := scored(t)
	e.limits(mac, 1, 0)
	e.limits(enc, 2, 0)
	e.rung("a.2160-archive", "archive")
	e.sized("a.2160-archive", 400e6)
	l := e.claim(mac)
	if l == nil {
		t.Fatal("the Mac took no archive job")
	}
	// A minute left on the Mac's archive encode.
	if _, err := e.s.Renew(mac, l.ID, Progress{Phase: "encoding", FPS: f64(12.5), FramesDone: i64(3000 - 750)}); err != nil {
		t.Fatal(err)
	}
	e.rung("b.2160-stage2", "stage2")
	e.sized("b.2160-stage2", 400e6)
	e.rung("c.2160-stage1", "stage1")
	e.sized("c.2160-stage1", 400e6)
	// encoder needs 12 min for the stage 1 job, the Mac 1 + 4 once its archive job is done: it
	// takes it anyway.
	x := e.claim(enc)
	if x == nil || x.Job.ID != "c.2160-stage1" {
		t.Fatalf("encoder's claim: %+v", x)
	}
	if ls := e.s.st.Leases[x.ID]; ls.RunnerUp != nil {
		t.Fatalf("the Mac counted as the better worker: %+v", ls.RunnerUp)
	}
	// The stage 2 job still waits for the Mac (and encoder would run it beside the stage 1 job).
	if y := e.claim(enc); y != nil {
		t.Fatalf("encoder took the stage 2 job too: %+v", y)
	}
}

// A worker with a free normal slot whose RAM a high-priority job holds is no better worker now:
// it waits for a lease to end, and with no end in sight it counts in no scoring (Codex on #21).
func TestARAMBlockedWorkerIsNoBetterWorker(t *testing.T) {
	e, enc, mac := scored(t)
	e.limits(mac, 1, 1)
	w := e.s.st.Workers[mac]
	w.Caps.Hardware = []byte(`{"cpu_threads":12,"memory_bytes":` + strconv.FormatInt(8*gib, 10) + `}`)
	used := 2 * gib
	e.s.PutPresence(mac, Presence{State: "idle", MemoryUsed: &used})
	e.sofortWhisper("k3v9x2m1q7pz.whisper")
	if x, err := e.high(mac); err != nil || x == nil {
		t.Fatalf("the captions on the Mac: %+v %v", x, err)
	}
	e.clock.Add(time.Second)
	used = 45 * gib / 10 // 8 - 4.5 - 1 GB leaves no room for a 4K encode's 5.5
	e.s.PutPresence(mac, Presence{State: "working", MemoryUsed: &used})
	e.presence(enc)
	e.rung("b.2160-stage2", "stage2")
	e.sized("b.2160-stage2", 400e6)
	// The Mac would take 4 min against encoder's 12, but cannot start it: encoder takes it.
	if x := e.claim(enc); x == nil || x.Job.ID != "b.2160-stage2" {
		t.Fatalf("encoder left the job to the RAM-blocked Mac: %+v", x)
	}
}

func TestThreadShareCountsTheJobsBesideIt(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.machineThreads(w, 12)
	wk := e.s.st.Workers[w]
	for _, c := range []struct {
		other  float64
		beside int
		want   float64
	}{
		{0, 0, 1}, {0, 1, 2}, {0, 2, 3}, {6, 0, 2}, {6, 1, 4}, {12, 0, 8}, {6, 3, 8},
	} {
		o := c.other
		wk.OtherLoad = &o
		if got := e.s.threadShare(wk, c.beside); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("other %v, beside %d: %v, want %v", c.other, c.beside, got, c.want)
		}
	}
	// An asking worker with one job running expects the next to take twice its time.
	e2, enc, mac := scored(t)
	e2.s.st.Workers[mac].LastSeen = nil // encoder alone
	e2.machineThreads(enc, 6)
	e2.limits(enc, 2, 1)
	e2.rung("a.2160-archive", "archive")
	e2.sized("a.2160-archive", 1e6)
	idle, _ := e2.s.ect(e2.s.st.Workers[enc], e2.s.st.Jobs["a.2160-archive"], true, e2.clock.Now())
	e2.rung("b.2160-archive", "archive")
	e2.sized("b.2160-archive", 1e6)
	if l := e2.claim(enc); l == nil {
		t.Fatal("no first job")
	}
	beside, _ := e2.s.ect(e2.s.st.Workers[enc], e2.s.st.Jobs["b.2160-archive"], true, e2.clock.Now())
	if math.Abs(beside.RunS-2*idle.RunS) > 1e-6 {
		t.Fatalf("run_s beside one job %v, alone %v", beside.RunS, idle.RunS)
	}
}

func TestFreeAtAndTheForecastCountTheHighPrioritySlots(t *testing.T) {
	e, enc, mac := scored(t)
	e.s.st.Workers[mac].LastSeen = nil // encoder alone
	e.limits(enc, 1, 1)
	e.rung("a.2160-archive", "archive")
	e.sized("a.2160-archive", 1e6)
	l := e.claim(enc)
	// An hour left on the archive encode.
	if _, err := e.s.Renew(enc, l.ID, Progress{Phase: "encoding", FPS: f64(1), FramesDone: i64(3000 - 3600)}); err != nil {
		t.Fatal(err)
	}
	now := e.clock.Now()
	w := e.s.st.Workers[enc]
	if f := e.s.freeAt(w, now, "", true); f == nil || !f.Equal(now) {
		t.Fatalf("free for a stage 1 job at %v, want now", f)
	}
	if f := e.s.freeAt(w, now, "", false); f == nil || f.Sub(now) != time.Hour {
		t.Fatalf("free for an archive job at %v, want in an hour", f)
	}
	e.rung("b.2160-stage1", "stage1")
	e.sized("b.2160-stage1", 1e6)
	e.rung("c.2160-stage2", "stage2")
	e.sized("c.2160-stage2", 1e6)
	fc := e.s.forecast(now)
	run := 3000 / (12.5 / 3) * float64(time.Second)
	if got := fc["b.2160-stage1"].Sub(now); math.Abs(float64(got)-run) > float64(time.Second) {
		t.Fatalf("the stage 1 job finishes in %v, want %v from now in the high-priority slot", got, time.Duration(run))
	}
	if got := fc["c.2160-stage2"].Sub(now); math.Abs(float64(got)-float64(time.Hour)-run) > float64(time.Second) {
		t.Fatalf("the stage 2 job finishes in %v, want after the archive encode", got)
	}
}

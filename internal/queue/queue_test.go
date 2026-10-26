package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

var reference = []Tool{{"ffmpeg", "7.1.2-0+deb13u1"}, {"libx265", "4.1+1-1d117be"}, {"libx264", "core 164 r3108 31e19f9"}, {"whisper.cpp", "v1.9.5"}}

const modelSHA = "6c14d5adee5f86394037b4e4e8b59f1673b6cee10e3cf0b11bbdbee79c156208"

type env struct {
	t     *testing.T
	s     *Store
	clock *clock
	dir   string
}

func newEnv(t *testing.T, ref string) *env {
	t.Helper()
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
	cfg := Config{StateDir: filepath.Join(dir, "state"), SourceDir: filepath.Join(dir, "src"), ReferenceWorker: ref,
		Subnet: netip.MustParsePrefix("10.10.20.0/24"), TunnelAddress: netip.MustParseAddr("10.10.20.1"),
		ServerPublicKey: "c2VydmVyc2VydmVyc2VydmVyc2VydmVyc2VydmVyMDA=", Endpoint: "vpn.example.org:51821", API: "http://10.10.20.1:8090/v1",
		Now: c.Now, FreeBytes: func(string) (int64, error) { return 1 << 40, nil }}
	os.MkdirAll(cfg.SourceDir, 0o755)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, s: s, clock: c, dir: dir}
}

func (e *env) reopen() {
	e.t.Helper()
	if err := e.s.Save(); err != nil {
		e.t.Fatal(err)
	}
	s, err := Open(e.s.cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.s = s
}

func key(n byte) string {
	b := make([]byte, 32)
	b[0] = n
	return string(must(json.Marshal(b)))[1:45]
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// worker adds a worker with these tools and says it is there.
func (e *env) worker(name string, tools []Tool) string {
	e.t.Helper()
	en, err := e.s.AddWorker(WorkerCreate{Name: name, PublicKey: key(byte(len(e.s.st.Workers) + 1))})
	if err != nil {
		e.t.Fatal(err)
	}
	id := en.Worker.ID
	if _, err := e.s.PutCapabilities(id, Capabilities{AppVersion: "0.1.0", OS: "test", Tools: tools, Models: []Model{{"ggml-medium.bin", modelSHA}}}); err != nil {
		e.t.Fatal(err)
	}
	e.presence(id)
	return id
}

func (e *env) presence(ids ...string) {
	for _, id := range ids {
		if _, err := e.s.PutPresence(id, Presence{State: "idle"}); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) source(name string, data string) FileIn {
	p := filepath.Join(e.s.cfg.SourceDir, name)
	os.MkdirAll(filepath.Dir(p), 0o755)
	os.WriteFile(p, []byte(data), 0o644)
	sum := sha256.Sum256([]byte(data))
	return FileIn{Path: name, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}

func (e *env) rung(id, prio string) JobView {
	e.t.Helper()
	f := e.source(strings.Split(id, ".")[0]+"/master.mov", "master of "+id)
	params := `{"rung":"2160","codec":"libx265","preset":"slower","crf":29,"width":3840,"height":2160,"vbv_maxrate_kbps":5000,"audio":{"mode":"copy"}}`
	v, created, err := e.s.Submit(id, JobSubmit{Kind: KindRung, Priority: prio, Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, Width: 3840, Height: 2160}, Result: ResultSpec{Name: "2160.mp4", MaxBytes: 1 << 30}})
	if err != nil || !created {
		e.t.Fatalf("submit %s: %v %v", id, created, err)
	}
	return v
}

func (e *env) claim(wid string) *LeaseView {
	e.t.Helper()
	l, err := e.s.Claim(context.Background(), wid, LeaseRequest{}, 0)
	if err != nil {
		e.t.Fatalf("claim: %v", err)
	}
	return l
}

func (e *env) deliver(wid, lid, content string) error {
	e.t.Helper()
	fid, err := e.s.CreateUpload(wid, lid, int64(len(content)))
	if err != nil {
		return err
	}
	if _, err := e.s.AppendUpload(wid, fid, 0, strings.NewReader(content)); err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(content))
	_, err = e.s.Deliver(wid, lid, ResultIn{Outcome: "file", File: "/v1/files/" + fid, SHA256: hex.EncodeToString(sum[:]), EncodeS: 1, Tools: reference[:3]})
	return err
}

func code(err error) string {
	var qe *Error
	if errors.As(err, &qe) {
		return qe.Code
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func (e *env) state(id string) string {
	v, err := e.s.GetJob(id)
	if err != nil {
		e.t.Fatal(err)
	}
	return v.State
}

func TestPriorityOrder(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("a.archive", "archive")
	e.rung("b.stage2", "stage2")
	e.rung("c.stage1", "stage1")
	e.rung("d.stage1", "stage1")
	var got []string
	for i := 0; i < 4; i++ {
		l := e.claim(w)
		got = append(got, l.Job.ID)
		e.s.Release(w, l.ID, "user")
		e.s.CancelJob(l.Job.ID)
	}
	want := "c.stage1 d.stage1 b.stage2 a.archive"
	if strings.Join(got, " ") != want {
		t.Fatalf("order %v, want %s", got, want)
	}
}

func TestMoveTakesNeighboursPriority(t *testing.T) {
	e := newEnv(t, "")
	e.rung("a.x", "archive")
	e.rung("b.x", "archive")
	e.rung("c.x", "stage2")
	if _, err := e.s.MoveJob("b.x", Move{Before: "a.x"}); err != nil {
		t.Fatal(err)
	}
	v, err := e.s.MoveJob("a.x", Move{Before: "c.x"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Priority != "stage2" {
		t.Fatalf("a.x has %s, want stage2", v.Priority)
	}
	list := e.s.ListJobs(JobFilter{})
	var ids []string
	for _, j := range list.Items {
		ids = append(ids, j.ID+":"+itoaPos(j.Position))
	}
	if strings.Join(ids, " ") != "a.x:1 c.x:2 b.x:3" {
		t.Fatalf("order %v", ids)
	}
	// The pipeline puts the same job again after a crash: unchanged, not a conflict.
	j, _ := e.s.GetJob("a.x")
	var spec JobSubmit
	spec.Kind, spec.Priority, spec.Params, spec.Source, spec.Result = j.Kind, "archive", j.Params, j.Source, j.ResultSpec
	if _, created, err := e.s.Submit("a.x", spec); err != nil || created {
		t.Fatalf("resubmit after a move: created=%v err=%v", created, err)
	}
}

func itoaPos(p *int) string {
	if p == nil {
		return "-"
	}
	return string(rune('0' + *p))
}

func TestVersionsMustMatch(t *testing.T) {
	e := newEnv(t, "encoder")
	e.rung("a.rung", "stage1")
	other := append([]Tool(nil), reference...)
	other[1] = Tool{"libx265", "4.0"}
	mac := e.worker("mac", other)
	// The reference has not said what it runs: nobody else may take a job.
	if l := e.claim(mac); l != nil {
		t.Fatal("a job before the reference worker announced its tools")
	}
	enc := e.worker("encoder", reference)
	rep, _ := e.s.GetCapabilities(mac)
	if len(rep.Refused) != 1 || rep.Refused[0].Kind != KindRung || !strings.Contains(rep.Refused[0].Reason, "libx265 differs") {
		t.Fatalf("refused %+v", rep.Refused)
	}
	if contains(rep.Accepted, KindRung) || !contains(rep.Accepted, KindWhisper) {
		t.Fatalf("accepted %v", rep.Accepted)
	}
	if l := e.claim(mac); l != nil {
		t.Fatal("a rung for a worker with another libx265")
	}
	// A whisper job needs only whisper.cpp and the model.
	f := e.source("w/sound.wav", "RIFF")
	if _, _, err := e.s.Submit("w.whisper", JobSubmit{Kind: KindWhisper, Priority: "stage1",
		Params: json.RawMessage(`{"model_sha256":"` + modelSHA + `","language":"de"}`),
		Source: SourceIn{FileIn: f, Container: "wav"}, Result: ResultSpec{Name: "whisper.json", MaxBytes: 1 << 20}}); err != nil {
		t.Fatal(err)
	}
	l := e.claim(mac)
	if l == nil || l.Job.ID != "w.whisper" {
		t.Fatalf("mac got %+v, want the whisper job", l)
	}
	if l := e.claim(enc); l == nil || l.Job.ID != "a.rung" {
		t.Fatalf("encoder got %+v", l)
	}
}

func TestLeaseExpiryLateResultFirstAcceptedWins(t *testing.T) {
	e := newEnv(t, "")
	a := e.worker("a", reference)
	b := e.worker("b", reference)
	e.rung("x.rung", "stage2")
	la := e.claim(a)
	if la.Attempt != 1 {
		t.Fatalf("attempt %d", la.Attempt)
	}
	// a goes quiet; its lease runs out after 120 s.
	e.clock.Add(121 * time.Second)
	e.presence(b)
	e.s.Tick()
	if st := e.state("x.rung"); st != StateQueued {
		t.Fatalf("after expiry %s", st)
	}
	if _, err := e.s.Renew(a, la.ID, Progress{Phase: "encoding"}); code(err) != "lease_expired" {
		t.Fatalf("renew after expiry: %v", err)
	}
	lb := e.claim(b)
	if lb == nil || lb.Attempt != 2 {
		t.Fatalf("b's lease %+v", lb)
	}
	// a's result arrives late: it is taken and judged.
	if err := e.deliver(a, la.ID, "file from a"); err != nil {
		t.Fatalf("late result: %v", err)
	}
	if st := e.state("x.rung"); st != StateChecking {
		t.Fatalf("with a late result %s", st)
	}
	if _, err := e.s.PutVerdict("x.rung", 1, Verdict{Verdict: VerdictAccepted}); err != nil {
		t.Fatal(err)
	}
	if st := e.state("x.rung"); st != StateDone {
		t.Fatalf("after acceptance %s", st)
	}
	if _, err := e.s.Renew(b, lb.ID, Progress{Phase: "encoding"}); code(err) != "job_done" {
		t.Fatalf("b's renew: %v", err)
	}
	if err := e.deliver(b, lb.ID, "file from b"); code(err) != "job_done" {
		t.Fatalf("b's result after the job is done: %v", err)
	}
	p, sha, err := e.s.ResultFile("x.rung", 1)
	if err != nil || sha == "" {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(p); string(data) != "file from a" {
		t.Fatalf("the accepted file holds %q", data)
	}
	wa, _ := e.s.GetWorker(a)
	if wa.Counts.Expired != 1 || wa.Counts.Accepted != 1 {
		t.Fatalf("a's counts %+v", wa.Counts)
	}
}

func TestRejectExcludesWorkerAndFails(t *testing.T) {
	e := newEnv(t, "")
	a := e.worker("a", reference)
	b := e.worker("b", reference)
	e.rung("x.rung", "stage1")
	l := e.claim(a)
	if err := e.deliver(a, l.ID, "bad"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.PutVerdict("x.rung", 1, Verdict{Verdict: VerdictRejected, Reason: "ssim 0.8 below 0.95"}); err != nil {
		t.Fatal(err)
	}
	if l := e.claim(a); l != nil {
		t.Fatal("a got the job it failed again")
	}
	if p, _, _ := e.s.ResultFile("x.rung", 1); p != "" {
		t.Fatal("the rejected file is still there")
	}
	lb := e.claim(b)
	if lb == nil || lb.Attempt != 2 {
		t.Fatalf("b's lease %+v", lb)
	}
	if _, err := e.s.Deliver(b, lb.ID, ResultIn{Outcome: "failed", Code: "encode_failed", Message: "x265 crashed"}); err != nil {
		t.Fatal(err)
	}
	c := e.worker("c", reference)
	lc := e.claim(c)
	if err := e.deliver(c, lc.ID, "bad again"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.PutVerdict("x.rung", 3, Verdict{Verdict: VerdictRejected}); err != nil {
		t.Fatal(err)
	}
	if st := e.state("x.rung"); st != StateFailed {
		t.Fatalf("after three failures %s", st)
	}
	if _, err := e.s.RetryJob("x.rung"); err != nil {
		t.Fatal(err)
	}
	if l := e.claim(a); l == nil || l.Attempt != 4 {
		t.Fatalf("after a retry a gets attempt 4, got %+v", l)
	}
}

func TestPreemptionForNewUploads(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("encoder", reference)
	e.rung("old.archive", "archive")
	la := e.claim(w)
	e.rung("new.stage1", "stage1")
	e.s.Tick()
	if _, err := e.s.Renew(w, la.ID, Progress{Phase: "encoding"}); code(err) != "preempted" {
		t.Fatalf("the archive lease: %v", err)
	}
	l := e.claim(w)
	if l == nil || l.Job.ID != "new.stage1" {
		t.Fatalf("after preemption %+v", l)
	}
	if st := e.state("old.archive"); st != StateQueued {
		t.Fatalf("the archive job is %s", st)
	}
	// A second new job with the worker busy on the first: stage1 is not preempted for stage2.
	e.rung("new2.stage2", "stage2")
	e.s.Tick()
	if _, err := e.s.Renew(w, l.ID, Progress{Phase: "encoding"}); err != nil {
		t.Fatalf("stage1 preempted for stage2: %v", err)
	}
	// A free worker means no preemption at all.
	e2 := newEnv(t, "")
	w1 := e2.worker("encoder", reference)
	e2.worker("mac", reference)
	e2.rung("old.archive", "archive")
	l1 := e2.claim(w1)
	e2.rung("new.stage1", "stage1")
	e2.s.Tick()
	if _, err := e2.s.Renew(w1, l1.ID, Progress{Phase: "encoding"}); err != nil {
		t.Fatalf("preempted although a worker was free: %v", err)
	}
}

func TestPauseGivesBack(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.rung("x.rung", "archive")
	l := e.claim(w)
	until := e.clock.Now().Add(30 * time.Minute)
	up := &until
	if _, err := e.s.PatchMe(w, WorkerPatch{PausedUntil: &up}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.Claim(context.Background(), w, LeaseRequest{}, 0); code(err) != "paused" {
		t.Fatalf("claim while paused: %v", err)
	}
	// The worker keeps renewing while paused; after 60 s it gives the job back.
	e.clock.Add(30 * time.Second)
	e.s.Renew(w, l.ID, Progress{Phase: "paused"})
	e.s.Tick()
	if st := e.state("x.rung"); st != StateLeased {
		t.Fatalf("after 30 s of pause %s", st)
	}
	if err := e.s.Release(w, l.ID, "paused"); err != nil {
		t.Fatal(err)
	}
	if st := e.state("x.rung"); st != StateQueued {
		t.Fatalf("after giving back %s", st)
	}
	// A worker that does not give back loses the lease 30 s after the minute.
	e.s.PatchMe(w, WorkerPatch{PausedUntil: new(*time.Time)})
	l2 := e.claim(w)
	e.s.PatchMe(w, WorkerPatch{PausedUntil: &up})
	for i := 0; i < 4; i++ {
		e.clock.Add(25 * time.Second)
		e.s.Renew(w, l2.ID, Progress{Phase: "encoding"})
		e.presence(w)
		e.s.Tick()
	}
	if st := e.state("x.rung"); st != StateQueued {
		t.Fatalf("a pause held past 90 s: %s", st)
	}
	v, _ := e.s.GetWorker(w)
	if v.State != "paused" || v.Counts.GivenBack != 2 {
		t.Fatalf("worker %s %+v", v.State, v.Counts)
	}
}

func TestOfflineWorkerLosesLease(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.rung("x.rung", "archive")
	l := e.claim(w)
	e.clock.Add(50 * time.Second)
	e.s.Renew(w, l.ID, Progress{Phase: "encoding"})
	e.clock.Add(91 * time.Second)
	e.s.Tick()
	if st := e.state("x.rung"); st != StateQueued {
		t.Fatalf("worker offline, job %s", st)
	}
}

func TestStateSurvivesRestart(t *testing.T) {
	e := newEnv(t, "")
	w := e.worker("mac", reference)
	e.rung("x.rung", "stage1")
	e.rung("y.rung", "archive")
	l := e.claim(w)
	rev := e.s.Status("t").Revision
	e.reopen()
	if got := e.s.Status("t").Revision; got != rev {
		t.Fatalf("revision %d after restart, was %d", got, rev)
	}
	if st := e.state("x.rung"); st != StateLeased {
		t.Fatalf("x after restart %s", st)
	}
	if _, err := e.s.Renew(w, l.ID, Progress{Phase: "encoding"}); err != nil {
		t.Fatalf("the lease after restart: %v", err)
	}
	if len(e.s.Peers()) != 1 {
		t.Fatal("the peer is gone after restart")
	}
	c := e.s.WaitChanges(context.Background(), rev+100, 10, 0)
	if !c.Reset || len(c.Items) != 2 {
		t.Fatalf("changes from a revision ahead: %+v", c)
	}
}

func TestChangesFeed(t *testing.T) {
	e := newEnv(t, "")
	e.rung("a.rung", "stage1")
	c := e.s.WaitChanges(context.Background(), 0, 10, 0)
	if len(c.Items) != 1 {
		t.Fatalf("%+v", c)
	}
	done := make(chan Changes)
	go func() { done <- e.s.WaitChanges(context.Background(), c.Revision, 10, 5*time.Second) }()
	time.Sleep(50 * time.Millisecond)
	e.s.CancelJob("a.rung")
	got := <-done
	if len(got.Items) != 1 || got.Items[0].(JobView).State != StateCancelled {
		t.Fatalf("%+v", got)
	}
	e.s.Forget("a.rung")
	got = e.s.WaitChanges(context.Background(), got.Revision, 10, 0)
	if len(got.Items) != 1 || !got.Items[0].(ForgottenView).Forgotten {
		t.Fatalf("%+v", got)
	}
}

func TestSubnetAndPeers(t *testing.T) {
	e := newEnv(t, "")
	var peers []PeerInfo
	e.s.OnPeers = func(p []PeerInfo) { peers = p }
	a := e.worker("a", reference)
	e.worker("b", reference)
	if len(peers) != 2 || peers[0].Address.String() != "10.10.20.2" || peers[1].Address.String() != "10.10.20.3" {
		t.Fatalf("peers %+v", peers)
	}
	en, _ := e.s.WorkerTunnel(a)
	if !strings.HasPrefix(en.ConfigLine, "eqtunnel1 address=10.10.20.2/32 server_public_key=") ||
		!strings.Contains(en.ConfigLine, "endpoint=vpn.example.org:51821 allowed_ips=10.10.20.1/32 keepalive=25 api=http://10.10.20.1:8090/v1") {
		t.Fatalf("line %q", en.ConfigLine)
	}
	if _, err := e.s.AddWorker(WorkerCreate{Name: "a", PublicKey: key(99)}); code(err) != "name_exists" {
		t.Fatalf("same name: %v", err)
	}
	e.s.RemoveWorker(a)
	if len(peers) != 1 {
		t.Fatalf("after removal %+v", peers)
	}
}

func TestSubmitChecksTheSource(t *testing.T) {
	e := newEnv(t, "")
	f := e.source("a/m.mov", "abc")
	spec := JobSubmit{Kind: KindRung, Priority: "stage1", Params: json.RawMessage(`{"codec":"libx265","preset":"slower","crf":29,"width":1920,"height":1080}`),
		Source: SourceIn{FileIn: f}, Result: ResultSpec{Name: "1080.mp4", MaxBytes: 100}}
	bad := spec
	bad.Source.Bytes = 4
	if _, _, err := e.s.Submit("a.1080", bad); code(err) != "source_size" {
		t.Fatalf("wrong size: %v", err)
	}
	bad = spec
	bad.Source.Path = "../etc/passwd"
	if _, _, err := e.s.Submit("a.1080", bad); code(err) != "invalid" {
		t.Fatalf("path out of the source directory: %v", err)
	}
	bad = spec
	bad.Params = json.RawMessage(`{"codec":"libx265","preset":"slower","crf":29,"width":1920,"height":1080,"command":"rm -rf /"}`)
	if _, _, err := e.s.Submit("a.1080", bad); code(err) != "invalid" {
		t.Fatalf("an unknown parameter: %v", err)
	}
	if _, created, err := e.s.Submit("a.1080", spec); err != nil || !created {
		t.Fatal(err)
	}
	if _, created, err := e.s.Submit("a.1080", spec); err != nil || created {
		t.Fatalf("the same again: %v %v", created, err)
	}
	other := spec
	other.Priority = "archive"
	if _, _, err := e.s.Submit("a.1080", other); code(err) != "job_exists" {
		t.Fatalf("another job under the same id: %v", err)
	}
}

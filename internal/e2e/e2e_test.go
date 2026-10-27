// Package e2e drives the queue end to end: a fake pipeline submits and judges jobs over the
// pipeline API, and real workers take them through a real userspace WireGuard tunnel on
// localhost. The test binary stands in for ffmpeg and whisper-cli (fake_test.go).
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/queue"
	"github.com/kyrios/encode-queue/internal/server"
	"github.com/kyrios/encode-queue/internal/tunnel"
	"github.com/kyrios/encode-queue/internal/worker"
)

func TestMain(m *testing.M) {
	if os.Getenv("EQ_FAKE_TOOL") == "1" {
		os.Exit(fakeTool(os.Args[1:]))
	}
	os.Setenv("EQ_FAKE_TOOL", "1") // for the tools the workers start
	os.Exit(m.Run())
}

const (
	pipelineToken = "pipeline-token-0123456789"
	adminToken    = "admin-token-0123456789abc"
	modelSHA      = "1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b"
)

type logWriter struct{ t *testing.T }

func (w logWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

type harness struct {
	t       *testing.T
	store   *queue.Store
	admin   *httptest.Server
	srcDir  string
	models  string
	log     *slog.Logger
	evMu    sync.Mutex
	events  []map[string]any
	verdict func(job queue.JobView, attempt queue.AttemptView, file []byte) (string, string)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(logWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))
	key, err := tunnel.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	addr := netip.MustParseAddr("10.99.0.1")
	door, err := tunnel.OpenDoor(key, addr, 0, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(door.Close)
	cfg := queue.Config{StateDir: filepath.Join(dir, "state"), SourceDir: filepath.Join(dir, "sources"),
		Lease: 4 * time.Second, RenewEvery: time.Second, LongPoll: 2 * time.Second, PresenceEvery: time.Second,
		PauseGiveBack: 2 * time.Second, PauseGrace: 2 * time.Second,
		Subnet: netip.MustParsePrefix("10.99.0.0/24"), TunnelAddress: addr, ServerPublicKey: key.Public().String(),
		Endpoint: fmt.Sprintf("127.0.0.1:%d", door.Port()), API: "http://10.99.0.1:8090/v1", Log: log}
	os.MkdirAll(cfg.SourceDir, 0o755)
	store, err := queue.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store.OnPeers = func(peers []queue.PeerInfo) {
		var ps []tunnel.Peer
		for _, p := range peers {
			k, _ := tunnel.ParseKey(p.PublicKey)
			ps = append(ps, tunnel.Peer{PublicKey: k, Address: p.Address})
		}
		if err := door.SetPeers(ps); err != nil {
			t.Errorf("peers: %v", err)
		}
	}
	store.PeerStatus = func() map[string]queue.PeerStatus {
		out := map[string]queue.PeerStatus{}
		for k, p := range door.PeerStatus() {
			out[k.String()] = queue.PeerStatus{Endpoint: p.Endpoint, LastHandshake: p.LastHandshake}
		}
		return out
	}
	srv := &server.Server{Store: store, Version: "test", PipelineToken: pipelineToken, AdminToken: adminToken, Log: log}
	l, err := door.Listen(8090)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})
	go server.Serve(ctx, l, srv.HelperHandler(), log)
	go store.Run(stop)
	admin := httptest.NewServer(srv.AdminHandler(true))
	t.Cleanup(func() { admin.Close(); cancel(); close(stop) })

	models := filepath.Join(dir, "models")
	os.MkdirAll(models, 0o755)
	h := &harness{t: t, store: store, admin: admin, srcDir: cfg.SourceDir, models: models, log: log}
	h.verdict = func(queue.JobView, queue.AttemptView, []byte) (string, string) { return "accepted", "" }
	// A model whose sha256 the whisper job names: a file whose content hashes to it does not
	// exist, so the test's model is any file and the job names its hash (see whisperJob).
	return h
}

// call is a request to the pipeline and admin API.
func (h *harness) call(method, path, token string, body, out any) int {
	h.t.Helper()
	var r io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		r = bytes.NewReader(data)
	}
	req, _ := http.NewRequest(method, h.admin.URL+path, r)
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(data, out); err != nil {
			h.t.Fatalf("%s %s: %v: %s", method, path, err, data)
		}
	}
	if resp.StatusCode >= 300 {
		h.t.Logf("%s %s: %d %s", method, path, resp.StatusCode, data)
	}
	return resp.StatusCode
}

type testWorker struct {
	w      *worker.Worker
	tc     *tunnel.Client
	cancel context.CancelFunc
	done   chan struct{}
	id     string
}

// pair adds a worker the way Thorsten does without Safari: its public key into the admin API,
// the one line back into the worker, which brings its tunnel up and takes jobs.
func (h *harness) pair(name string) *testWorker {
	h.t.Helper()
	key, _ := tunnel.GenerateKey()
	var en queue.Enrolment
	if st := h.call("POST", "/v1/workers", adminToken, map[string]string{"name": name, "public_key": key.Public().String()}, &en); st != 201 {
		h.t.Fatalf("pair %s: %d", name, st)
	}
	cfg, err := tunnel.ParseLine(en.ConfigLine)
	if err != nil {
		h.t.Fatal(err)
	}
	tc, err := tunnel.Dial(key, cfg, "", h.log)
	if err != nil {
		h.t.Fatal(err)
	}
	api, err := worker.NewClient(tc.HTTP, cfg.API)
	if err != nil {
		h.t.Fatal(err)
	}
	self, _ := os.Executable()
	w, err := worker.New(worker.Config{Version: "0.1.0", StateDir: h.t.TempDir(), FFmpeg: self, WhisperCLI: self,
		WhisperVersion: "v1.9.5", ModelsDir: h.models, MaxParallel: 1, ChunkBytes: 1 << 10,
		LongPoll: time.Second, LogEvery: 200 * time.Millisecond, RetryAfter: 200 * time.Millisecond,
		Log: h.log.With("worker", name), Events: func(ev map[string]any) {
			h.evMu.Lock()
			h.events = append(h.events, ev)
			h.evMu.Unlock()
		}}, api)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tw := &testWorker{w: w, tc: tc, cancel: cancel, done: make(chan struct{}), id: en.Worker.ID}
	go func() { defer close(tw.done); w.Run(ctx) }()
	h.t.Cleanup(func() { tw.stop() })
	return tw
}

func (tw *testWorker) stop() {
	tw.cancel()
	<-tw.done
	tw.tc.Close()
}

// crash takes the tunnel down first, so the worker can give nothing back.
func (tw *testWorker) crash() {
	tw.tc.Close()
	tw.cancel()
	<-tw.done
}

func (h *harness) source(rel, content string) queue.FileIn {
	p := filepath.Join(h.srcDir, rel)
	os.MkdirAll(filepath.Dir(p), 0o755)
	os.WriteFile(p, []byte(content), 0o644)
	s := sha256.Sum256([]byte(content))
	return queue.FileIn{Path: rel, Bytes: int64(len(content)), SHA256: hex.EncodeToString(s[:])}
}

func (h *harness) rungJob(id, prio string, extra map[string]any) {
	h.t.Helper()
	up := strings.Split(id, ".")[0]
	body := map[string]any{"kind": "rung", "priority": prio, "upload_id": up, "title": "#one " + up, "stage": 1,
		"params": map[string]any{"rung": "2160", "codec": "libx265", "preset": "slower", "crf": 29, "width": 3840, "height": 2160,
			"vbv_maxrate_kbps": 5000, "audio": map[string]any{"mode": "copy"}},
		"source": map[string]any{"path": up + "/master.mov", "bytes": 0, "sha256": "", "frames": 10, "width": 3840, "height": 2160},
		"result": map[string]any{"name": "2160.mp4", "max_bytes": 1 << 20}}
	f := h.source(up+"/master.mov", strings.Repeat("master of "+id+"\n", 300))
	src := body["source"].(map[string]any)
	src["bytes"], src["sha256"] = f.Bytes, f.SHA256
	for k, v := range extra {
		body[k] = v
	}
	if st := h.call("PUT", "/v1/jobs/"+id, pipelineToken, body, nil); st != 201 {
		h.t.Fatalf("submit %s: %d", id, st)
	}
}

func (h *harness) whisperJob(id string) {
	h.t.Helper()
	model := filepath.Join(h.models, "ggml-small.bin")
	os.WriteFile(model, []byte("a model"), 0o644)
	s := sha256.Sum256([]byte("a model"))
	up := strings.Split(id, ".")[0]
	f := h.source(up+"/speech.wav", "RIFF....WAVEfmt ")
	body := map[string]any{"kind": "whisper", "priority": "stage1", "upload_id": up,
		"params": map[string]any{"model_sha256": hex.EncodeToString(s[:]), "model": "ggml-small.bin", "language": "de"},
		"source": map[string]any{"path": f.Path, "bytes": f.Bytes, "sha256": f.SHA256, "container": "wav"},
		"result": map[string]any{"name": "whisper.json", "max_bytes": 1 << 20}}
	if st := h.call("PUT", "/v1/jobs/"+id, pipelineToken, body, nil); st != 201 {
		h.t.Fatalf("submit %s: %d", id, st)
	}
}

// runPipeline is the fake pipeline: it follows /changes, fetches every result waiting for a
// verdict, checks its sha256 and judges it with h.verdict.
func (h *harness) runPipeline(ctx context.Context) {
	var since int64
	for ctx.Err() == nil {
		req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/changes?since=%d", h.admin.URL, since), nil)
		req.Header.Set("Authorization", "Bearer "+pipelineToken)
		req.Header.Set("Prefer", "wait=2")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		var c struct {
			Revision int64             `json:"revision"`
			Items    []json.RawMessage `json:"items"`
		}
		json.NewDecoder(resp.Body).Decode(&c)
		resp.Body.Close()
		since = c.Revision
		for _, raw := range c.Items {
			var j queue.JobView
			if json.Unmarshal(raw, &j) != nil || j.State != "checking" {
				continue
			}
			for _, a := range j.Attempts {
				if a.Result == nil || a.Result.Verdict != "checking" {
					continue
				}
				req, _ := http.NewRequestWithContext(ctx, "GET", h.admin.URL+*a.Result.FileURL, nil)
				req.Header.Set("Authorization", "Bearer "+pipelineToken)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return
				}
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				sum := sha256.Sum256(data)
				v, reason := h.verdict(j, a, data)
				if hex.EncodeToString(sum[:]) != a.Result.SHA256 || `"`+a.Result.SHA256+`"` != resp.Header.Get("ETag") {
					v, reason = "rejected", "sha256 differs"
				}
				h.call("PUT", fmt.Sprintf("/v1/jobs/%s/attempts/%d/verdict", j.ID, a.Attempt), pipelineToken,
					map[string]any{"verdict": v, "reason": reason}, nil)
			}
		}
	}
}

func (h *harness) pipeline() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); h.runPipeline(ctx) }()
	h.t.Cleanup(func() { cancel(); wg.Wait() })
}

func (h *harness) job(id string) queue.JobView {
	var j queue.JobView
	h.call("GET", "/v1/jobs/"+id, adminToken, nil, &j)
	return j
}

func (h *harness) waitFor(what string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) waitState(id, state string, timeout time.Duration) queue.JobView {
	h.t.Helper()
	var j queue.JobView
	h.waitFor(id+" "+state, timeout, func() bool { j = h.job(id); return j.State == state })
	return j
}

func (h *harness) metrics() string {
	resp, err := http.Get(h.admin.URL + "/metrics")
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func TestEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	tw := h.pair("encoder")
	h.rungJob("one_528.2160-stage1", "stage1", nil)
	h.whisperJob("one_528.whisper")
	a := h.waitState("one_528.2160-stage1", "done", 30*time.Second)
	b := h.waitState("one_528.whisper", "done", 30*time.Second)

	if len(a.Attempts) != 1 || a.Attempts[0].Result.Verdict != "accepted" || a.Worker == nil || a.Worker.Name != "encoder" {
		t.Fatalf("the rung: %+v", a)
	}
	got := h.fetch(a, 1)
	if !strings.HasPrefix(got, "master of one_528.2160-stage1") ||
		!strings.Contains(got, "-c:v libx265 -preset slower -crf 29 -pix_fmt yuv420p10le -profile:v main10 -tag:v hvc1 -x265-params log-level=error:vbv-maxrate=5000:vbv-bufsize=10000 -c:a copy") {
		t.Fatalf("the rung's file: %.300q", got)
	}
	if r := a.Attempts[0].Result; len(r.Tools) != 3 || r.Tools[1].Version != "4.1+1-fake" || r.Frames != 10 {
		t.Fatalf("the result's tools and frames: %+v", r)
	}
	if got := h.fetch(b, 1); !strings.Contains(got, "Hallo") {
		t.Fatalf("whisper's JSON: %q", got)
	}
	var logs struct{ Items []queue.LogLine }
	h.call("GET", "/v1/leases/"+a.Attempts[0].LeaseID+"/logs", adminToken, nil, &logs)
	if len(logs.Items) == 0 || !strings.Contains(logs.Items[0].Message, "sha256 checked") {
		t.Fatalf("logs %+v", logs.Items)
	}
	var ws struct{ Items []queue.WorkerView }
	h.call("GET", "/v1/workers", adminToken, nil, &ws)
	if len(ws.Items) != 1 || ws.Items[0].Counts.Accepted != 2 || len(ws.Items[0].Tools) != 4 || ws.Items[0].State == "offline" {
		t.Fatalf("workers %+v", ws.Items)
	}
	if wv := ws.Items[0]; wv.Endpoint == nil || !strings.HasPrefix(*wv.Endpoint, "127.0.0.1:") || wv.LastHandshake == nil ||
		wv.TunnelState == nil || *wv.TunnelState != "up" || wv.System.MemoryUsed == nil ||
		wv.System.CPUThreads == nil || wv.System.Arch == nil || wv.System.ThreadsInUse == nil {
		t.Fatalf("the worker's endpoint and system: %v %v %+v", wv.Endpoint, wv.LastHandshake, wv.System)
	}
	m := h.metrics()
	for _, want := range []string{
		fmt.Sprintf(`encode_queue_worker_attempts_total{worker_id="%s",worker="encoder",outcome="accepted"} 2`, tw.id),
		fmt.Sprintf(`encode_queue_worker_connected{worker_id="%s",worker="encoder"} 1`, tw.id),
		`encode_queue_jobs{priority="stage1",state="done"} 2`,
		fmt.Sprintf(`encode_queue_worker_tool_info{worker_id="%s",worker="encoder",tool="libx265",version="4.1+1-fake"} 1`, tw.id),
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	st := tw.w.Status()
	if st.FinishedToday != 2 || len(st.Recent) != 2 {
		t.Errorf("the worker's own status %+v", st)
	}
	// What Slipstream reads on the worker's standard output.
	h.evMu.Lock()
	seen := map[string]bool{}
	for _, ev := range h.events {
		seen[fmt.Sprint(ev["event"])] = true
		if ev["event"] == "connection" && ev["up"] == true {
			seen["up"] = true
		}
		if ev["event"] == "job" && ev["phase"] == "encoding" {
			seen["encoding"] = true
		}
	}
	h.evMu.Unlock()
	for _, want := range []string{"tools", "up", "idle", "job", "encoding", "log"} {
		if !seen[want] {
			t.Errorf("no %s event", want)
		}
	}
	// The pipeline forgets the job: its files go.
	if code := h.call("DELETE", "/v1/jobs/"+a.ID, pipelineToken, nil, nil); code != 204 {
		t.Fatalf("forget: %d", code)
	}
	if code := h.call("GET", "/v1/jobs/"+a.ID, pipelineToken, nil, nil); code != 404 {
		t.Fatalf("a forgotten job: %d", code)
	}
}

func (h *harness) fetch(j queue.JobView, attempt int) string {
	h.t.Helper()
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/jobs/%s/attempts/%d/file", h.admin.URL, j.ID, attempt), nil)
	req.Header.Set("Authorization", "Bearer "+pipelineToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return string(data)
}

func TestTokensAreKeptApart(t *testing.T) {
	h := newHarness(t)
	if code := h.call("GET", "/v1/jobs", "wrong", nil, nil); code != 401 {
		t.Fatalf("a wrong token: %d", code)
	}
	if code := h.call("PUT", "/v1/jobs/a.b", adminToken, map[string]any{}, nil); code != 403 {
		t.Fatalf("the admin submitting: %d", code)
	}
	if code := h.call("POST", "/v1/workers", pipelineToken, map[string]any{"name": "x", "public_key": "x"}, nil); code != 403 {
		t.Fatalf("the pipeline pairing: %d", code)
	}
}

func TestPreemptionOverTheTunnel(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "1000")
	t.Setenv("FAKE_FRAME_MS", "10")
	h.pair("encoder")
	h.rungJob("old_1.2160-archive", "archive", nil)
	h.waitState("old_1.2160-archive", "leased", 30*time.Second)
	os.Setenv("FAKE_FRAMES", "5") // the stage 1 job is short
	h.rungJob("new_2.2160-stage1", "stage1", nil)
	n := h.waitState("new_2.2160-stage1", "done", 30*time.Second)
	if n.Attempts[0].Worker.Name != "encoder" {
		t.Fatalf("%+v", n.Attempts)
	}
	o := h.waitState("old_1.2160-archive", "done", 30*time.Second)
	if len(o.Attempts) != 2 || o.Attempts[0].State != "preempted" || o.Attempts[1].Result.Verdict != "accepted" {
		t.Fatalf("the archive job's attempts: %+v", o.Attempts)
	}
}

func TestAssignOverTheTunnel(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "1000")
	t.Setenv("FAKE_FRAME_MS", "10")
	enc := h.pair("encoder")
	h.rungJob("old_1.2160-archive", "archive", nil)
	j := h.waitState("old_1.2160-archive", "leased", 30*time.Second)
	if j.Worker == nil || j.Worker.Name != "encoder" {
		t.Fatalf("leased by %+v", j.Worker)
	}
	os.Setenv("FAKE_FRAMES", "5")
	mac := h.pair("mac")
	h.waitFor("the mac idle", 30*time.Second, func() bool {
		var w queue.WorkerView
		h.call("GET", "/v1/workers/"+mac.id, adminToken, nil, &w)
		return w.State == "idle" && len(w.AcceptedKinds) > 0
	})
	var c queue.Candidates
	if code := h.call("GET", "/v1/jobs/"+j.ID+"/candidates", adminToken, nil, &c); code != 200 || len(c.Items) != 2 ||
		!c.Items[0].Current || !c.Items[1].Available || c.Items[1].Current {
		t.Fatalf("candidates %d %+v", code, c.Items)
	}
	var v queue.JobView
	if code := h.call("POST", "/v1/jobs/"+j.ID+"/assign", adminToken, map[string]string{"worker_id": mac.id}, &v); code != 200 {
		t.Fatalf("assign: %d", code)
	}
	if v.PinnedTo == nil || v.PinnedTo.WorkerID != mac.id {
		t.Fatalf("pinned to %+v", v.PinnedTo)
	}
	if code := h.call("POST", "/v1/jobs/"+j.ID+"/assign", adminToken, map[string]string{"worker_id": "wk_zzzzzzzz"}, nil); code != 404 {
		t.Fatalf("an unknown worker: %d", code)
	}
	d := h.waitState(j.ID, "done", 30*time.Second)
	if len(d.Attempts) != 2 || d.Attempts[0].Worker.ID != enc.id || d.Attempts[0].State != "preempted" ||
		d.Attempts[0].EndCode == nil || *d.Attempts[0].EndCode != "assigned" || d.Attempts[1].Worker.ID != mac.id || d.PinnedTo != nil {
		t.Fatalf("the attempts: %+v", d.Attempts)
	}
}

func TestCrashedWorkerLeaseExpires(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "1000")
	a := h.pair("mac")
	h.rungJob("x_1.1080", "archive", nil)
	h.waitState("x_1.1080", "leased", 30*time.Second)
	a.crash()
	os.Setenv("FAKE_FRAMES", "5")
	h.pair("server")
	j := h.waitState("x_1.1080", "done", 30*time.Second)
	if len(j.Attempts) != 2 || j.Attempts[0].State != "expired" || j.Attempts[1].Worker.Name != "server" {
		t.Fatalf("attempts %+v", j.Attempts)
	}
	if c := j.Attempts[0].EndCode; c == nil || (*c != "offline" && *c != "lease_expired") {
		t.Fatalf("end code %v", c)
	}
}

func TestPauseGivesTheJobBack(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "500")
	tw := h.pair("mac")
	h.rungJob("p_1.2160", "archive", nil)
	h.waitFor("encoding", 30*time.Second, func() bool {
		j := h.job("p_1.2160")
		return j.Progress != nil && j.Progress.Phase == "encoding"
	})
	tw.w.Pause(time.Now().Add(time.Hour))
	h.waitFor("the queue sees the pause", 5*time.Second, func() bool {
		var w queue.WorkerView
		h.call("GET", "/v1/workers/"+tw.id, adminToken, nil, &w)
		j := h.job("p_1.2160")
		return w.State == "paused" && j.Progress != nil && j.Progress.Phase == "paused"
	})
	j := h.waitState("p_1.2160", "queued", 10*time.Second)
	if a := j.Attempts[0]; a.State != "given_back" || a.EndCode == nil || *a.EndCode != "paused" {
		t.Fatalf("after the pause %+v", a)
	}
	time.Sleep(1500 * time.Millisecond)
	if j := h.job("p_1.2160"); j.State != "queued" {
		t.Fatalf("a paused worker took a job: %s", j.State)
	}
	os.Setenv("FAKE_FRAMES", "5")
	tw.w.Resume()
	j = h.waitState("p_1.2160", "done", 20*time.Second)
	if len(j.Attempts) != 2 {
		t.Fatalf("attempts %+v", j.Attempts)
	}
	// The Encoder page pauses a worker too; the worker hears it with its next presence.
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	h.call("PATCH", "/v1/workers/"+tw.id, adminToken, map[string]any{"paused_until": until}, nil)
	h.waitFor("the worker hears the admin's pause", 5*time.Second, func() bool { return tw.w.Status().State == "paused" })
}

func TestRejectedAndFailedResults(t *testing.T) {
	h := newHarness(t)
	h.verdict = func(queue.JobView, queue.AttemptView, []byte) (string, string) {
		return "rejected", "ssim 0.80 below 0.95"
	}
	h.pipeline()
	tw := h.pair("mac")
	h.rungJob("r_1.720", "stage2", map[string]any{"max_failures": 1})
	j := h.waitState("r_1.720", "failed", 20*time.Second)
	if r := j.Attempts[0].Result; r.Verdict != "rejected" || r.Message == nil || *r.Message != "ssim 0.80 below 0.95" || r.Path != nil {
		t.Fatalf("the rejected result %+v", r)
	}
	if m := h.metrics(); !strings.Contains(m, fmt.Sprintf(`encode_queue_worker_results_rejected_total{worker_id="%s",worker="mac"} 1`, tw.id)) {
		t.Fatalf("no rejection in the metrics")
	}
	h.waitFor("the worker hears the verdict", 1*time.Second, func() bool { return len(tw.w.Status().Recent) == 1 })

	t.Setenv("FAKE_FAIL", "1")
	h.rungJob("f_1.720", "stage2", map[string]any{"max_failures": 1})
	j = h.waitState("f_1.720", "failed", 20*time.Second)
	r := j.Attempts[0].Result
	if r.Outcome != "failed" || r.FailureCode == nil || *r.FailureCode != "encode_failed" || !strings.Contains(*r.Message, "fake failure") {
		t.Fatalf("the failure %+v", r)
	}
}

func TestCancelStopsTheWorker(t *testing.T) {
	h := newHarness(t)
	h.pipeline()
	t.Setenv("FAKE_FRAMES", "1000")
	tw := h.pair("mac")
	h.rungJob("c_1.2160", "stage1", nil)
	h.waitState("c_1.2160", "leased", 30*time.Second)
	if code := h.call("POST", "/v1/jobs/c_1.2160/cancel", adminToken, nil, nil); code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	h.waitFor("the worker stops", 5*time.Second, func() bool {
		st := tw.w.Status()
		return len(st.Jobs) == 0 && len(st.Recent) == 1 && st.Recent[0].Outcome == "stopped"
	})
	if r := tw.w.Status().Recent[0].Reason; r != "lease_cancelled" {
		t.Fatalf("stopped for %s", r)
	}
}

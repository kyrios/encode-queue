package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// hfStandIn answers for huggingface.co with a model's bytes, ranges included, and counts the
// requests.
type hfStandIn struct {
	mu   sync.Mutex
	body []byte
	cut  int // the first request ends after this many bytes (a broken download); 0 none
	seen []string
}

func (h *hfStandIn) RoundTrip(r *http.Request) (*http.Response, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, r.Header.Get("Range"))
	body, status := h.body, 200
	if rg := r.Header.Get("Range"); rg != "" {
		from, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(rg, "bytes="), "-"))
		body, status = body[from:], 206
	}
	if h.cut > 0 {
		body, h.cut = body[:h.cut], 0
		return &http.Response{StatusCode: status, Body: io.NopCloser(&brokenReader{data: body}), Header: http.Header{}}, nil
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(body))), Header: http.Header{}}, nil
}

type brokenReader struct{ data []byte }

func (b *brokenReader) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

const pinned = "https://huggingface.co/ggerganov/whisper.cpp/resolve/5359861c739e955e79d9a303bcbc70fb988958b1/ggml-small.bin"

func fetchWorker(t *testing.T, hf *hfStandIn) (*Worker, *[]map[string]any) {
	dir := t.TempDir()
	var events []map[string]any
	var mu sync.Mutex
	w := &Worker{cfg: Config{StateDir: dir, Log: slog.New(slog.DiscardHandler), FetchClient: &http.Client{Transport: hf}, Events: func(e map[string]any) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}}, models: map[string]string{}}
	return w, &events
}

func TestFetchAModel(t *testing.T) {
	data := []byte(strings.Repeat("ggml model bytes ", 100000))
	sum := sha256.Sum256(data)
	m := WantedModel{Name: "ggml-small.bin", SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)), URL: pinned}
	hf := &hfStandIn{body: data, cut: 500000}
	w, events := fetchWorker(t, hf)
	// The first try breaks off; the part stays and the next one resumes with a range.
	if w.fetchModels(context.Background(), []WantedModel{m}) {
		t.Fatal("a broken download counted")
	}
	if !w.fetchModels(context.Background(), []WantedModel{m}) {
		t.Fatal("the second try failed")
	}
	if hf.seen[1] != "bytes=500000-" {
		t.Fatalf("no resume: %v", hf.seen)
	}
	path := filepath.Join(w.cfg.StateDir, "models", "ggml-small.bin")
	if got, _ := os.ReadFile(path); string(got) != string(data) || w.modelPath(m.SHA256) != path {
		t.Fatal("the model is not in place")
	}
	states := map[string]bool{}
	for _, e := range *events {
		if e["event"] == "model" {
			states[e["state"].(string)] = true
		}
	}
	for _, s := range []string{"wanted", "checking", "failed"} {
		if !states[s] {
			t.Fatalf("no %q event: %v", s, *events)
		}
	}
	// Held already: nothing fetched again.
	n := len(hf.seen)
	w.fetchModels(context.Background(), []WantedModel{m})
	if len(hf.seen) != n {
		t.Fatal("fetched a model it holds")
	}
}

func TestFetchRefusesWhatIsNotPinnedOrDoesNotMatch(t *testing.T) {
	data := []byte("not the model")
	hf := &hfStandIn{body: data}
	w, _ := fetchWorker(t, hf)
	for _, u := range []string{
		"https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-small.bin",
		"https://example.com/ggml-small.bin",
		"http://huggingface.co/ggerganov/whisper.cpp/resolve/5359861c739e955e79d9a303bcbc70fb988958b1/ggml-small.bin",
	} {
		err := w.fetchModel(context.Background(), WantedModel{Name: "ggml-small.bin", SHA256: strings.Repeat("a", 64), Bytes: 13, URL: u})
		if err == nil || !strings.Contains(err.Error(), "not a pinned model") {
			t.Fatalf("%s: %v", u, err)
		}
	}
	if len(hf.seen) != 0 {
		t.Fatal("asked the network for an unpinned model")
	}
	err := w.fetchModel(context.Background(), WantedModel{Name: "ggml-small.bin", SHA256: strings.Repeat("a", 64), Bytes: int64(len(data)), URL: pinned})
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("a wrong sha256: %v", err)
	}
	if _, err := os.Stat(filepath.Join(w.cfg.StateDir, "models", "ggml-small.bin")); err == nil {
		t.Fatal("kept a model whose sha256 did not match")
	}
}

// encoder's /models is a read-only mount that infra fills: a model the queue wants there is
// provided by the host, never a failure (the Encoder page and the alerts must not show one).
func TestAReadOnlyModelsFolderIsNoFailure(t *testing.T) {
	hf := &hfStandIn{body: []byte("x")}
	w, events := fetchWorker(t, hf)
	ro := filepath.Join(w.cfg.StateDir, "ro")
	os.MkdirAll(ro, 0o755)
	if os.Geteuid() == 0 {
		// Root writes into a 0555 folder (the toolchain image's tests run as root): a folder
		// below a plain file cannot be made or written by anyone.
		os.WriteFile(filepath.Join(ro, "file"), nil, 0o644)
		ro = filepath.Join(ro, "file", "models")
	} else {
		os.Chmod(ro, 0o555)
		t.Cleanup(func() { os.Chmod(ro, 0o755) })
	}
	w.cfg.ModelsDir = ro
	m := WantedModel{Name: "ggml-large-v3-turbo.bin", SHA256: strings.Repeat("a", 64), Bytes: 1624555275, URL: pinned}
	for i := 0; i < 2; i++ {
		if w.fetchModels(context.Background(), []WantedModel{m}) {
			t.Fatal("fetched into a read-only folder")
		}
	}
	if len(hf.seen) != 0 {
		t.Fatal("asked the network for a model it cannot keep")
	}
	for _, e := range *events {
		if e["state"] == "failed" {
			t.Fatalf("a failure for a model the host provides: %v", e)
		}
	}
	if n := len(*events); n != 2 || (*events)[0]["state"] != "unavailable" || !strings.Contains((*events)[0]["reason"].(string), "read-only") {
		t.Fatalf("events %v", *events)
	}
	if !w.unavailable[m.SHA256] || w.markUnavailable(m.SHA256) {
		t.Fatal("not noted once")
	}
}

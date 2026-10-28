package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kyrios/encode-queue/internal/aligner"
	"github.com/kyrios/encode-queue/internal/queue"
	"github.com/kyrios/encode-queue/internal/worker"
)

// The align job end to end (v0.2.16): the queue, a worker of 0.2.16 whose aligner's Python is
// the fake tool (fakeAligner), the words as an input, whisper-cli's JSON back.

// alignerModel puts a stand-in of the aligner's files in the models folder: small files whose
// sizes and sha256 stand in the catalogue for the test.
func (h *harness) alignerModel() {
	h.t.Helper()
	old := aligner.Files
	h.t.Cleanup(func() { aligner.Files = old })
	dir := filepath.Join(h.models, aligner.Dir)
	os.MkdirAll(dir, 0o755)
	var files []aligner.File
	for _, name := range []string{"config.json", "model.safetensors"} {
		data := []byte("the aligner's " + name)
		os.WriteFile(filepath.Join(dir, name), data, 0o644)
		s := sha256.Sum256(data)
		files = append(files, aligner.File{Name: name, SHA256: hex.EncodeToString(s[:]), Bytes: int64(len(data))})
	}
	aligner.Files = files
}

func (h *harness) alignJob(id, container string) {
	h.t.Helper()
	up := strings.Split(id, ".")[0]
	src := h.source(up+"/preview.mp4", "a preview")
	if container == "wav" {
		src = h.source(up+"/speech.wav", "RIFF....WAVEfmt ")
	}
	words := h.source(up+"/words.json", `{"words":[{"text":"Ein","start":0.5,"end":0.8},{"text":"Thema","start":0.8,"end":1.3},{"text":"–","start":1.3,"end":1.3}]}`)
	body := map[string]any{"kind": "align", "priority": "now", "upload_id": up, "stage": 1,
		"params": map[string]any{"model_sha256": aligner.SHA256, "model": aligner.Name, "language": "de", "text": "text"},
		"source": map[string]any{"path": src.Path, "bytes": src.Bytes, "sha256": src.SHA256, "container": container, "duration_s": 26.8},
		"inputs": map[string]any{"text": map[string]any{"path": words.Path, "bytes": words.Bytes, "sha256": words.SHA256}},
		"result": map[string]any{"name": "align.json", "max_bytes": 1 << 20}}
	if st := h.call("PUT", "/v1/jobs/"+id, pipelineToken, body, nil); st != 201 {
		h.t.Fatalf("submit %s: %d", id, st)
	}
}

func alignerWorker(c *worker.Config) {
	self, _ := os.Executable()
	c.Version, c.AlignerPython = "0.2.16", self
}

func TestAlignEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.alignerModel()
	h.pipeline()
	tw := h.pairWith("encoder", alignerWorker)
	h.alignJob("one_529.align", "mp4")
	j := h.waitState("one_529.align", "done", 30*time.Second)
	var doc struct {
		Transcription []struct {
			Text    string
			Offsets struct{ From, To int }
		}
		Argv map[string]string
	}
	if err := json.Unmarshal([]byte(h.fetch(j, 1)), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Transcription) != 3 || doc.Transcription[1].Text != " Thema" || doc.Transcription[1].Offsets.From != 500 {
		t.Fatalf("the result: %+v", doc.Transcription)
	}
	// The sound the worker's ffmpeg made from the preview, the model's folder, the language, and
	// float32 or lean by the memory the test machine has.
	a := doc.Argv
	if !strings.HasSuffix(a["wav"], "speech.wav") || filepath.Base(a["model"]) != aligner.Dir || a["language"] != "de" ||
		(a["precision"] != "float32" && a["precision"] != "lean") || !strings.HasSuffix(a["words"], "input-text") {
		t.Fatalf("the aligner's arguments: %+v", a)
	}
	r := j.Attempts[0].Result
	if len(r.Tools) != 2 || r.Tools[0] != (queue.Tool{Name: "qwen-asr", Version: "0.0.6"}) || r.Tools[1].Name != "ffmpeg" {
		t.Fatalf("the result's tools: %+v", r.Tools)
	}
	if j.Attempts[0].ModelSHA256 == nil || *j.Attempts[0].ModelSHA256 != aligner.SHA256 {
		t.Fatalf("the attempt's model: %+v", j.Attempts[0])
	}
	// The worker said what it runs: the engine as a tool, the aligner among its models.
	var ws struct{ Items []queue.WorkerView }
	h.call("GET", "/v1/workers", adminToken, nil, &ws)
	found := false
	for _, m := range ws.Items[0].Models {
		found = found || (m.SHA256 == aligner.SHA256 && m.Name == aligner.Name)
	}
	if !found {
		t.Fatalf("the worker's models: %+v", ws.Items[0].Models)
	}
	if st := tw.w.Status(); st.FinishedToday != 1 {
		t.Fatalf("status %+v", st)
	}
}

// A worker without the engine, or of an older version, never gets the job; a refusal of the
// words fails the attempt with the aligner's reason.
func TestAlignRefusalsEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.alignerModel()
	h.pipeline()
	h.pair("plain")
	h.alignJob("one_530.align", "wav")
	time.Sleep(1500 * time.Millisecond)
	if j := h.job("one_530.align"); j.State != "queued" || len(j.Attempts) != 0 {
		t.Fatalf("a worker without the aligner took it: %+v", j)
	}
	t.Setenv("FAKE_ALIGN_REFUSE", "1")
	h.pairWith("encoder", alignerWorker)
	h.waitFor("a failed attempt", 30*time.Second, func() bool {
		j := h.job("one_530.align")
		return len(j.Attempts) > 0 && j.Attempts[0].Result != nil && j.Attempts[0].Result.Outcome == "failed"
	})
	a := h.job("one_530.align").Attempts[0]
	if a.Result.FailureCode == nil || *a.Result.FailureCode != "other" || a.Result.Message == nil || !strings.Contains(*a.Result.Message, "the aligner refused the words") {
		t.Fatalf("the failure: %+v", a.Result)
	}
}

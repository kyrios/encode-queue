package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// **The whisper models the queue wants** (v0.2.7; Thorsten, 2026-10-27: "Choose the whisper model
// based on measured performance at first launch"). The queue answers the capabilities with the
// models it wants this worker to hold, chosen from its calibration. The worker fetches the ones
// it lacks into its models folder, from ggerganov/whisper.cpp at a pinned revision only, checks
// each by its sha256, and tells the app (Slipstream's Settings → Leistung) how far it is.

// WantedModel is one model the queue wants this worker to hold.
type WantedModel struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	URL    string `json:"url"`
}

// pinnedModelURL: only a file of ggerganov/whisper.cpp at a commit, never a branch.
var pinnedModelURL = regexp.MustCompile(`^https://huggingface\.co/ggerganov/whisper\.cpp/resolve/[0-9a-f]{40}/ggml-[a-z0-9.-]+\.bin$`)

var modelNameRe = regexp.MustCompile(`^ggml-[a-z0-9.-]+\.bin$`)

var sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// emitModel tells the app about one model: wanted, downloading, checking, calibrating, ready or
// failed, with what the state has (bytes, bytes_done, speed, reason).
func (w *Worker) emitModel(sha, name, state string, extra map[string]any) {
	f := map[string]any{"sha256": sha, "name": name, "state": state}
	for k, v := range extra {
		f[k] = v
	}
	w.emit("model", f)
}

// modelsDir is where fetched models go: the configured folder, else STATE/models.
func (w *Worker) modelsDir() string {
	if w.cfg.ModelsDir != "" {
		return w.cfg.ModelsDir
	}
	return filepath.Join(w.cfg.StateDir, "models")
}

// fetchModels fetches the wanted models the worker lacks, one at a time, and says whether it
// got one. A failure leaves the others to try.
func (w *Worker) fetchModels(ctx context.Context, wanted []WantedModel) bool {
	got := false
	for _, m := range wanted {
		if w.modelPath(m.SHA256) != "" {
			continue
		}
		w.emitModel(m.SHA256, m.Name, "wanted", map[string]any{"bytes": m.Bytes})
		if err := w.fetchModel(ctx, m); err != nil {
			w.cfg.Log.Warn("model not fetched", "model", m.Name, "err", err)
			w.emitModel(m.SHA256, m.Name, "failed", map[string]any{"bytes": m.Bytes, "reason": err.Error()})
			continue
		}
		got = true
	}
	return got
}

func (w *Worker) fetchModel(ctx context.Context, m WantedModel) error {
	if !pinnedModelURL.MatchString(m.URL) || !modelNameRe.MatchString(m.Name) || !sha256Re.MatchString(m.SHA256) || m.Bytes <= 0 {
		return fmt.Errorf("not a pinned model: %s", m.URL)
	}
	dir := w.modelsDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	final := filepath.Join(dir, m.Name)
	part := final + ".part"
	// Resume a part from an earlier run with a range request.
	var done int64
	if fi, err := os.Stat(part); err == nil && fi.Size() < m.Bytes {
		done = fi.Size()
	} else {
		os.Remove(part)
	}
	req, err := http.NewRequestWithContext(ctx, "GET", m.URL, nil)
	if err != nil {
		return err
	}
	if done > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(done, 10)+"-")
	}
	resp, err := w.fetchClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusPartialContent && done > 0:
	case resp.StatusCode == http.StatusOK:
		done = 0 // the server sends the whole file
	default:
		return fmt.Errorf("%s: %s", m.URL, resp.Status)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if done > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}
	last := time.Time{}
	buf := make([]byte, 1<<20)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := f.Write(buf[:n]); err != nil {
				f.Close()
				return err
			}
			done += int64(n)
			if done > m.Bytes {
				f.Close()
				os.Remove(part)
				return fmt.Errorf("more than its %d bytes", m.Bytes)
			}
			if time.Since(last) >= time.Second {
				last = time.Now()
				w.emitModel(m.SHA256, m.Name, "downloading", map[string]any{"bytes": m.Bytes, "bytes_done": done})
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			f.Close()
			return rerr // the part stays for the next try
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if done != m.Bytes {
		return fmt.Errorf("%d of its %d bytes", done, m.Bytes)
	}
	w.emitModel(m.SHA256, m.Name, "checking", map[string]any{"bytes": m.Bytes, "bytes_done": done})
	got, err := fileSHA256(part)
	if err != nil {
		return err
	}
	if got != m.SHA256 {
		os.Remove(part)
		return fmt.Errorf("sha256 %s, not %s", got, m.SHA256)
	}
	if err := os.Rename(part, final); err != nil {
		return err
	}
	w.mu.Lock()
	w.models[m.SHA256] = final
	w.mu.Unlock()
	w.cfg.Log.Info("model fetched", "model", m.Name, "bytes", m.Bytes)
	return nil
}

// modelPath is the file of a model the worker holds, or "".
func (w *Worker) modelPath(sha string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.models[sha]
}

// heldModels is a copy of the models the worker holds, sha256 to path.
func (w *Worker) heldModels() map[string]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]string, len(w.models))
	for k, v := range w.models {
		out[k] = v
	}
	return out
}

// fetchClient is the client for model downloads: the internet, not the tunnel. Tests set it.
func (w *Worker) fetchClient() *http.Client {
	if w.cfg.FetchClient != nil {
		return w.cfg.FetchClient
	}
	return &http.Client{Timeout: 0}
}

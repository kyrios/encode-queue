package worker

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/kyrios/encode-queue/internal/aligner"
)

// **The align job** (v0.2.16, internal/aligner): Qwen3-ForcedAligner times the words of a text
// in the source's sound, through align.py and an engine's own Python. On Linux the image brings
// the engine (PyTorch for the CPU, EQ_ALIGNER_PYTHON). On an Apple-silicon Mac the worker sets up
// MLX itself, as it fetches whisper models: python-build-standalone's CPython and the pinned
// wheels, each by its sha256, in STATE/aligner, so Slipstream needs no change. A worker with an
// engine holds the model too: it fetches the eight files at the pinned revision into its models
// folder, or, where that is read-only (encoder's), the host puts them there.

// AlignParams are an align job's settings (helper.yaml).
type AlignParams struct {
	ModelSHA256 string `json:"model_sha256"`
	Model       string `json:"model,omitempty"`
	Language    string `json:"language"`
	Text        string `json:"text,omitempty"`
}

// alignerOff turns the aligner off (EQ_ALIGNER_PYTHON=off).
const alignerOff = "off"

// autoEngine says whether the worker sets up the Mac's engine itself: on an Apple-silicon Mac
// when no Python is configured.
func autoEngine(cfg Config) bool {
	return cfg.AlignerPython == "" && runtime.GOOS == "darwin" && runtime.GOARCH == "arm64"
}

// engineDir is where the worker sets up the Mac's engine, by the engine's id.
func engineDir(cfg Config) string {
	return filepath.Join(cfg.StateDir, "aligner", "engine-"+aligner.EngineID(aligner.PythonDarwinArm64, aligner.RequirementsMLX))
}

// enginePython is the aligner's Python: EQ_ALIGNER_PYTHON, or the Mac's own engine once it is
// set up; "" for none.
func enginePython(cfg Config) string {
	switch {
	case cfg.AlignerPython == alignerOff:
		return ""
	case cfg.AlignerPython != "":
		return cfg.AlignerPython
	case autoEngine(cfg) && fileExists(filepath.Join(engineDir(cfg), ".ready")):
		return filepath.Join(engineDir(cfg), "python", "bin", "python3")
	}
	return ""
}

// alignScript writes align.py where the engine's Python runs it from and returns its path.
func alignScript(stateDir string) (string, error) {
	dir := filepath.Join(stateDir, "aligner")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	p := filepath.Join(dir, "align.py")
	if old, err := os.ReadFile(p); err == nil && string(old) == string(aligner.Script) {
		return p, nil
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, aligner.Script, 0o644); err != nil {
		return "", err
	}
	return p, os.Rename(tmp, p)
}

// alignEnv keeps the engine off the network and out of the worker's way.
var alignEnv = []string{"HF_HUB_OFFLINE=1", "TRANSFORMERS_OFFLINE=1", "HF_HUB_DISABLE_TELEMETRY=1",
	"PYTHONDONTWRITEBYTECODE=1", "PYTHONNOUSERSITE=1", "TOKENIZERS_PARALLELISM=false"}

// DetectAligner is the aligner engine the configured Python runs, as a tool with its version.
func DetectAligner(ctx context.Context, cfg Config) (*Tool, string) {
	py := enginePython(cfg)
	if py == "" {
		return nil, ""
	}
	script, err := alignScript(cfg.StateDir)
	if err != nil {
		return nil, "aligner: " + err.Error()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute) // PyTorch's import takes 6 s on encoder
	defer cancel()
	cmd := exec.CommandContext(ctx, py, "-I", script, "version")
	cmd.Env = append(os.Environ(), alignEnv...)
	out, err := cmd.Output()
	if err != nil {
		msg := err.Error()
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			msg = lastLines(string(ee.Stderr), 2)
		}
		return nil, fmt.Sprintf("aligner (%s): %s", py, msg)
	}
	var v struct{ Name, Version string }
	if err := json.Unmarshal(out, &v); err != nil || aligner.Rank(v.Name) < 0 || v.Version == "" {
		return nil, fmt.Sprintf("aligner (%s): %q is no engine it knows", py, strings.TrimSpace(string(out)))
	}
	return &Tool{v.Name, v.Version}, ""
}

// DetectAlignerModel is the aligner's folder in the models folder when every file is there with
// its size and sha256 (cached by size and time, as the whisper models), or "".
func DetectAlignerModel(dir, cacheFile string) (string, error) {
	if dir == "" {
		return "", nil
	}
	md := filepath.Join(dir, aligner.Dir)
	if fi, err := os.Stat(md); err != nil || !fi.IsDir() {
		return "", nil
	}
	cache := map[string]modelCacheEntry{}
	if data, err := os.ReadFile(cacheFile); err == nil {
		json.Unmarshal(data, &cache)
	}
	changed := false
	defer func() {
		if changed && cacheFile != "" {
			if data, err := json.Marshal(cache); err == nil {
				os.WriteFile(cacheFile, data, 0o600)
			}
		}
	}()
	for _, f := range aligner.Files {
		p := filepath.Join(md, f.Name)
		fi, err := os.Stat(p)
		if err != nil || fi.Size() != f.Bytes {
			return "", nil
		}
		c, ok := cache[p]
		if !ok || c.Size != fi.Size() || !c.ModTime.Equal(fi.ModTime()) {
			sum, err := fileSHA256(p)
			if err != nil {
				return "", err
			}
			c = modelCacheEntry{Size: fi.Size(), ModTime: fi.ModTime(), SHA256: sum}
			cache[p] = c
			changed = true
		}
		if c.SHA256 != f.SHA256 {
			return "", fmt.Errorf("%s: sha256 %s, not %s", p, c.SHA256, f.SHA256)
		}
	}
	return md, nil
}

// alignerDir is the aligner's folder when the worker holds it, or "".
func (w *Worker) alignerDir() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.aligner
}

// alignerLoop sets up the Mac's engine where the worker does that itself, fetches the model
// where it has an engine and lacks it, and tells the queue. Beside the claims, as the models.
func (w *Worker) alignerLoop(ctx context.Context) {
	changed := false
	if autoEngine(w.cfg) && w.engine() == "" {
		// Set up once, or again when the one set up does not answer (DetectAligner at the start).
		if enginePython(w.cfg) != "" {
			w.cfg.Log.Warn("aligner engine does not run: set up again", "dir", engineDir(w.cfg))
			os.RemoveAll(engineDir(w.cfg))
		}
		if err := w.installEngine(ctx); err != nil {
			if ctx.Err() == nil {
				w.cfg.Log.Warn("aligner engine not set up", "err", err)
				w.emit("aligner", map[string]any{"state": "failed", "reason": err.Error()})
			}
			return
		}
		tool, problem := DetectAligner(ctx, w.cfg)
		if tool == nil {
			w.cfg.Log.Warn("aligner engine does not run", "problem", problem)
			w.emit("aligner", map[string]any{"state": "failed", "reason": problem})
			return
		}
		w.mu.Lock()
		w.caps.Tools = append(w.caps.Tools, *tool)
		w.mu.Unlock()
		changed = true
	}
	engine := w.engine()
	if engine == "" {
		return
	}
	w.emit("aligner", map[string]any{"state": "ready", "engine": engine})
	if w.alignerDir() == "" {
		dir, err := w.fetchAligner(ctx)
		if err != nil {
			if ctx.Err() == nil {
				w.cfg.Log.Warn("aligner model not fetched", "err", err)
				w.emitModel(aligner.SHA256, aligner.Name, "failed", map[string]any{"bytes": aligner.Bytes(), "reason": err.Error()})
			}
		} else if dir != "" {
			w.mu.Lock()
			w.aligner = dir
			w.caps.Models = append(w.caps.Models, Model{Name: aligner.Name, SHA256: aligner.SHA256})
			w.mu.Unlock()
			changed = true
		}
	}
	if !changed {
		return
	}
	w.mu.Lock()
	caps := w.caps
	w.mu.Unlock()
	if _, err := w.api.PutCapabilities(ctx, caps); err != nil {
		w.cfg.Log.Warn("capabilities not sent", "err", err)
	}
}

// engine is the aligner engine the worker runs, or "".
func (w *Worker) engine() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range aligner.Engines {
		for _, t := range w.caps.Tools {
			if t.Name == e {
				return e
			}
		}
	}
	return ""
}

// fetchAligner fetches the model's files into the models folder and returns its folder; "" and
// no error where the folder is read-only (the host provides it) or the machine's memory could
// never hold a job.
func (w *Worker) fetchAligner(ctx context.Context) (string, error) {
	sha, name, total := aligner.SHA256, aligner.Name, aligner.Bytes()
	if !writable(w.modelsDir()) {
		if w.markUnavailable(sha) {
			w.cfg.Log.Info("aligner model provided by the host", "dir", filepath.Join(w.modelsDir(), aligner.Dir))
		}
		w.emitModel(sha, name, "unavailable", map[string]any{"bytes": total, "reason": "the models folder is read-only: the host provides the models"})
		return "", nil
	}
	if mem := hardwareMemory(); mem > 0 && mem < aligner.PeakBytes[aligner.EngineMLX]+(1<<30) {
		w.emitModel(sha, name, "unavailable", map[string]any{"bytes": total, "reason": "too little memory for the aligner"})
		return "", nil
	}
	final := filepath.Join(w.modelsDir(), aligner.Dir)
	part := final + ".part"
	if err := os.MkdirAll(part, 0o755); err != nil {
		return "", err
	}
	w.emitModel(sha, name, "wanted", map[string]any{"bytes": total})
	var before int64
	for _, f := range aligner.Files {
		p := filepath.Join(part, f.Name)
		if fi, err := os.Stat(p); err == nil && fi.Size() == f.Bytes {
			if got, err := fileSHA256(p); err == nil && got == f.SHA256 {
				before += f.Bytes
				continue
			}
		}
		last := time.Time{}
		err := download(ctx, w.fetchClient(), f.URL(), p, f.Bytes, f.SHA256, func(n int64) {
			if time.Since(last) >= time.Second {
				last = time.Now()
				w.emitModel(sha, name, "downloading", map[string]any{"bytes": total, "bytes_done": before + n})
			}
		})
		if err != nil {
			return "", fmt.Errorf("%s: %w", f.Name, err)
		}
		before += f.Bytes
	}
	w.emitModel(sha, name, "checking", map[string]any{"bytes": total, "bytes_done": total})
	os.RemoveAll(final)
	if err := os.Rename(part, final); err != nil {
		return "", err
	}
	dir, err := DetectAlignerModel(w.modelsDir(), filepath.Join(w.cfg.StateDir, "models-cache.json"))
	if err != nil || dir == "" {
		return "", fmt.Errorf("the model's files do not check out: %v", err)
	}
	w.emitModel(sha, name, "ready", map[string]any{"bytes": total})
	w.cfg.Log.Info("aligner model fetched", "dir", dir, "bytes", total)
	return dir, nil
}

// download fetches url into path, resuming a .part with a range request, and checks its size and
// sha256; progress gets the bytes done.
func download(ctx context.Context, client *http.Client, url, path string, size int64, sum string, progress func(int64)) error {
	part := path + ".part"
	var done int64
	if fi, err := os.Stat(part); err == nil && fi.Size() < size {
		done = fi.Size()
	} else {
		os.Remove(part)
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	if done > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(done, 10)+"-")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusPartialContent && done > 0:
	case resp.StatusCode == http.StatusOK:
		done = 0
	default:
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if done > 0 {
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	f, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, &counting{r: io.LimitReader(resp.Body, size-done+1), n: done, progress: progress})
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err // the part stays for the next try
	}
	if done+n != size {
		os.Remove(part)
		return fmt.Errorf("%d bytes, not %d", done+n, size)
	}
	got, err := fileSHA256(part)
	if err != nil {
		return err
	}
	if got != sum {
		os.Remove(part)
		return fmt.Errorf("sha256 %s, not %s", got, sum)
	}
	return os.Rename(part, path)
}

type counting struct {
	r        io.Reader
	n        int64
	progress func(int64)
}

func (c *counting) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if n > 0 && c.progress != nil {
		c.progress(c.n)
	}
	return n, err
}

// installEngine sets up the Mac's engine in engineDir: the Python, unpacked, and the wheels of
// RequirementsMLX installed into it by pip, by their hashes, binaries only, nothing else. A
// `.ready` file says it is whole; anything there without it is a broken try and goes.
func (w *Worker) installEngine(ctx context.Context) error {
	dir := engineDir(w.cfg)
	if fileExists(filepath.Join(dir, ".ready")) {
		return nil
	}
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	// Older engines go: a new one is a new id.
	if old, _ := filepath.Glob(filepath.Join(parent, "engine-*")); len(old) > 0 {
		for _, o := range old {
			if o != dir {
				os.RemoveAll(o)
			}
		}
	}
	os.RemoveAll(dir)
	py := aligner.PythonDarwinArm64
	w.emit("aligner", map[string]any{"state": "downloading", "engine": aligner.EngineMLX, "bytes": py.Bytes})
	tgz := filepath.Join(parent, "python-"+py.SHA256[:12]+".tar.gz")
	last := time.Time{}
	if err := download(ctx, w.fetchClient(), py.URL, tgz, py.Bytes, py.SHA256, func(n int64) {
		if time.Since(last) >= time.Second {
			last = time.Now()
			w.emit("aligner", map[string]any{"state": "downloading", "engine": aligner.EngineMLX, "bytes": py.Bytes, "bytes_done": n})
		}
	}); err != nil {
		return fmt.Errorf("python: %w", err)
	}
	if err := untar(tgz, dir); err != nil {
		os.RemoveAll(dir)
		return fmt.Errorf("python: %w", err)
	}
	os.Remove(tgz)
	req := filepath.Join(dir, "requirements.txt")
	if err := os.WriteFile(req, aligner.RequirementsMLX, 0o644); err != nil {
		return err
	}
	w.emit("aligner", map[string]any{"state": "installing", "engine": aligner.EngineMLX})
	ictx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	python := filepath.Join(dir, "python", "bin", "python3")
	cmd := exec.CommandContext(ictx, python, "-I", "-m", "pip", "install", "--no-deps", "--require-hashes",
		"--only-binary=:all:", "--no-input", "--disable-pip-version-check", "--no-cache-dir",
		"--break-system-packages", "-r", req)
	cmd.Env = append(os.Environ(), "PIP_CONFIG_FILE="+os.DevNull, "PYTHONNOUSERSITE=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		return fmt.Errorf("pip: %v: %s", err, lastLines(string(out), 3))
	}
	if err := os.WriteFile(filepath.Join(dir, ".ready"), []byte(aligner.EngineID(py, aligner.RequirementsMLX)+"\n"), 0o644); err != nil {
		return err
	}
	w.cfg.Log.Info("aligner engine set up", "dir", dir)
	return nil
}

// untar unpacks a python-build-standalone tarball into dir: regular files, folders and links
// that stay inside it, nothing else.
func untar(tgz, dir string) error {
	f, err := os.Open(tgz)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tr := tar.NewReader(gz)
	root, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	inside := func(p string) bool {
		return p == root || strings.HasPrefix(p, root+string(filepath.Separator))
	}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		p := filepath.Join(root, filepath.Clean("/"+h.Name))
		if !inside(p) {
			return fmt.Errorf("%s leaves the folder", h.Name)
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(p, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(h.Mode)&0o755|0o600)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if filepath.IsAbs(h.Linkname) || !inside(filepath.Join(filepath.Dir(p), h.Linkname)) {
				return fmt.Errorf("%s links out of the folder", h.Name)
			}
			os.MkdirAll(filepath.Dir(p), 0o755)
			if err := os.Symlink(h.Linkname, p); err != nil {
				return err
			}
		case tar.TypeLink:
			target := filepath.Join(root, filepath.Clean("/"+h.Linkname))
			if !inside(target) {
				return fmt.Errorf("%s links out of the folder", h.Name)
			}
			if err := os.Link(target, p); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s: type %c", h.Name, h.Typeflag)
		}
	}
}

// workAlign runs an align job on the lease's sound and words: whisper-cli's JSON to out.
func (r *run) workAlign(ctx context.Context, src string, inputs map[string]string, out string) ([]Tool, error) {
	l := r.l
	var p AlignParams
	if err := json.Unmarshal(l.Params, &p); err != nil {
		return nil, fail("other", "params: %v", err)
	}
	if p.ModelSHA256 != aligner.SHA256 {
		return nil, fail("model_missing", "no aligner with sha256 %s", p.ModelSHA256)
	}
	model := r.w.alignerDir()
	if model == "" {
		return nil, fail("model_missing", "no %s", aligner.Name)
	}
	py, engine := enginePython(r.w.cfg), r.w.engine()
	if py == "" || engine == "" {
		return nil, fail("tool_missing", "no aligner engine")
	}
	if _, ok := aligner.Languages[p.Language]; !ok {
		return nil, fail("other", "language %q", p.Language)
	}
	name := p.Text
	if name == "" {
		name = "text"
	}
	words := inputs[name]
	if words == "" {
		return nil, fail("file_not_found", "input %s", name)
	}
	script, err := alignScript(r.w.cfg.StateDir)
	if err != nil {
		return nil, fail("other", "align.py: %v", err)
	}
	wav := src
	names := []string{engine}
	if l.Job.Source.Container != "wav" {
		wav = filepath.Join(r.dir, "speech.wav")
		if err := r.runTool(ctx, speechArgs(r.w.cfg.FFmpeg, src, wav), "ffmpeg", nil); err != nil {
			return nil, err
		}
		names = append(names, "ffmpeg")
	}
	// On the CPU, float32 where the memory beside what runs allows, else the lean precision, which
	// the queue's RAM gate counts on (aligner.PeakBytes).
	precision := "float32"
	if engine == aligner.EngineTorch && freeMemory() < aligner.TorchFloat32Bytes {
		precision = "lean"
	}
	args := []string{py, "-I", script, "run", "--model", model, "--wav", wav, "--words", words,
		"--language", p.Language, "--out", out, "--threads", strconv.Itoa(r.w.cfg.CPUThreads), "--precision", precision}
	r.log("info", "worker", strings.Join(args, " "))
	r.env = alignEnv
	if err := r.runTool(ctx, args, "aligner", nil); err != nil {
		var f *failure
		if errors.As(err, &f) && f.code == "encode_failed" && f.extra["exit_code"] == 3 {
			return nil, fail("other", "the aligner refused the words: %s", f.msg)
		}
		return nil, err
	}
	return r.w.toolsNamed(names...), nil
}

// freeMemory is what a new process may take: in a container its cgroup's limit less its use
// (MemAvailable is the host's, 2026-10-28: encoder's 14 GiB against the worker's 10g), else
// what memoryAvailable says; 0 unknown.
func freeMemory() int64 {
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/proc/meminfo"); err == nil {
			if total, used := linuxMemory(string(data), cgroupRoot); total > 0 && used > 0 {
				return max(total-used, 0)
			}
		}
	}
	return memoryAvailable()
}

// hardwareMemory is the machine's memory, or the cgroup's limit where lower; 0 unknown.
func hardwareMemory() int64 {
	var hw struct {
		MemoryBytes int64 `json:"memory_bytes"`
	}
	json.Unmarshal(hardware(), &hw)
	return hw.MemoryBytes
}

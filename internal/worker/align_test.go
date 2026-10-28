package worker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kyrios/encode-queue/internal/aligner"
)

// tgz is a gzipped tarball of these entries: a name ending in "/" is a folder, "->" a symlink.
func tgz(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range entries {
		h := &tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}
		switch {
		case strings.HasSuffix(name, "/"):
			h.Typeflag, h.Size = tar.TypeDir, 0
		case strings.HasPrefix(body, "->"):
			h.Typeflag, h.Linkname, h.Size = tar.TypeSymlink, strings.TrimPrefix(body, "->"), 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			tw.Write([]byte(body))
		}
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

func TestUntarStaysInside(t *testing.T) {
	for name, entries := range map[string]map[string]string{
		"dotdot":   {"python/../../evil": "x"},
		"link out": {"python/bin/x": "->../../../etc/passwd"},
		"abs link": {"python/bin/y": "->/etc/passwd"},
	} {
		dir := t.TempDir()
		p := filepath.Join(dir, "a.tar.gz")
		os.WriteFile(p, tgz(t, entries), 0o644)
		err := untar(p, filepath.Join(dir, "out"))
		if name == "dotdot" {
			// "/../.." cleans to the root: the file lands inside, never outside.
			if _, serr := os.Stat(filepath.Join(dir, "evil")); err == nil && serr == nil {
				t.Errorf("%s: a file outside", name)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: unpacked", name)
		}
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "a.tar.gz")
	os.WriteFile(p, tgz(t, map[string]string{"python/": "", "python/bin/python3.12": "#!/bin/sh\n", "python/bin/python3": "->python3.12"}), 0o644)
	if err := untar(p, filepath.Join(dir, "out")); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "out/python/bin/python3")); err != nil || fi.Mode()&0o111 == 0 {
		t.Fatalf("python3: %v %v", fi, err)
	}
}

// The Mac's engine: python-build-standalone's tarball by its sha256, then pip with the hashes
// of RequirementsMLX, binaries only, nothing else; `.ready` once whole.
func TestInstallEngine(t *testing.T) {
	py := "#!/bin/sh\necho \"$@\" > \"$(dirname \"$0\")/../../pip-args\"\n"
	body := tgz(t, map[string]string{"python/bin/python3": py})
	sum := sha256.Sum256(body)
	old := aligner.PythonDarwinArm64
	t.Cleanup(func() { aligner.PythonDarwinArm64 = old })
	aligner.PythonDarwinArm64 = aligner.Python{URL: "https://github.com/astral-sh/python-build-standalone/releases/download/x/cpython.tar.gz",
		SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(body))}
	hf := &hfStandIn{body: body}
	var mu sync.Mutex
	var states []string
	w := &Worker{cfg: Config{StateDir: t.TempDir(), Log: slog.New(slog.DiscardHandler), FetchClient: &http.Client{Transport: hf},
		Events: func(e map[string]any) {
			mu.Lock()
			if st := e["state"].(string); len(states) == 0 || states[len(states)-1] != st {
				states = append(states, st)
			}
			mu.Unlock()
		}}}
	if err := w.installEngine(context.Background()); err != nil {
		t.Fatal(err)
	}
	dir := engineDir(w.cfg)
	args, _ := os.ReadFile(filepath.Join(dir, "pip-args"))
	for _, want := range []string{"-I -m pip install", "--no-deps", "--require-hashes", "--only-binary=:all:", "-r " + filepath.Join(dir, "requirements.txt")} {
		if !strings.Contains(string(args), want) {
			t.Errorf("pip's arguments lack %q: %s", want, args)
		}
	}
	if req, _ := os.ReadFile(filepath.Join(dir, "requirements.txt")); string(req) != string(aligner.RequirementsMLX) {
		t.Error("requirements")
	}
	if !fileExists(filepath.Join(dir, ".ready")) || strings.Join(states, " ") != "downloading installing" {
		t.Fatalf("ready: %v, events %v", fileExists(filepath.Join(dir, ".ready")), states)
	}
	// Set up once: a second call fetches nothing.
	if err := w.installEngine(context.Background()); err != nil || len(hf.seen) != 1 {
		t.Fatalf("again: %v, %d requests", err, len(hf.seen))
	}
	// A tarball with another sha256 is refused.
	aligner.PythonDarwinArm64.SHA256 = strings.Repeat("0", 64)
	w.cfg.StateDir = t.TempDir()
	if err := w.installEngine(context.Background()); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("a wrong tarball: %v", err)
	}
}

func TestDetectAlignerModel(t *testing.T) {
	old := aligner.Files
	t.Cleanup(func() { aligner.Files = old })
	models := t.TempDir()
	cache := filepath.Join(t.TempDir(), "cache.json")
	if dir, err := DetectAlignerModel(models, cache); dir != "" || err != nil {
		t.Fatalf("no folder: %q %v", dir, err)
	}
	md := filepath.Join(models, aligner.Dir)
	os.MkdirAll(md, 0o755)
	os.WriteFile(filepath.Join(md, "config.json"), []byte("{}"), 0o644)
	s := sha256.Sum256([]byte("{}"))
	aligner.Files = []aligner.File{{Name: "config.json", SHA256: hex.EncodeToString(s[:]), Bytes: 2}, {Name: "vocab.json", SHA256: hex.EncodeToString(s[:]), Bytes: 2}}
	if dir, err := DetectAlignerModel(models, cache); dir != "" || err != nil {
		t.Fatalf("a file missing: %q %v", dir, err)
	}
	os.WriteFile(filepath.Join(md, "vocab.json"), []byte("[]"), 0o644)
	if _, err := DetectAlignerModel(models, cache); err == nil {
		t.Fatal("a file with another sha256")
	}
	os.WriteFile(filepath.Join(md, "vocab.json"), []byte("{}"), 0o644)
	if dir, err := DetectAlignerModel(models, cache); dir != md || err != nil {
		t.Fatalf("whole: %q %v", dir, err)
	}
}

// fetchAligner on a read-only folder (encoder's /models) fetches nothing and fails nothing.
func TestAlignerModelOnAReadOnlyFolder(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes anywhere")
	}
	ro := t.TempDir()
	os.Chmod(ro, 0o555)
	t.Cleanup(func() { os.Chmod(ro, 0o755) })
	var got []map[string]any
	w := &Worker{cfg: Config{StateDir: t.TempDir(), ModelsDir: ro, Log: slog.New(slog.DiscardHandler), FetchClient: offlineClient,
		Events: func(e map[string]any) { got = append(got, e) }}}
	if dir, err := w.fetchAligner(context.Background()); dir != "" || err != nil {
		t.Fatalf("%q %v", dir, err)
	}
	if len(got) != 1 || got[0]["state"] != "unavailable" || got[0]["name"] != aligner.Name {
		t.Fatalf("events %+v", got)
	}
}

var offlineClient = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
	return nil, os.ErrPermission
})}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestEnginePython(t *testing.T) {
	cfg := Config{StateDir: t.TempDir(), AlignerPython: "/opt/aligner/python/bin/python3"}
	if enginePython(cfg) != "/opt/aligner/python/bin/python3" {
		t.Fatal("configured")
	}
	cfg.AlignerPython = "off"
	if enginePython(cfg) != "" {
		t.Fatal("off")
	}
	cfg.AlignerPython = ""
	if enginePython(cfg) != "" {
		t.Fatal("nothing set up yet")
	}
	if autoEngine(cfg) {
		os.MkdirAll(engineDir(cfg), 0o755)
		os.WriteFile(filepath.Join(engineDir(cfg), ".ready"), nil, 0o644)
		if enginePython(cfg) != filepath.Join(engineDir(cfg), "python/bin/python3") {
			t.Fatal("the Mac's own")
		}
	}
}

// TestRealMacEngine sets up the Mac's real engine in EQ_REAL_ALIGNER_STATE from the internet
// (python-build-standalone and the pinned wheels, about 300 MB) and asks it its version.
func TestRealMacEngine(t *testing.T) {
	state := os.Getenv("EQ_REAL_ALIGNER_STATE")
	if state == "" || !autoEngine(Config{}) {
		t.Skip("EQ_REAL_ALIGNER_STATE on an Apple-silicon Mac")
	}
	w := &Worker{cfg: Config{StateDir: state, Log: slog.Default(), Events: func(map[string]any) {}}}
	if err := w.installEngine(context.Background()); err != nil {
		t.Fatal(err)
	}
	tool, problem := DetectAligner(context.Background(), w.cfg)
	if tool == nil || *tool != (Tool{"mlx-qwen3-asr", "0.4.4"}) {
		t.Fatalf("%+v %s", tool, problem)
	}
}

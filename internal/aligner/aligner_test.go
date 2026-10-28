package aligner

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestManifestIsTheModelsSHA256(t *testing.T) {
	if got := Manifest(Files); got != SHA256 {
		t.Fatalf("manifest %s, SHA256 %s", got, SHA256)
	}
	if Bytes() != 1840013484 {
		t.Fatalf("bytes %d", Bytes())
	}
	pinned := regexp.MustCompile(`^https://huggingface\.co/Qwen/Qwen3-ForcedAligner-0\.6B/resolve/[0-9a-f]{40}/[a-z_.]+$`)
	for _, f := range Files {
		if !pinned.MatchString(f.URL()) {
			t.Errorf("%s: %s is not pinned", f.Name, f.URL())
		}
	}
}

func TestEnginesAndLanguages(t *testing.T) {
	if Rank(EngineMLX) != 0 || Rank(EngineTorch) != 1 || Rank("whisper.cpp") != -1 {
		t.Fatal("rank")
	}
	for _, e := range Engines {
		if PeakBytes[e] <= 0 || Speed[e] <= 0 || LoadS[e] <= 0 {
			t.Errorf("%s: no peak, speed or load", e)
		}
	}
	// align.py's own table, which the worker runs with.
	for code, name := range Languages {
		if !strings.Contains(string(Script), `"`+code+`": "`+name+`"`) {
			t.Errorf("align.py lacks %s: %s", code, name)
		}
	}
	if !strings.Contains(string(Script), "PIECE_S = 30.0") || PieceS != 30 {
		t.Error("pieces of 30 s on both sides")
	}
}

func TestRequirementsAreHashedAndPinned(t *testing.T) {
	for name, req := range map[string]string{"mlx": string(RequirementsMLX)} {
		pkgs := 0
		for _, line := range strings.Split(req, "\n") {
			if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "    --hash=") {
				continue
			}
			if !regexp.MustCompile(`^[a-z0-9-]+==[0-9][0-9a-z.+]* \\$`).MatchString(line) {
				t.Errorf("%s: %q is not pinned", name, line)
			}
			pkgs++
		}
		if pkgs < 3 || !strings.Contains(req, "mlx-qwen3-asr==0.4.4") || !strings.Contains(req, "mlx==0.32.4") {
			t.Errorf("%s: %d packages", name, pkgs)
		}
	}
	if EngineID(PythonDarwinArm64, RequirementsMLX) == EngineID(PythonDarwinArm64, append(RequirementsMLX, 'x')) {
		t.Error("a new requirements file must be a new engine")
	}
}

// TestScript runs align.py's own tests where a python3 is (CI has one): the pieces, the
// placing of the aligner's words and whisper-cli's shape, with no engine.
func TestScript(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("no python3")
	}
	out, err := exec.Command(py, "-m", "unittest", "-q", "test_align").CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

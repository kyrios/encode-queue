package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Tool is a tool and its exact version string (helper.yaml, Tool).
type Tool struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Model is a whisper model by its sha256.
type Model struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	path   string
}

// Capabilities is what this worker runs.
type Capabilities struct {
	AppVersion string          `json:"app_version"`
	OS         string          `json:"os"`
	Tools      []Tool          `json:"tools"`
	Models     []Model         `json:"models,omitempty"`
	Fonts      []string        `json:"fonts,omitempty"`
	Hardware   json.RawMessage `json:"hardware,omitempty"`
}

var (
	ffmpegVersionRe = regexp.MustCompile(`(?m)^ffmpeg version (\S+)`)
	x265VersionRe   = regexp.MustCompile(`x265 \(build \d+\) - ([^:\s]+)`)
	x264VersionRe   = regexp.MustCompile(`x264 - (core \d+(?: r\d+ [0-9a-f]+)?)`)
)

func runOut(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		return out.Bytes(), fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, strings.TrimSpace(lastLines(errb.String(), 3)))
	}
	return out.Bytes(), nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// oneFrame encodes one black frame with an encoder and returns the stream, whose SEI names
// the encoder's version.
func oneFrame(ctx context.Context, ffmpeg, encoder, format string, extra ...string) ([]byte, error) {
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=64x64:d=0.04",
		"-frames:v", "1", "-c:v", encoder}
	args = append(args, extra...)
	args = append(args, "-f", format, "-")
	return runOut(ctx, ffmpeg, args...)
}

// DetectTools reads the versions of ffmpeg, libx265, libx264 and whisper.cpp as helper.yaml's
// Tool says. A tool that is missing is left out, and its problem returned for the log.
func DetectTools(ctx context.Context, cfg Config) ([]Tool, []string) {
	var tools []Tool
	var problems []string
	out, err := runOut(ctx, cfg.FFmpeg, "-version")
	if m := ffmpegVersionRe.FindSubmatch(out); err == nil && m != nil {
		tools = append(tools, Tool{"ffmpeg", string(m[1])})
		if s, err := oneFrame(ctx, cfg.FFmpeg, "libx265", "hevc", "-x265-params", "log-level=error"); err == nil {
			if m := x265VersionRe.FindSubmatch(s); m != nil {
				tools = append(tools, Tool{"libx265", string(m[1])})
			} else {
				problems = append(problems, "libx265: no version in its stream")
			}
		} else {
			problems = append(problems, "libx265: "+err.Error())
		}
		if s, err := oneFrame(ctx, cfg.FFmpeg, "libx264", "h264"); err == nil {
			if m := x264VersionRe.FindSubmatch(s); m != nil {
				tools = append(tools, Tool{"libx264", string(m[1])})
			} else {
				problems = append(problems, "libx264: no version in its stream")
			}
		} else {
			problems = append(problems, "libx264: "+err.Error())
		}
	} else {
		problems = append(problems, fmt.Sprintf("ffmpeg (%s): %v", cfg.FFmpeg, err))
	}
	if cfg.WhisperVersion != "" {
		if _, err := exec.LookPath(cfg.WhisperCLI); err == nil {
			tools = append(tools, Tool{"whisper.cpp", cfg.WhisperVersion})
		} else {
			problems = append(problems, fmt.Sprintf("whisper.cpp: %s not found", cfg.WhisperCLI))
		}
	}
	return tools, problems
}

type modelCacheEntry struct {
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mtime"`
	SHA256  string    `json:"sha256"`
}

// DetectModels finds the whisper models in dir by their sha256, caching each by size and
// time so a restart does not read gigabytes again.
func DetectModels(dir, cacheFile string) ([]Model, error) {
	if dir == "" {
		return nil, nil
	}
	cache := map[string]modelCacheEntry{}
	if data, err := os.ReadFile(cacheFile); err == nil {
		json.Unmarshal(data, &cache)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var models []Model
	changed := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".bin") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		c, ok := cache[p]
		if !ok || c.Size != fi.Size() || !c.ModTime.Equal(fi.ModTime()) {
			sum, err := fileSHA256(p)
			if err != nil {
				return nil, err
			}
			c = modelCacheEntry{Size: fi.Size(), ModTime: fi.ModTime(), SHA256: sum}
			cache[p] = c
			changed = true
		}
		models = append(models, Model{Name: e.Name(), SHA256: c.SHA256, path: p})
	}
	if changed && cacheFile != "" {
		if data, err := json.Marshal(cache); err == nil {
			os.WriteFile(cacheFile, data, 0o600)
		}
	}
	sort.Slice(models, func(i, j int) bool { return models[i].Name < models[j].Name })
	return models, nil
}

// DetectFonts lists the font families fontconfig knows, plus extra.
func DetectFonts(ctx context.Context, extra []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(f string) {
		f = strings.TrimSpace(f)
		if f != "" && !strings.HasPrefix(f, ".") && !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	for _, f := range extra {
		add(f)
	}
	if data, err := runOut(ctx, "fc-list", ":", "family"); err == nil {
		sc := bufio.NewScanner(bytes.NewReader(data))
		for sc.Scan() {
			for _, f := range strings.Split(sc.Text(), ",") {
				add(strings.ReplaceAll(f, `\-`, "-"))
			}
		}
	}
	sort.Strings(out)
	return out
}

func osName() string {
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
			return "macOS " + strings.TrimSpace(string(out))
		}
		return "macOS"
	}
	if data, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if v, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
				return strings.Trim(v, `"`) + " (" + runtime.GOOS + "/" + runtime.GOARCH + ")"
			}
		}
	}
	return runtime.GOOS + "/" + runtime.GOARCH
}

func hardware() json.RawMessage {
	hw := map[string]any{"cpus": runtime.NumCPU()}
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			hw["chip"] = strings.TrimSpace(string(out))
		}
		for key, name := range map[string]string{"hw.perflevel0.physicalcpu": "performance_cores", "hw.perflevel1.physicalcpu": "efficiency_cores", "hw.memsize": "memory_bytes"} {
			if out, err := exec.Command("sysctl", "-n", key).Output(); err == nil {
				var n int64
				fmt.Sscan(strings.TrimSpace(string(out)), &n)
				hw[name] = n
			}
		}
	} else if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		var kb int64
		fmt.Sscanf(string(data), "MemTotal: %d kB", &kb)
		hw["memory_bytes"] = kb * 1024
		if c, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(c), "\n") {
				if v, ok := strings.CutPrefix(line, "model name"); ok {
					hw["chip"] = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), ":"))
					break
				}
			}
		}
	}
	data, _ := json.Marshal(hw)
	return data
}

// DetectCapabilities is everything this worker says it runs.
func DetectCapabilities(ctx context.Context, cfg Config) (Capabilities, []string) {
	tools, problems := DetectTools(ctx, cfg)
	models, err := DetectModels(cfg.ModelsDir, filepath.Join(cfg.StateDir, "models-cache.json"))
	if err != nil {
		problems = append(problems, "models: "+err.Error())
	}
	return Capabilities{AppVersion: cfg.Version, OS: osName(), Tools: tools, Models: models,
		Fonts: DetectFonts(ctx, cfg.Fonts), Hardware: hardware()}, problems
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

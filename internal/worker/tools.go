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

	"golang.org/x/sys/unix"
)

// Tool is a tool and its exact version string (helper.yaml, Tool).
type Tool struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// HostApp is the app a worker runs inside.
type HostApp struct {
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
	HostApp    *HostApp        `json:"host_app,omitempty"`
	Tools      []Tool          `json:"tools"`
	Models     []Model         `json:"models,omitempty"`
	Fonts      []string        `json:"fonts,omitempty"`
	Hardware   json.RawMessage `json:"hardware,omitempty"`
	// Calibration: this worker's fps on the calibration clip per rung class (v0.2.5).
	Calibration *Calibration `json:"calibration,omitempty"`
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
	wv := cfg.WhisperVersion
	if cli, err := exec.LookPath(cfg.WhisperCLI); err != nil {
		problems = append(problems, fmt.Sprintf("whisper.cpp: %s not found", cfg.WhisperCLI))
	} else {
		if wv == "" {
			wv = toolchainVersion(cli, "whisper.cpp")
		}
		if wv != "" {
			tools = append(tools, Tool{"whisper.cpp", wv})
		} else {
			problems = append(problems, "whisper.cpp: no version (EQ_WHISPER_VERSION, or versions.txt beside whisper-cli)")
		}
	}
	return tools, problems
}

// toolchainVersion reads a tool's version from the toolchain's versions.txt beside a binary
// ("whisper.cpp v1.9.5"), since whisper-cli cannot print its own.
func toolchainVersion(binary, name string) string {
	if p, err := filepath.EvalSymlinks(binary); err == nil {
		binary = p
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(binary), "versions.txt"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == name {
			return strings.Join(f[1:], " ")
		}
	}
	return ""
}

// hasAss is whether ffmpeg was built with libass, which burnt-in captions need.
func hasAss(ctx context.Context, ffmpeg string) bool {
	out, err := runOut(ctx, ffmpeg, "-hide_banner", "-filters")
	return err == nil && regexp.MustCompile(`(?m)^\s*\S+\s+ass\s`).Match(out)
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
	hw := map[string]any{"cpus": runtime.NumCPU(), "cpu_threads": runtime.NumCPU(), "arch": runtime.GOARCH}
	var u unix.Utsname
	if unix.Uname(&u) == nil {
		hw["kernel"] = unix.ByteSliceToString(u.Release[:])
	}
	if runtime.GOOS == "darwin" {
		hw["os_name"] = "macOS"
		if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
			hw["os_version"] = strings.TrimSpace(string(out))
		}
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			hw["chip"] = strings.TrimSpace(string(out))
		}
		for key, name := range map[string]string{"hw.perflevel0.physicalcpu": "performance_cores", "hw.perflevel1.physicalcpu": "efficiency_cores",
			"hw.memsize": "memory_bytes", "hw.physicalcpu": "cpu_cores"} {
			if out, err := exec.Command("sysctl", "-n", key).Output(); err == nil {
				var n int64
				fmt.Sscan(strings.TrimSpace(string(out)), &n)
				hw[name] = n
			}
		}
		data, _ := json.Marshal(hw)
		return data
	}
	if data, err := os.ReadFile("/etc/os-release"); err == nil {
		name, version := osRelease(string(data))
		if name != "" {
			hw["os_name"] = name
		}
		if version != "" {
			hw["os_version"] = version
		}
	}
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		var kb int64
		fmt.Sscanf(string(data), "MemTotal: %d kB", &kb)
		hw["memory_bytes"] = kb * 1024
	}
	if c, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		chip, cores := cpuInfo(string(c))
		if chip != "" {
			hw["chip"] = chip
		}
		if cores > 0 {
			hw["cpu_cores"] = cores
		}
	}
	data, _ := json.Marshal(hw)
	return data
}

// memoryUsed is the RAM in use: on macOS active, wired and compressed pages (what Activity
// Monitor calls used, less the app cache), on Linux MemTotal less MemAvailable; 0 unknown.
func memoryUsed() int64 {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("vm_stat").Output()
		if err != nil {
			return 0
		}
		return vmStatUsed(string(out))
	}
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	return meminfoUsed(string(data))
}

// memoryAvailable is the RAM a new process could get: Linux MemAvailable, macOS the memory
// less what is in use; 0 unknown.
func memoryAvailable() int64 {
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0
		}
		var total int64
		fmt.Sscan(strings.TrimSpace(string(out)), &total)
		if used := memoryUsed(); used > 0 && used < total {
			return total - used
		}
		return 0
	}
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	return meminfoField(string(data), "MemAvailable")
}

func meminfoField(data, field string) int64 {
	for _, line := range strings.Split(data, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && k == field {
			var n int64
			fmt.Sscan(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), &n)
			return n * 1024
		}
	}
	return 0
}

func vmStatUsed(out string) int64 {
	page := int64(4096)
	var used int64
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "page size of") {
			fmt.Sscanf(line[strings.Index(line, "page size of"):], "page size of %d bytes", &page)
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "Pages active", "Pages wired down", "Pages occupied by compressor":
			var n int64
			fmt.Sscan(strings.TrimSuffix(strings.TrimSpace(v), "."), &n)
			used += n
		}
	}
	return used * page
}

func meminfoUsed(data string) int64 {
	var total, avail int64
	for _, line := range strings.Split(data, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		var n int64
		fmt.Sscan(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), &n)
		switch k {
		case "MemTotal":
			total = n
		case "MemAvailable":
			avail = n
		}
	}
	if total == 0 || avail == 0 {
		return 0
	}
	return (total - avail) * 1024
}

// osRelease is NAME and VERSION_ID of /etc/os-release.
func osRelease(data string) (name, version string) {
	for _, line := range strings.Split(data, "\n") {
		if v, ok := strings.CutPrefix(line, "NAME="); ok {
			name = strings.Trim(v, `"`)
		}
		if v, ok := strings.CutPrefix(line, "VERSION_ID="); ok {
			version = strings.Trim(v, `"`)
		}
	}
	return name, version
}

// cpuInfo is the first model name in /proc/cpuinfo and the physical cores: distinct pairs of
// physical id and core id (0 when the file has none, as on some ARM boards).
func cpuInfo(data string) (model string, cores int) {
	seen := map[string]bool{}
	phys := ""
	for _, line := range strings.Split(data, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch k {
		case "model name":
			if model == "" {
				model = v
			}
		case "physical id":
			phys = v
		case "core id":
			seen[phys+"/"+v] = true
		}
	}
	return model, len(seen)
}

// DetectCapabilities is everything this worker says it runs.
func DetectCapabilities(ctx context.Context, cfg Config) (Capabilities, []string) {
	tools, problems := DetectTools(ctx, cfg)
	models, err := DetectModels(cfg.ModelsDir, filepath.Join(cfg.StateDir, "models-cache.json"))
	if err != nil {
		problems = append(problems, "models: "+err.Error())
	}
	var fonts []string
	if hasAss(ctx, cfg.FFmpeg) {
		fonts = DetectFonts(ctx, cfg.Fonts)
	} else {
		problems = append(problems, "ffmpeg has no libass: no jobs with burnt-in captions")
	}
	var host *HostApp
	if cfg.HostAppName != "" {
		host = &HostApp{Name: cfg.HostAppName, Version: cfg.HostAppVersion}
	}
	return Capabilities{AppVersion: cfg.Version, OS: osName(), HostApp: host, Tools: tools, Models: models,
		Fonts: fonts, Hardware: hardware()}, problems
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

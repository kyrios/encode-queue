// encode-worker takes jobs from encode-queue through its own userspace WireGuard and runs
// them at nice 19.
//
//	encode-worker pubkey        print this worker's public key (made on first use), for pairing
//	encode-worker versions      print what this worker runs, as the queue will see it
//	encode-worker selftest      encode, cut and remux a short clip with this machine's ffmpeg and
//	                            check each result; exit 1 with the problems
//	encode-worker healthcheck   exit 0 if the queue answered within the last two minutes
//	encode-worker [run]         take jobs
//
// Every setting is a flag or an environment variable (the flag wins); the README lists them.
// `--key-stdin --control stdio` is how an app embeds it (README, "Embedding").
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kyrios/encode-queue/internal/config"
	"github.com/kyrios/encode-queue/internal/tunnel"
	"github.com/kyrios/encode-queue/internal/worker"
)

var version = "dev"

type flags struct {
	version                               bool
	keyStdin                              bool
	stateDir, keyFile, tunnelFile, tunnel string
	endpoint, workDir                     string
	name, appVersion                      string
	hostAppName, hostAppVersion           string
	threads, jobs                         int
	gpu                                   string
	calibrate                             string
	ffmpeg, ffprobe, whisperCLI, models   string
	whisperVersion, fonts, control        string
	toolchainVersions                     string
}

func main() {
	var f flags
	env := config.New()
	flag.BoolVar(&f.version, "version", false, "print the version and exit")
	flag.BoolVar(&f.keyStdin, "key-stdin", false, "read the WireGuard private key (base64) from the first line of standard input")
	flag.StringVar(&f.stateDir, "state-dir", env.String("EQ_WORKER_STATE_DIR", defaultStateDir()), "state: key, tunnel line, model cache (EQ_WORKER_STATE_DIR)")
	flag.StringVar(&f.keyFile, "key-file", env.String("EQ_WORKER_KEY_FILE", ""), "the WireGuard private key, made there if missing (EQ_WORKER_KEY_FILE; default STATE/wireguard.key)")
	flag.StringVar(&f.tunnel, "tunnel", env.String("EQ_TUNNEL", ""), "the tunnel line (EQ_TUNNEL)")
	flag.StringVar(&f.tunnelFile, "tunnel-file", env.String("EQ_TUNNEL_FILE", ""), "a file holding the tunnel line (EQ_TUNNEL_FILE; default STATE/tunnel.conf)")
	flag.StringVar(&f.endpoint, "endpoint", env.String("EQ_TUNNEL_ENDPOINT", ""), "another host:port for the queue's UDP (EQ_TUNNEL_ENDPOINT)")
	flag.StringVar(&f.workDir, "work-dir", env.String("EQ_WORK_DIR", ""), "scratch space for sources and results (EQ_WORK_DIR; default STATE/work)")
	flag.StringVar(&f.name, "name", env.String("EQ_WORKER_NAME", ""), "this worker's name, for its log (the queue keeps the name it was paired with)")
	flag.StringVar(&f.appVersion, "app-version", env.String("EQ_APP_VERSION", ""), "the version the queue compares with its minimum (EQ_APP_VERSION; default the worker's)")
	flag.StringVar(&f.hostAppName, "host-app-name", env.String("EQ_HOST_APP_NAME", ""), "the app this worker runs inside, for the Encoder page (EQ_HOST_APP_NAME, e.g. Slipstream)")
	flag.StringVar(&f.hostAppVersion, "host-app-version", env.String("EQ_HOST_APP_VERSION", ""), "that app's version (EQ_HOST_APP_VERSION)")
	flag.IntVar(&f.threads, "threads", env.Int("EQ_CPU_THREADS", 0), "x265's pool and whisper.cpp's threads, 0 for the tools' own (EQ_CPU_THREADS)")
	flag.StringVar(&f.gpu, "gpu", env.String("EQ_GPU", "false"), "whisper.cpp may use the GPU: true or false (EQ_GPU)")
	flag.IntVar(&f.jobs, "jobs", env.Int("EQ_MAX_PARALLEL_JOBS", 1), "jobs at once (EQ_MAX_PARALLEL_JOBS)")
	flag.StringVar(&f.calibrate, "calibrate", env.String("EQ_CALIBRATE", "true"), "encode the calibration clip at pairing and on a tools change: true or false (EQ_CALIBRATE)")
	flag.StringVar(&f.ffmpeg, "ffmpeg", env.String("EQ_FFMPEG", "ffmpeg"), "ffmpeg (EQ_FFMPEG)")
	flag.StringVar(&f.ffprobe, "ffprobe", env.String("EQ_FFPROBE", "ffprobe"), "ffprobe, for the toolchain's set (EQ_FFPROBE)")
	flag.StringVar(&f.whisperCLI, "whisper-cli", env.String("EQ_WHISPER_CLI", "whisper-cli"), "whisper-cli (EQ_WHISPER_CLI)")
	flag.StringVar(&f.models, "whisper-models", env.String("EQ_WHISPER_MODELS_DIR", ""), "the whisper models, found by sha256 (EQ_WHISPER_MODELS_DIR)")
	flag.StringVar(&f.whisperVersion, "whisper-version", env.String("EQ_WHISPER_VERSION", ""), "whisper.cpp's release, if no versions.txt is beside whisper-cli (EQ_WHISPER_VERSION)")
	flag.StringVar(&f.toolchainVersions, "toolchain-versions", env.String("EQ_TOOLCHAIN_VERSIONS", ""), "the toolchain's versions.txt, when it is not beside ffmpeg and whisper-cli: Slipstream's bundle (EQ_TOOLCHAIN_VERSIONS)")
	flag.StringVar(&f.fonts, "fonts", env.String("EQ_FONTS", ""), "more font families, comma-separated (EQ_FONTS)")
	flag.StringVar(&f.control, "control", env.String("EQ_CONTROL_SOCKET", ""), "stdio for JSON lines on standard input and output, or a Unix socket path (EQ_CONTROL_SOCKET)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: encode-worker [flags] [pubkey|versions|healthcheck|run]\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if f.version {
		fmt.Println(version)
		return
	}
	if err := env.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "encode-worker:", err)
		os.Exit(2)
	}
	if err := run(flag.Arg(0), f, env); err != nil {
		fmt.Fprintln(os.Stderr, "encode-worker:", err)
		os.Exit(1)
	}
}

func defaultStateDir() string {
	if d, err := os.UserConfigDir(); err == nil {
		return filepath.Join(d, "encode-worker")
	}
	return ".encode-worker"
}

func run(cmd string, f flags, env *config.Env) error {
	var level slog.Level
	level.UnmarshalText([]byte(env.String("EQ_LOG_LEVEL", "info")))
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if f.keyFile == "" {
		f.keyFile = filepath.Join(f.stateDir, "wireguard.key")
	}
	var fonts []string
	for _, s := range strings.Split(f.fonts, ",") {
		if s = strings.TrimSpace(s); s != "" {
			fonts = append(fonts, s)
		}
	}
	appVersion := f.appVersion
	if appVersion == "" {
		appVersion = strings.TrimPrefix(version, "v")
	}
	cfg := worker.Config{
		Version:           appVersion,
		HostAppName:       f.hostAppName,
		HostAppVersion:    f.hostAppVersion,
		StateDir:          f.stateDir,
		WorkDir:           f.workDir,
		FFmpeg:            f.ffmpeg,
		WhisperCLI:        f.whisperCLI,
		WhisperVersion:    f.whisperVersion,
		ToolchainVersions: f.toolchainVersions,
		ModelsDir:         f.models,
		Fonts:             fonts,
		CPUThreads:        f.threads,
		GPU:               f.gpu == "true" || f.gpu == "1" || f.gpu == "yes",
		MaxParallel:       f.jobs,
		Calibrate:         f.calibrate == "true",
		Log:               log,
	}
	if cmd == "healthcheck" {
		fi, err := os.Stat(filepath.Join(f.stateDir, "health"))
		if err != nil {
			return errors.New("the queue never answered")
		}
		if age := time.Since(fi.ModTime()); age > 2*time.Minute {
			return fmt.Errorf("the queue last answered %s ago", age.Round(time.Second))
		}
		return nil
	}
	if err := os.MkdirAll(f.stateDir, 0o700); err != nil {
		return err
	}
	stdin := bufio.NewReader(os.Stdin)
	var key tunnel.Key
	var err error
	if f.keyStdin {
		line, rerr := stdin.ReadString('\n')
		if rerr != nil && line == "" {
			return fmt.Errorf("--key-stdin: no key on standard input: %v", rerr)
		}
		if key, err = tunnel.ParseKey(line); err != nil {
			return fmt.Errorf("--key-stdin: %w", err)
		}
	} else if cmd == "pubkey" || cmd == "" || cmd == "run" {
		if key, err = tunnel.LoadOrCreateKey(f.keyFile); err != nil {
			return err
		}
	}
	switch cmd {
	case "pubkey":
		fmt.Println(key.Public())
		return nil
	case "versions":
		caps, problems := worker.DetectCapabilities(context.Background(), cfg)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "warning:", p)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(caps)
	case "selftest":
		res, problems := worker.SelfTest(context.Background(), cfg)
		res["problems"] = problems
		if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
			return err
		}
		if len(problems) > 0 {
			return fmt.Errorf("selftest: %d problems", len(problems))
		}
		return nil
	case "", "run":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}

	line := f.tunnel
	if line == "" {
		path := f.tunnelFile
		if path == "" {
			path = filepath.Join(f.stateDir, "tunnel.conf")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("no tunnel configuration: set EQ_TUNNEL or put the line in %s (%v)", path, err)
		}
		line = strings.TrimSpace(string(data))
	}
	tcfg, err := tunnel.ParseLine(line)
	if err != nil {
		return fmt.Errorf("the tunnel configuration: %w", err)
	}
	if err := worker.Renice(); err != nil {
		log.Warn("nice 19", "err", err)
	}
	stdio := f.control == "stdio"
	if stdio {
		var mu sync.Mutex
		out := json.NewEncoder(os.Stdout)
		cfg.Events = func(ev map[string]any) {
			mu.Lock()
			defer mu.Unlock()
			out.Encode(ev)
		}
	}
	tc, err := tunnel.Dial(key, tcfg, f.endpoint, log)
	if err != nil {
		return fmt.Errorf("the tunnel: %w", err)
	}
	defer tc.Close()
	api, err := worker.NewClient(tc.HTTP, tcfg.API)
	if err != nil {
		return err
	}
	w, err := worker.New(cfg, api)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	switch {
	case stdio:
		go readCommands(stdin, w, log)
	case f.control != "":
		go func() {
			if err := w.ServeControl(ctx, f.control); err != nil {
				log.Error("the control socket", "path", f.control, "err", err)
			}
		}()
	}
	log.Info("encode-worker up", "version", version, "name", f.name, "address", tcfg.Address, "api", tcfg.API, "public_key", key.Public())
	return w.Run(ctx)
}

// readCommands reads Slipstream's JSON lines: pause, resume, limits, quit; the end of input
// quits too.
func readCommands(r *bufio.Reader, w *worker.Worker, log *slog.Logger) {
	dec := json.NewDecoder(r)
	for {
		var c struct {
			Cmd     string     `json:"cmd"`
			Until   *time.Time `json:"until"`
			Threads int        `json:"threads"`
			GPU     bool       `json:"gpu"`
			Jobs    int        `json:"jobs"`
		}
		if err := dec.Decode(&c); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Warn("standard input", "err", err)
			}
			w.Quit()
			return
		}
		switch c.Cmd {
		case "pause":
			if c.Until != nil {
				w.Pause(*c.Until)
			}
		case "resume":
			w.Resume()
		case "limits":
			w.SetLimits(c.Threads, c.GPU, c.Jobs)
		case "quit":
			w.Quit()
			return
		default:
			log.Warn("unknown command", "cmd", c.Cmd)
		}
	}
}

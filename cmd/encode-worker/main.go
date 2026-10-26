// encode-worker takes jobs from encode-queue through its own userspace WireGuard and runs
// them at nice 19.
//
//	encode-worker pubkey     print this worker's public key (made on first use), for pairing
//	encode-worker versions   print what this worker runs, as the queue will see it
//	encode-worker [run]      take jobs (the tunnel configuration from EQ_TUNNEL or EQ_TUNNEL_FILE)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kyrios/encode-queue/internal/config"
	"github.com/kyrios/encode-queue/internal/tunnel"
	"github.com/kyrios/encode-queue/internal/worker"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: encode-worker [-version] [pubkey|versions|run]\n\nConfigured from the environment; see the README.\n")
	}
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(flag.Arg(0)); err != nil {
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

func run(cmd string) error {
	env := config.New()
	var level slog.Level
	level.UnmarshalText([]byte(env.String("EQ_LOG_LEVEL", "info")))
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	stateDir := env.String("EQ_WORKER_STATE_DIR", defaultStateDir())
	keyFile := env.String("EQ_WORKER_KEY_FILE", filepath.Join(stateDir, "wireguard.key"))
	var fonts []string
	for _, f := range strings.Split(env.String("EQ_FONTS", ""), ",") {
		if f = strings.TrimSpace(f); f != "" {
			fonts = append(fonts, f)
		}
	}
	cfg := worker.Config{
		Version:        strings.TrimPrefix(version, "v"),
		StateDir:       stateDir,
		WorkDir:        env.String("EQ_WORK_DIR", ""),
		FFmpeg:         env.String("EQ_FFMPEG", "ffmpeg"),
		WhisperCLI:     env.String("EQ_WHISPER_CLI", "whisper-cli"),
		WhisperVersion: env.String("EQ_WHISPER_VERSION", ""),
		ModelsDir:      env.String("EQ_WHISPER_MODELS_DIR", ""),
		Fonts:          fonts,
		CPUThreads:     env.Int("EQ_CPU_THREADS", 0),
		GPU:            env.Bool("EQ_GPU", false),
		MaxParallel:    env.Int("EQ_MAX_PARALLEL_JOBS", 1),
		Log:            log,
	}
	if err := env.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return err
	}
	switch cmd {
	case "pubkey":
		k, err := tunnel.LoadOrCreateKey(keyFile)
		if err != nil {
			return err
		}
		fmt.Println(k.Public())
		return nil
	case "versions":
		caps, problems := worker.DetectCapabilities(context.Background(), cfg)
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "warning:", p)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(caps)
	case "", "run":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}

	line := env.String("EQ_TUNNEL", "")
	if line == "" {
		path := env.String("EQ_TUNNEL_FILE", filepath.Join(stateDir, "tunnel.conf"))
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
	key, err := tunnel.LoadOrCreateKey(keyFile)
	if err != nil {
		return err
	}
	if err := worker.Nice(); err != nil {
		log.Warn("nice 19", "err", err)
	}
	tc, err := tunnel.Dial(key, tcfg, env.String("EQ_TUNNEL_ENDPOINT", ""), log)
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
	if sock := env.String("EQ_CONTROL_SOCKET", ""); sock != "" {
		go func() {
			if err := w.ServeControl(ctx, sock); err != nil {
				log.Error("the control socket", "path", sock, "err", err)
			}
		}()
	}
	log.Info("encode-worker up", "version", version, "address", tcfg.Address, "api", tcfg.API, "public_key", key.Public())
	return w.Run(ctx)
}

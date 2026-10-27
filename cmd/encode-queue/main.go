// encode-queue is the queue server: the jobs, their leases and the workers, the pipeline and
// admin API on a TCP listener, and the helper API inside its own userspace WireGuard.
//
// Everything that names an address, a port or a path comes from the environment; the README
// lists the variables.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/kyrios/encode-queue/internal/config"
	"github.com/kyrios/encode-queue/internal/queue"
	"github.com/kyrios/encode-queue/internal/server"
	"github.com/kyrios/encode-queue/internal/tunnel"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: encode-queue [-version] [pubkey|healthcheck]\n\n"+
			"Runs the queue, configured from the environment (see the README).\n"+
			"pubkey prints the queue's WireGuard public key, making the key if there is none.\n"+
			"healthcheck asks the running queue's /healthz on EQ_API_LISTEN and exits 0 if it answers.\n")
	}
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(flag.Arg(0)); err != nil {
		fmt.Fprintln(os.Stderr, "encode-queue:", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	l.UnmarshalText([]byte(level))
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func run(cmd string) error {
	env := config.New()
	log := newLogger(env.String("EQ_LOG_LEVEL", "info"))
	stateDir := env.Required("EQ_STATE_DIR")
	keyFile := env.String("EQ_WG_PRIVATE_KEY_FILE", filepath.Join(stateDir, "wireguard.key"))
	if cmd == "pubkey" {
		if err := env.Err(); err != nil {
			return err
		}
		k, err := tunnel.LoadOrCreateKey(keyFile)
		if err != nil {
			return err
		}
		fmt.Println(k.Public())
		return nil
	}
	if cmd == "healthcheck" {
		return healthcheck(env.Required("EQ_API_LISTEN"), env)
	}
	if cmd != "" {
		return fmt.Errorf("unknown command %q", cmd)
	}

	cfg := queue.Config{
		StateDir:        stateDir,
		SourceDir:       env.Required("EQ_SOURCE_DIR"),
		ResultDir:       env.String("EQ_RESULT_DIR", ""),
		Lease:           env.Seconds("EQ_LEASE_S"),
		RenewEvery:      env.Seconds("EQ_RENEW_EVERY_S"),
		LongPoll:        env.Seconds("EQ_LONG_POLL_S"),
		PresenceEvery:   env.Seconds("EQ_PRESENCE_EVERY_S"),
		PauseGiveBack:   env.Seconds("EQ_PAUSE_GIVE_BACK_S"),
		DoneKeep:        retention(env),
		MaxFailures:     env.Int("EQ_MAX_FAILURES", 0),
		ExcludeBackoff:  env.Seconds("EQ_EXCLUDE_BACKOFF_S"),
		ResultReserve:   env.Int64("EQ_RESULT_RESERVE_BYTES", 2<<30),
		ReferenceWorker: env.String("EQ_REFERENCE_WORKER", ""),
		MaxParallelJobs: env.Int("EQ_MAX_PARALLEL_JOBS", 0),
		HoldFactor:      float64(env.Int("EQ_HOLD_FACTOR", 0)),
		HoldMin:         env.Seconds("EQ_HOLD_MIN_S"),
		HoldMax:         env.Seconds("EQ_HOLD_MAX_S"),
		WhisperMinSpeed: float64(env.Int("EQ_WHISPER_MIN_SPEED_PCT", 100)) / 100,
		MinAppVersion:   env.String("EQ_MIN_APP_VERSION", ""),
		Endpoint:        env.Required("EQ_WG_ENDPOINT"),
		Keepalive:       env.Int("EQ_WG_KEEPALIVE_S", 25),
		Log:             log,
	}
	apiListen := env.Required("EQ_API_LISTEN")
	metricsListen := env.String("EQ_METRICS_LISTEN", "")
	wgPort := env.Int("EQ_WG_LISTEN_PORT", 0)
	if wgPort == 0 {
		env.Required("EQ_WG_LISTEN_PORT")
	}
	tunnelAPIPort := env.Int("EQ_WG_API_PORT", 0)
	if tunnelAPIPort == 0 {
		env.Required("EQ_WG_API_PORT")
	}
	addrText := env.Required("EQ_WG_ADDRESS")
	subnetText := env.Required("EQ_WG_SUBNET")
	pipelineToken := env.SecretFile("EQ_PIPELINE_TOKEN_FILE")
	adminToken := env.SecretFile("EQ_ADMIN_TOKEN_FILE")
	if err := env.Err(); err != nil {
		return err
	}
	addr, err := netip.ParseAddr(addrText)
	if err != nil {
		return fmt.Errorf("EQ_WG_ADDRESS: %w", err)
	}
	subnet, err := netip.ParsePrefix(subnetText)
	if err != nil {
		return fmt.Errorf("EQ_WG_SUBNET: %w", err)
	}
	if !subnet.Contains(addr) {
		return fmt.Errorf("EQ_WG_ADDRESS %s is not in EQ_WG_SUBNET %s", addr, subnet)
	}
	if pipelineToken == adminToken {
		return fmt.Errorf("the pipeline's and the admin's tokens must differ")
	}
	cfg.Subnet, cfg.TunnelAddress = subnet.Masked(), addr
	cfg.API = fmt.Sprintf("http://%s/v1", net.JoinHostPort(addr.String(), fmt.Sprint(tunnelAPIPort)))

	key, err := tunnel.LoadOrCreateKey(keyFile)
	if err != nil {
		return fmt.Errorf("the WireGuard key: %w", err)
	}
	cfg.ServerPublicKey = key.Public().String()

	store, err := queue.Open(cfg)
	if err != nil {
		return err
	}
	door, err := tunnel.OpenDoor(key, addr, wgPort, log)
	if err != nil {
		return fmt.Errorf("WireGuard on UDP %d: %w", wgPort, err)
	}
	defer door.Close()
	setPeers := func(peers []queue.PeerInfo) {
		var ps []tunnel.Peer
		for _, p := range peers {
			k, err := tunnel.ParseKey(p.PublicKey)
			if err != nil {
				log.Error("a peer's key", "address", p.Address, "err", err)
				continue
			}
			ps = append(ps, tunnel.Peer{PublicKey: k, Address: p.Address})
		}
		if err := door.SetPeers(ps); err != nil {
			log.Error("set the peers", "err", err)
		}
	}
	setPeers(store.Peers())
	store.OnPeers = setPeers
	store.PeerStatus = func() map[string]queue.PeerStatus {
		out := map[string]queue.PeerStatus{}
		for k, p := range door.PeerStatus() {
			out[k.String()] = queue.PeerStatus{Endpoint: p.Endpoint, LastHandshake: p.LastHandshake}
		}
		return out
	}

	helperL, err := door.Listen(tunnelAPIPort)
	if err != nil {
		return fmt.Errorf("the helper API in the tunnel: %w", err)
	}
	apiL, err := net.Listen("tcp", apiListen)
	if err != nil {
		return fmt.Errorf("EQ_API_LISTEN %s: %w", apiListen, err)
	}
	var metricsL net.Listener
	if metricsListen != "" {
		if metricsL, err = net.Listen("tcp", metricsListen); err != nil {
			return fmt.Errorf("EQ_METRICS_LISTEN %s: %w", metricsListen, err)
		}
	}

	srv := &server.Server{Store: store, Version: version, PipelineToken: pipelineToken, AdminToken: adminToken, Log: log}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); store.Run(stop) }()
	go func() { defer wg.Done(); store.RunSaver(stop) }()

	log.Info("encode-queue up", "version", version, "api", apiL.Addr().String(), "metrics", metricsListen, "wireguard_udp", wgPort,
		"tunnel", addr.String(), "helper_api", cfg.API, "public_key", cfg.ServerPublicKey,
		"reference_worker", cfg.ReferenceWorker, "peers", len(store.Peers()))
	errc := make(chan error, 3)
	servers := 2
	go func() { errc <- server.Serve(ctx, helperL, srv.HelperHandler(), log) }()
	go func() { errc <- server.Serve(ctx, apiL, srv.AdminHandler(metricsL == nil), log) }()
	if metricsL != nil {
		servers++
		go func() { errc <- server.Serve(ctx, metricsL, srv.MetricsHandler(), log) }()
	}
	n := 0
	var firstErr error
	select {
	case <-ctx.Done():
	case firstErr = <-errc:
		n++
	}
	log.Info("shutting down")
	store.Drain()
	cancel()
	for ; n < servers; n++ {
		if err := <-errc; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	close(stop)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return store.Save()
}

// healthcheck asks the queue's /healthz on its API listener, for the image's HEALTHCHECK
// (distroless has no curl).
func healthcheck(listen string, env *config.Env) error {
	if err := env.Err(); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("EQ_API_LISTEN %q: %w", listen, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("/healthz answered %d", resp.StatusCode)
	}
	return nil
}

// retention is EQ_JOB_RETENTION, or the older EQ_DONE_KEEP_S; 0 is the queue's default.
func retention(env *config.Env) time.Duration {
	if d := env.Duration("EQ_JOB_RETENTION"); d > 0 {
		return d
	}
	return env.Seconds("EQ_DONE_KEEP_S")
}

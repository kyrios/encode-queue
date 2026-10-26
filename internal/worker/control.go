package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"time"
)

// ServeControl serves the local control API (api/worker-control.yaml) on a Unix socket, for
// the app that embeds the worker: its state, pause, resume, quit.
func (w *Worker) ServeControl(ctx context.Context, path string) error {
	os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	os.Chmod(path, 0o600)
	mux := http.NewServeMux()
	reply := func(rw http.ResponseWriter, status int, v any) {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(status)
		json.NewEncoder(rw).Encode(v)
	}
	mux.HandleFunc("GET /status", func(rw http.ResponseWriter, r *http.Request) {
		reply(rw, 200, w.Status())
	})
	mux.HandleFunc("POST /pause", func(rw http.ResponseWriter, r *http.Request) {
		var body struct {
			Until   *time.Time `json:"until"`
			Minutes int        `json:"minutes"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(rw, r.Body, 4096)).Decode(&body); err != nil {
			reply(rw, 422, map[string]string{"error": "want {\"minutes\": N} or {\"until\": time}"})
			return
		}
		until := time.Now().Add(time.Duration(body.Minutes) * time.Minute)
		if body.Until != nil {
			until = *body.Until
		}
		if !until.After(time.Now()) {
			reply(rw, 422, map[string]string{"error": "the pause must end in the future"})
			return
		}
		w.Pause(until)
		reply(rw, 200, w.Status())
	})
	mux.HandleFunc("POST /resume", func(rw http.ResponseWriter, r *http.Request) {
		w.Resume()
		reply(rw, 200, w.Status())
	})
	mux.HandleFunc("POST /quit", func(rw http.ResponseWriter, r *http.Request) {
		reply(rw, 202, map[string]string{"state": "quitting"})
		w.Quit()
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	err = srv.Serve(l)
	os.Remove(path)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

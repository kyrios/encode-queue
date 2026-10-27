// Package server puts the two APIs in front of the queue: the helper API inside the tunnel
// (api/helper.yaml) and the pipeline and admin API on the queue's own listener
// (api/pipeline-admin.yaml).
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kyrios/encode-queue/internal/queue"
)

// Server holds what both APIs need.
type Server struct {
	Store         *queue.Store
	Version       string
	PipelineToken string
	AdminToken    string
	Log           *slog.Logger
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	Code   string `json:"code"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.Encode(v)
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var qe *queue.Error
	if !errors.As(err, &qe) {
		s.Log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		qe = &queue.Error{Status: 500, Code: "internal", Detail: "the queue could not do this; see its log"}
	}
	if qe.Code == "draining" {
		w.Header().Set("Retry-After", "30")
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(qe.Status)
	json.NewEncoder(w).Encode(problem{Type: "about:blank", Title: http.StatusText(qe.Status), Status: qe.Status, Detail: qe.Detail, Code: qe.Code})
}

func invalid(format string, args ...any) error {
	return &queue.Error{Status: 422, Code: "invalid", Detail: fmt.Sprintf(format, args...)}
}

func decode(r *http.Request, limit int64, v any) error {
	body := http.MaxBytesReader(nil, r.Body, limit)
	dec := json.NewDecoder(body)
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return &queue.Error{Status: 413, Code: "too_large", Detail: fmt.Sprintf("at most %d bytes", limit)}
		}
		return invalid("the body is not the JSON this wants: %v", err)
	}
	return nil
}

// preferWait reads `Prefer: wait=N`.
func preferWait(r *http.Request) time.Duration {
	for _, part := range strings.Split(r.Header.Get("Prefer"), ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && strings.EqualFold(k, "wait") {
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
				return time.Duration(n) * time.Second
			}
		}
	}
	return 0
}

func applied(w http.ResponseWriter, wait, max time.Duration) {
	if wait > max {
		wait = max
	}
	w.Header().Set("Preference-Applied", fmt.Sprintf("wait=%d", int(wait/time.Second)))
}

// patchFields reads a merge patch, keeping which fields were there.
func patchFields(r *http.Request) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := decode(r, 64<<10, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func workerPatch(m map[string]json.RawMessage, allowName, allowLimits bool) (queue.WorkerPatch, error) {
	var p queue.WorkerPatch
	for k, raw := range m {
		switch {
		case k == "paused_until":
			var t *time.Time
			if string(raw) != "null" {
				var v time.Time
				if err := json.Unmarshal(raw, &v); err != nil {
					return p, invalid("paused_until: %v", err)
				}
				t = &v
			}
			p.PausedUntil = &t
		case k == "name" && allowName:
			var n string
			if err := json.Unmarshal(raw, &n); err != nil {
				return p, invalid("name: %v", err)
			}
			p.Name = &n
		case k == "limits" && allowLimits:
			var l queue.Limits
			if err := json.Unmarshal(raw, &l); err != nil {
				return p, invalid("limits: %v", err)
			}
			p.Limits = &l
		default:
			return p, invalid("%s cannot be changed here", k)
		}
	}
	return p, nil
}

// countingWriter counts the body bytes written, for the transfer rates.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.ResponseWriter.Write(b)
	c.n += int64(n)
	return n, err
}

// serveFile sends a file with Range support and its sha256 as the ETag.
func serveFile(w http.ResponseWriter, r *http.Request, path, sha string) error {
	f, err := os.Open(path)
	if err != nil {
		return &queue.Error{Status: 404, Code: "not_found", Detail: "the file is gone"}
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	w.Header().Set("ETag", `"`+sha+`"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", fi.ModTime(), f)
	return nil
}

// ---------------------------------------------------------------- the helper API

type ctxKey struct{}

// HelperHandler is the helper API. It knows a worker by the request's source address,
// which only WireGuard's cryptokey routing can put there.
func (s *Server) HelperHandler() http.Handler {
	mux := http.NewServeMux()
	h := func(pattern string, fn func(w http.ResponseWriter, r *http.Request, wid string) error) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			wid, _ := r.Context().Value(ctxKey{}).(string)
			if err := fn(w, r, wid); err != nil {
				s.fail(w, r, err)
			}
		})
	}
	st := s.Store
	h("GET /v1/me", func(w http.ResponseWriter, r *http.Request, wid string) error {
		v, err := st.Me(wid)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("PATCH /v1/me", func(w http.ResponseWriter, r *http.Request, wid string) error {
		m, err := patchFields(r)
		if err != nil {
			return err
		}
		p, err := workerPatch(m, false, true)
		if err != nil {
			return err
		}
		v, err := st.PatchMe(wid, p)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("GET /v1/me/capabilities", func(w http.ResponseWriter, r *http.Request, wid string) error {
		v, err := st.GetCapabilities(wid)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("PUT /v1/me/capabilities", func(w http.ResponseWriter, r *http.Request, wid string) error {
		var c queue.Capabilities
		if err := decode(r, 1<<20, &c); err != nil {
			return err
		}
		v, err := st.PutCapabilities(wid, c)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("PUT /v1/me/presence", func(w http.ResponseWriter, r *http.Request, wid string) error {
		var p queue.Presence
		if err := decode(r, 64<<10, &p); err != nil {
			return err
		}
		v, err := st.PutPresence(wid, p)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	source := func(w http.ResponseWriter, r *http.Request, wid string) error {
		path, sha, err := st.SourceFile(wid, r.PathValue("job_id"), r.PathValue("name"))
		if err != nil {
			return err
		}
		cw := &countingWriter{ResponseWriter: w}
		start := time.Now()
		err = serveFile(cw, r, path, sha)
		st.NoteTransfer(wid, queue.DirDownload, cw.n, time.Since(start))
		return err
	}
	h("GET /v1/jobs/{job_id}/source", source)
	h("GET /v1/jobs/{job_id}/inputs/{name}", source)
	h("GET /v1/leases", func(w http.ResponseWriter, r *http.Request, wid string) error {
		v, err := st.MyLeases(wid)
		if err != nil {
			return err
		}
		writeJSON(w, 200, map[string]any{"items": v})
		return nil
	})
	h("POST /v1/leases", func(w http.ResponseWriter, r *http.Request, wid string) error {
		var req queue.LeaseRequest
		body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 64<<10))
		if err != nil {
			return invalid("the body: %v", err)
		}
		if len(strings.TrimSpace(string(body))) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				return invalid("the body is not a LeaseRequest: %v", err)
			}
		}
		wait := preferWait(r)
		applied(w, wait, st.Config().LongPoll)
		l, err := st.Claim(r.Context(), wid, req, wait)
		if err != nil {
			return err
		}
		if l == nil {
			w.WriteHeader(204)
			return nil
		}
		w.Header().Set("Location", "/v1/leases/"+l.ID)
		writeJSON(w, 201, l)
		return nil
	})
	h("GET /v1/leases/{lease_id}", func(w http.ResponseWriter, r *http.Request, wid string) error {
		v, err := st.GetLease(wid, r.PathValue("lease_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("PATCH /v1/leases/{lease_id}", func(w http.ResponseWriter, r *http.Request, wid string) error {
		var body struct {
			Progress *queue.Progress `json:"progress"`
		}
		if err := decode(r, 64<<10, &body); err != nil {
			return err
		}
		if body.Progress == nil {
			return invalid("progress is required")
		}
		v, err := st.Renew(wid, r.PathValue("lease_id"), *body.Progress)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("DELETE /v1/leases/{lease_id}", func(w http.ResponseWriter, r *http.Request, wid string) error {
		if err := st.Release(wid, r.PathValue("lease_id"), r.URL.Query().Get("reason")); err != nil {
			return err
		}
		w.WriteHeader(204)
		return nil
	})
	h("POST /v1/leases/{lease_id}/logs", func(w http.ResponseWriter, r *http.Request, wid string) error {
		var body struct {
			Items []queue.LogLineIn `json:"items"`
		}
		if err := decode(r, 256<<10, &body); err != nil {
			return err
		}
		if err := st.AppendLogs(wid, r.PathValue("lease_id"), body.Items); err != nil {
			return err
		}
		w.WriteHeader(202)
		return nil
	})
	h("PUT /v1/leases/{lease_id}/result", func(w http.ResponseWriter, r *http.Request, wid string) error {
		var in queue.ResultIn
		if err := decode(r, 64<<10, &in); err != nil {
			return err
		}
		v, err := st.Deliver(wid, r.PathValue("lease_id"), in)
		if err != nil {
			return err
		}
		w.Header().Set("Location", "/v1/leases/"+v.LeaseID+"/result")
		writeJSON(w, 202, v)
		return nil
	})
	h("GET /v1/leases/{lease_id}/result", func(w http.ResponseWriter, r *http.Request, wid string) error {
		v, err := st.GetResult(wid, r.PathValue("lease_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	s.tusRoutes(h)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ap, err := netip.ParseAddrPort(r.RemoteAddr)
		if err != nil {
			s.fail(w, r, &queue.Error{Status: 403, Code: "unknown_peer", Detail: "no tunnel address"})
			return
		}
		wid, ok := st.WorkerByAddress(ap.Addr())
		if !ok {
			s.fail(w, r, &queue.Error{Status: 403, Code: "unknown_peer", Detail: "this tunnel address belongs to no worker"})
			return
		}
		mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, wid)))
	})
}

func (s *Server) tusRoutes(h func(string, func(http.ResponseWriter, *http.Request, string) error)) {
	st := s.Store
	tusHeaders := func(w http.ResponseWriter) {
		w.Header().Set("Tus-Resumable", "1.0.0")
	}
	needTus := func(w http.ResponseWriter, r *http.Request) error {
		tusHeaders(w)
		if r.Header.Get("Tus-Resumable") != "1.0.0" {
			w.Header().Set("Tus-Version", "1.0.0")
			return &queue.Error{Status: 412, Code: "invalid", Detail: "Tus-Resumable: 1.0.0 is required"}
		}
		return nil
	}
	h("OPTIONS /v1/files", func(w http.ResponseWriter, r *http.Request, wid string) error {
		tusHeaders(w)
		w.Header().Set("Tus-Version", "1.0.0")
		w.Header().Set("Tus-Extension", "creation,termination")
		w.WriteHeader(204)
		return nil
	})
	h("POST /v1/files", func(w http.ResponseWriter, r *http.Request, wid string) error {
		if err := needTus(w, r); err != nil {
			return err
		}
		length, err := strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
		if err != nil {
			return invalid("Upload-Length is required")
		}
		meta := parseMetadata(r.Header.Get("Upload-Metadata"))
		lid := meta["lease_id"]
		if lid == "" {
			return invalid("Upload-Metadata must name lease_id")
		}
		id, err := st.CreateUpload(wid, lid, length)
		if err != nil {
			return err
		}
		w.Header().Set("Location", "/v1/files/"+id)
		w.WriteHeader(201)
		return nil
	})
	h("HEAD /v1/files/{file_id}", func(w http.ResponseWriter, r *http.Request, wid string) error {
		if err := needTus(w, r); err != nil {
			return err
		}
		off, length, err := st.UploadOffset(wid, r.PathValue("file_id"))
		if err != nil {
			return err
		}
		w.Header().Set("Upload-Offset", strconv.FormatInt(off, 10))
		w.Header().Set("Upload-Length", strconv.FormatInt(length, 10))
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(200)
		return nil
	})
	h("PATCH /v1/files/{file_id}", func(w http.ResponseWriter, r *http.Request, wid string) error {
		if err := needTus(w, r); err != nil {
			return err
		}
		if r.Header.Get("Content-Type") != "application/offset+octet-stream" {
			return &queue.Error{Status: 415, Code: "invalid", Detail: "Content-Type must be application/offset+octet-stream"}
		}
		off, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
		if err != nil {
			return invalid("Upload-Offset is required")
		}
		n, err := st.AppendUpload(wid, r.PathValue("file_id"), off, r.Body)
		if err != nil {
			return err
		}
		w.Header().Set("Upload-Offset", strconv.FormatInt(n, 10))
		w.WriteHeader(204)
		return nil
	})
	h("DELETE /v1/files/{file_id}", func(w http.ResponseWriter, r *http.Request, wid string) error {
		if err := needTus(w, r); err != nil {
			return err
		}
		if err := st.DeleteUpload(wid, r.PathValue("file_id")); err != nil {
			return err
		}
		w.WriteHeader(204)
		return nil
	})
}

// parseMetadata reads tus's Upload-Metadata: key and base64 value pairs.
func parseMetadata(h string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(h, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(pair), " ")
		if k == "" {
			continue
		}
		dec, err := decodeB64(v)
		if err != nil {
			continue
		}
		out[k] = dec
	}
	return out
}

// ---------------------------------------------------------------- the pipeline and admin API

type caller int

const (
	callerNone caller = iota
	callerPipeline
	callerAdmin
)

func (s *Server) caller(r *http.Request) caller {
	auth := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok || tok == "" {
		return callerNone
	}
	eq := func(a, b string) bool { return b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
	switch {
	case eq(tok, s.PipelineToken):
		return callerPipeline
	case eq(tok, s.AdminToken):
		return callerAdmin
	}
	return callerNone
}

// MetricsHandler is /metrics and /healthz, for a listener of their own.
func (s *Server) MetricsHandler() http.Handler {
	mux := http.NewServeMux()
	s.healthAndMetrics(mux, true)
	return mux
}

func (s *Server) healthAndMetrics(mux *http.ServeMux, metrics bool) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "ok\n")
	})
	if metrics {
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			s.Store.WriteMetrics(w, s.Version)
		})
	}
}

// AdminHandler is the pipeline and admin API and /healthz, and /metrics unless they have a
// listener of their own.
func (s *Server) AdminHandler(withMetrics bool) http.Handler {
	mux := http.NewServeMux()
	st := s.Store
	// h registers a route; who is the callers it takes (none: either).
	h := func(pattern string, who caller, fn func(w http.ResponseWriter, r *http.Request) error) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			c := s.caller(r)
			if c == callerNone {
				w.Header().Set("WWW-Authenticate", `Bearer realm="encode-queue"`)
				s.fail(w, r, &queue.Error{Status: 401, Code: "unauthorized", Detail: "a bearer token is required"})
				return
			}
			if who != callerNone && c != who {
				s.fail(w, r, &queue.Error{Status: 403, Code: "forbidden", Detail: "this is the other caller's operation"})
				return
			}
			if err := fn(w, r); err != nil {
				s.fail(w, r, err)
			}
		})
	}
	s.healthAndMetrics(mux, withMetrics)
	h("GET /v1/status", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		writeJSON(w, 200, st.Status(s.Version))
		return nil
	})
	h("GET /v1/jobs", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		q := r.URL.Query()
		list := func(k string) []string {
			if v := q.Get(k); v != "" {
				return strings.Split(v, ",")
			}
			return nil
		}
		f := queue.JobFilter{States: list("state"), Priorities: list("priority"), Kinds: list("kind"), UploadID: q.Get("upload_id")}
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 500 {
				return invalid("limit is 1 to 500")
			}
			f.Limit = n
		}
		if v := q.Get("cursor"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return invalid("cursor is a `next` from an earlier page")
			}
			f.Cursor = n
		}
		writeJSON(w, 200, st.ListJobs(f))
		return nil
	})
	h("PUT /v1/jobs/{job_id}", callerPipeline, func(w http.ResponseWriter, r *http.Request) error {
		var spec queue.JobSubmit
		if err := decode(r, 1<<20, &spec); err != nil {
			return err
		}
		v, created, err := st.Submit(r.PathValue("job_id"), spec)
		if err != nil {
			return err
		}
		if created {
			w.Header().Set("Location", "/v1/jobs/"+v.ID)
			writeJSON(w, 201, v)
		} else {
			writeJSON(w, 200, v)
		}
		return nil
	})
	h("GET /v1/jobs/{job_id}", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		v, err := st.GetJob(r.PathValue("job_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("DELETE /v1/jobs/{job_id}", callerPipeline, func(w http.ResponseWriter, r *http.Request) error {
		if err := st.Forget(r.PathValue("job_id")); err != nil {
			return err
		}
		w.WriteHeader(204)
		return nil
	})
	h("POST /v1/jobs/{job_id}/move", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		var m queue.Move
		if err := decode(r, 64<<10, &m); err != nil {
			return err
		}
		v, err := st.MoveJob(r.PathValue("job_id"), m)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("POST /v1/jobs/cleanup", callerAdmin, func(w http.ResponseWriter, r *http.Request) error {
		var c queue.Cleanup
		if err := decode(r, 64<<10, &c); err != nil {
			return err
		}
		v, err := st.CleanupJobs(c)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("POST /v1/jobs/{job_id}/assign", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		var a queue.Assign
		if err := decode(r, 64<<10, &a); err != nil {
			return err
		}
		v, err := st.AssignJob(r.PathValue("job_id"), a)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("DELETE /v1/jobs/{job_id}/assign", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		v, err := st.UnassignJob(r.PathValue("job_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("GET /v1/jobs/{job_id}/candidates", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		v, err := st.JobCandidates(r.PathValue("job_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("POST /v1/jobs/{job_id}/cancel", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		v, err := st.CancelJob(r.PathValue("job_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("POST /v1/jobs/{job_id}/retry", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		v, err := st.RetryJob(r.PathValue("job_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("GET /v1/changes", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		q := r.URL.Query()
		since, err := strconv.ParseInt(q.Get("since"), 10, 64)
		if err != nil || since < 0 {
			return invalid("since is a revision, 0 to start")
		}
		limit := 500
		if v := q.Get("limit"); v != "" {
			if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > 1000 {
				return invalid("limit is 1 to 1000")
			}
		}
		wait := preferWait(r)
		applied(w, wait, st.Config().LongPoll)
		writeJSON(w, 200, st.WaitChanges(r.Context(), since, limit, wait))
		return nil
	})
	attempt := func(r *http.Request) (int, error) {
		n, err := strconv.Atoi(r.PathValue("attempt"))
		if err != nil || n < 1 {
			return 0, &queue.Error{Status: 404, Code: "not_found", Detail: "no such attempt"}
		}
		return n, nil
	}
	h("GET /v1/jobs/{job_id}/attempts/{attempt}/file", callerPipeline, func(w http.ResponseWriter, r *http.Request) error {
		n, err := attempt(r)
		if err != nil {
			return err
		}
		path, sha, err := st.ResultFile(r.PathValue("job_id"), n)
		if err != nil {
			return err
		}
		return serveFile(w, r, path, sha)
	})
	h("PUT /v1/jobs/{job_id}/attempts/{attempt}/verdict", callerPipeline, func(w http.ResponseWriter, r *http.Request) error {
		n, err := attempt(r)
		if err != nil {
			return err
		}
		var v queue.Verdict
		if err := decode(r, 1<<20, &v); err != nil {
			return err
		}
		jv, err := st.PutVerdict(r.PathValue("job_id"), n, v)
		if err != nil {
			return err
		}
		writeJSON(w, 200, jv)
		return nil
	})
	h("GET /v1/leases/{lease_id}/logs", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		var after int64
		if v := r.URL.Query().Get("after"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return invalid("after is a sequence number")
			}
			after = n
		}
		lines, err := st.Logs(r.PathValue("lease_id"), after)
		if err != nil {
			return err
		}
		writeJSON(w, 200, map[string]any{"items": lines})
		return nil
	})
	h("GET /v1/workers", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		writeJSON(w, 200, map[string]any{"server_time": time.Now().UTC(), "items": st.ListWorkers()})
		return nil
	})
	h("POST /v1/workers", callerAdmin, func(w http.ResponseWriter, r *http.Request) error {
		var c queue.WorkerCreate
		if err := decode(r, 64<<10, &c); err != nil {
			return err
		}
		e, err := st.AddWorker(c)
		if err != nil {
			return err
		}
		w.Header().Set("Location", "/v1/workers/"+e.Worker.ID)
		writeJSON(w, 201, e)
		return nil
	})
	h("GET /v1/workers/{worker_id}", callerNone, func(w http.ResponseWriter, r *http.Request) error {
		v, err := st.GetWorker(r.PathValue("worker_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("PATCH /v1/workers/{worker_id}", callerAdmin, func(w http.ResponseWriter, r *http.Request) error {
		m, err := patchFields(r)
		if err != nil {
			return err
		}
		p, err := workerPatch(m, true, false)
		if err != nil {
			return err
		}
		v, err := st.PatchWorker(r.PathValue("worker_id"), p)
		if err != nil {
			return err
		}
		writeJSON(w, 200, v)
		return nil
	})
	h("DELETE /v1/workers/{worker_id}", callerAdmin, func(w http.ResponseWriter, r *http.Request) error {
		if err := st.RemoveWorker(r.PathValue("worker_id")); err != nil {
			return err
		}
		w.WriteHeader(204)
		return nil
	})
	h("GET /v1/workers/{worker_id}/tunnel", callerAdmin, func(w http.ResponseWriter, r *http.Request) error {
		e, err := st.WorkerTunnel(r.PathValue("worker_id"))
		if err != nil {
			return err
		}
		writeJSON(w, 200, e)
		return nil
	})
	return mux
}

// Serve runs an HTTP server on l until ctx ends, then shuts it down gracefully.
func Serve(ctx context.Context, l net.Listener, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 30 * time.Second, ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(l) }()
	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

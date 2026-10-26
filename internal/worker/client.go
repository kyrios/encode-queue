package worker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// APIError is a problem+json answer.
type APIError struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *APIError) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Detail) }

// gone is a lease that ended without this worker (410).
func gone(err error) (string, bool) {
	var ae *APIError
	if errors.As(err, &ae) && ae.Status == 410 {
		return ae.Code, true
	}
	return "", false
}

// Client talks to the helper API.
type Client struct {
	HTTP   *http.Client
	base   string // .../v1
	origin string // scheme://host
}

// NewClient is a client for the API at base (the tunnel configuration's api).
func NewClient(h *http.Client, base string) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("api %q is not a URL", base)
	}
	return &Client{HTTP: h, base: u.String(), origin: u.Scheme + "://" + u.Host}, nil
}

// url resolves a reference the API gave: an absolute path (/v1/...), a path relative to the
// API (/jobs/...), or a full URL.
func (c *Client) url(ref string) string {
	switch {
	case strings.HasPrefix(ref, "http://"), strings.HasPrefix(ref, "https://"):
		return ref
	case strings.HasPrefix(ref, "/v1/"):
		return c.origin + ref
	}
	return c.base + ref
}

func (c *Client) do(ctx context.Context, method, ref string, hdr http.Header, body io.Reader, out any) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.url(ref), body)
	if err != nil {
		return nil, err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		ae := &APIError{Status: resp.StatusCode}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if json.Unmarshal(data, ae) != nil || ae.Code == "" {
			ae.Code = "http_" + strconv.Itoa(resp.StatusCode)
			ae.Detail = strings.TrimSpace(string(data))
		}
		ae.Status = resp.StatusCode
		return resp, ae
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp, fmt.Errorf("%s %s: the answer: %w", method, ref, err)
		}
	} else {
		io.Copy(io.Discard, resp.Body)
	}
	return resp, nil
}

func (c *Client) jsonCall(ctx context.Context, method, ref, contentType string, in, out any) (*http.Response, error) {
	var body io.Reader
	hdr := http.Header{}
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
		if contentType == "" {
			contentType = "application/json"
		}
		hdr.Set("Content-Type", contentType)
	}
	return c.do(ctx, method, ref, hdr, body, out)
}

// Me is the worker as the queue knows it.
type Me struct {
	ID          string     `json:"worker_id"`
	Name        string     `json:"name"`
	State       string     `json:"state"`
	PausedUntil *time.Time `json:"paused_until"`
	Address     string     `json:"tunnel_address"`
}

// CapabilityReport is the queue's verdict on what this worker runs.
type CapabilityReport struct {
	Accepted []string `json:"accepted"`
	Refused  []struct {
		Kind   string `json:"kind"`
		Reason string `json:"reason"`
	} `json:"refused"`
}

// PresenceAck answers a presence.
type PresenceAck struct {
	PresenceEveryS int        `json:"presence_every_s"`
	PausedUntil    *time.Time `json:"paused_until"`
	Draining       bool       `json:"draining"`
}

// FileRef is a file to fetch.
type FileRef struct {
	URL    string `json:"url"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// Lease is a held job.
type Lease struct {
	ID      string `json:"lease_id"`
	Attempt int    `json:"attempt"`
	Job     struct {
		ID     string `json:"job_id"`
		Title  string `json:"title"`
		Kind   string `json:"kind"`
		Source struct {
			FileRef
			Container string  `json:"container"`
			DurationS float64 `json:"duration_s"`
			Frames    int64   `json:"frames"`
			Width     int     `json:"width"`
			Height    int     `json:"height"`
		} `json:"source"`
		Inputs map[string]FileRef `json:"inputs"`
	} `json:"job"`
	ExpiresAt      time.Time       `json:"expires_at"`
	RenewEveryS    int             `json:"renew_every_s"`
	PauseGiveBackS int             `json:"pause_give_back_s"`
	PausedUntil    *time.Time      `json:"paused_until"`
	Params         json.RawMessage `json:"params"`
	Result         struct {
		Name     string `json:"name"`
		MaxBytes int64  `json:"max_bytes"`
	} `json:"result"`
}

// Progress is a report on a lease.
type Progress struct {
	Phase       string     `json:"phase"`
	Fraction    *float64   `json:"fraction,omitempty"`
	FramesDone  *int64     `json:"frames_done,omitempty"`
	FramesTotal *int64     `json:"frames_total,omitempty"`
	FPS         *float64   `json:"fps,omitempty"`
	Speed       *float64   `json:"speed,omitempty"`
	ETAS        *int64     `json:"eta_s,omitempty"`
	BytesDone   *int64     `json:"bytes_done,omitempty"`
	BytesTotal  *int64     `json:"bytes_total,omitempty"`
	PausedSince *time.Time `json:"paused_since,omitempty"`
}

// LogLine is a line for the queue.
type LogLine struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Source  string    `json:"source,omitempty"`
	Message string    `json:"message"`
}

// Result is the verdict on a delivered result.
type Result struct {
	Verdict string  `json:"verdict"`
	Reason  *string `json:"reason"`
}

func (c *Client) GetMe(ctx context.Context) (Me, error) {
	var m Me
	_, err := c.jsonCall(ctx, "GET", "/me", "", nil, &m)
	return m, err
}

func (c *Client) PatchMe(ctx context.Context, patch map[string]any) (Me, error) {
	var m Me
	_, err := c.jsonCall(ctx, "PATCH", "/me", "application/merge-patch+json", patch, &m)
	return m, err
}

func (c *Client) PutCapabilities(ctx context.Context, caps Capabilities) (CapabilityReport, error) {
	var r CapabilityReport
	_, err := c.jsonCall(ctx, "PUT", "/me/capabilities", "", caps, &r)
	return r, err
}

func (c *Client) PutPresence(ctx context.Context, state string, diskFree int64) (PresenceAck, error) {
	var a PresenceAck
	body := map[string]any{"state": state}
	if diskFree > 0 {
		body["disk_free_bytes"] = diskFree
	}
	_, err := c.jsonCall(ctx, "PUT", "/me/presence", "", body, &a)
	return a, err
}

// Claim long-polls for a job; nil without an error when none came.
func (c *Client) Claim(ctx context.Context, wait time.Duration) (*Lease, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", c.url("/leases"), strings.NewReader("{}"))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", fmt.Sprintf("wait=%d", int(wait/time.Second)))
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 204:
		return nil, nil
	case resp.StatusCode == 201:
		var l Lease
		if err := json.NewDecoder(resp.Body).Decode(&l); err != nil {
			return nil, err
		}
		return &l, nil
	}
	ae := &APIError{}
	json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(ae)
	ae.Status = resp.StatusCode
	if ae.Code == "" {
		ae.Code = "http_" + strconv.Itoa(resp.StatusCode)
	}
	return nil, ae
}

func (c *Client) MyLeases(ctx context.Context) ([]Lease, error) {
	var out struct {
		Items []Lease `json:"items"`
	}
	_, err := c.jsonCall(ctx, "GET", "/leases", "", nil, &out)
	return out.Items, err
}

// Renew reports progress; the answer is the lease with its current pause.
func (c *Client) Renew(ctx context.Context, id string, p Progress) (Lease, error) {
	var l Lease
	_, err := c.jsonCall(ctx, "PATCH", "/leases/"+id, "application/merge-patch+json", map[string]any{"progress": p}, &l)
	return l, err
}

func (c *Client) Release(ctx context.Context, id, reason string) error {
	_, err := c.do(ctx, "DELETE", "/leases/"+id+"?reason="+url.QueryEscape(reason), nil, nil, nil)
	return err
}

func (c *Client) Logs(ctx context.Context, id string, lines []LogLine) error {
	_, err := c.jsonCall(ctx, "POST", "/leases/"+id+"/logs", "", map[string]any{"items": lines}, nil)
	return err
}

func (c *Client) PutResult(ctx context.Context, id string, body map[string]any) (Result, error) {
	var r Result
	_, err := c.jsonCall(ctx, "PUT", "/leases/"+id+"/result", "", body, &r)
	return r, err
}

func (c *Client) GetResult(ctx context.Context, id string) (Result, error) {
	var r Result
	_, err := c.jsonCall(ctx, "GET", "/leases/"+id+"/result", "", nil, &r)
	return r, err
}

// Download fetches ref into path, resuming from what is there with Range, and checks the
// sha256. progress is called with the bytes so far; wait blocks while the worker is paused.
func (c *Client) Download(ctx context.Context, f FileRef, path string, progress func(int64), wait func(context.Context) error) error {
	part := path + ".part"
	for attempt := 0; ; attempt++ {
		err := c.downloadOnce(ctx, f, part, progress, wait)
		if err == nil {
			break
		}
		var ae *APIError
		if ctx.Err() != nil || errors.As(err, &ae) || attempt >= 20 {
			return err
		}
		select { // a broken connection: resume after a moment
		case <-time.After(time.Duration(attempt+1) * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	sum, err := fileSHA256(part)
	if err != nil {
		return err
	}
	if sum != f.SHA256 {
		os.Remove(part)
		return &checksumError{want: f.SHA256, got: sum}
	}
	return os.Rename(part, path)
}

type checksumError struct{ want, got string }

func (e *checksumError) Error() string {
	return fmt.Sprintf("the download's sha256 is %s, not %s", e.got, e.want)
}

func (c *Client) downloadOnce(ctx context.Context, f FileRef, part string, progress func(int64), wait func(context.Context) error) error {
	out, err := os.OpenFile(part, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	have, err := out.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if have > f.Bytes {
		out.Truncate(0)
		have, _ = out.Seek(0, io.SeekStart)
	}
	if have == f.Bytes {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, "GET", c.url(f.URL), nil)
	if err != nil {
		return err
	}
	if have > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", have))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		if have > 0 {
			out.Truncate(0)
			out.Seek(0, io.SeekStart)
			have = 0
		}
	case 206:
	default:
		ae := &APIError{}
		json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(ae)
		ae.Status = resp.StatusCode
		return ae
	}
	buf := make([]byte, 1<<20)
	for {
		if err := wait(ctx); err != nil {
			return err
		}
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				return err
			}
			have += int64(n)
			progress(have)
		}
		if rerr == io.EOF {
			if have != f.Bytes {
				return fmt.Errorf("the download ended at %d of %d bytes", have, f.Bytes)
			}
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

// Upload sends path over tus for a lease, resuming after a broken connection, and returns
// the upload's id and the file's sha256.
func (c *Client) Upload(ctx context.Context, leaseID, path string, chunk int64, progress func(int64), wait func(context.Context) error) (string, string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}
	sum, err := fileSHA256(path)
	if err != nil {
		return "", "", err
	}
	hdr := http.Header{}
	hdr.Set("Tus-Resumable", "1.0.0")
	hdr.Set("Upload-Length", strconv.FormatInt(fi.Size(), 10))
	hdr.Set("Upload-Metadata", "lease_id "+base64.StdEncoding.EncodeToString([]byte(leaseID)))
	resp, err := c.do(ctx, "POST", "/files", hdr, nil, nil)
	if err != nil {
		return "", "", err
	}
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", "", errors.New("the queue gave no Location for the upload")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	var off int64
	failures := 0
	for off < fi.Size() {
		if err := wait(ctx); err != nil {
			return "", "", err
		}
		n := chunk
		if rest := fi.Size() - off; rest < n {
			n = rest
		}
		h := http.Header{}
		h.Set("Tus-Resumable", "1.0.0")
		h.Set("Upload-Offset", strconv.FormatInt(off, 10))
		h.Set("Content-Type", "application/offset+octet-stream")
		r, err := c.do(ctx, "PATCH", loc, h, io.NewSectionReader(f, off, n), nil)
		if err == nil {
			off, _ = strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
			progress(off)
			failures = 0
			continue
		}
		var ae *APIError
		if errors.As(err, &ae) && ae.Code != "offset_mismatch" || ctx.Err() != nil {
			return "", "", err
		}
		failures++
		if failures > 20 {
			return "", "", err
		}
		time.Sleep(time.Duration(failures) * time.Second)
		hh := http.Header{}
		hh.Set("Tus-Resumable", "1.0.0")
		r, herr := c.do(ctx, "HEAD", loc, hh, nil, nil)
		if herr == nil {
			off, _ = strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
		}
	}
	return loc, sum, nil
}

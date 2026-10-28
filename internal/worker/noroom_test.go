package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// A result the queue has no room for yet waits and asks again (v0.2.20), rather than failing
// the attempt; it goes up whole once there is room.
func TestAnUploadWaitsForRoomOnTheQueuesDisk(t *testing.T) {
	saved := NoRoomWait
	NoRoomWait = 10 * time.Millisecond
	defer func() { NoRoomWait = saved }()
	var asked, refused atomic.Int32
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			asked.Add(1)
			if refused.Add(1) <= 3 {
				rw.Header().Set("Content-Type", "application/problem+json")
				rw.WriteHeader(503)
				io.WriteString(rw, `{"status":503,"code":"no_room","detail":"ask again later"}`)
				return
			}
			rw.Header().Set("Location", "/v1/files/f_1")
			rw.WriteHeader(201)
		case "PATCH":
			n, _ := io.Copy(io.Discard, r.Body)
			off, _ := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
			got.Store(off + n)
			rw.Header().Set("Upload-Offset", strconv.FormatInt(off+n, 10))
			rw.WriteHeader(204)
		}
	}))
	defer srv.Close()
	c, err := NewClient(srv.Client(), srv.URL+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "2160.mp4")
	if err := os.WriteFile(path, make([]byte, 5000), 0o600); err != nil {
		t.Fatal(err)
	}
	loc, _, err := c.Upload(context.Background(), "ls_1", path, 2048, func(int64) {}, func(context.Context) error { return nil })
	if err != nil || loc == "" {
		t.Fatalf("the upload: %q %v", loc, err)
	}
	if asked.Load() != 4 || got.Load() != 5000 {
		t.Fatalf("asked %d times, sent %d bytes", asked.Load(), got.Load())
	}
	// A lease that ends while it waits ends the wait.
	refused.Store(-1000)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := c.Upload(ctx, "ls_2", path, 2048, func(int64) {}, func(context.Context) error { return nil }); err == nil {
		t.Fatal("an upload that never had room ended without an error")
	}
}

package worker

import (
	"sync"
	"testing"
	"time"
)

// A job's log lines reach the app with slog's level words (v0.2.21), so Slipstream files a
// warning as WARN and not as INFO.
func TestLogEventsCarryTheLevel(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	w, err := New(Config{StateDir: t.TempDir(), Events: func(ev map[string]any) {
		if ev["event"] == "log" {
			mu.Lock()
			got[ev["line"].(string)], _ = ev["level"].(string)
			mu.Unlock()
		}
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := newRun(w, &Lease{ID: "ls_LEVEL"}, false)
	want := map[string]string{"a": "INFO", "b": "WARN", "c": "WARN", "d": "ERROR", "e": "DEBUG", "f": "INFO"}
	for line, level := range map[string]string{"a": "info", "b": "warn", "c": "warning", "d": "error", "e": "debug", "f": "notice"} {
		r.log(level, "worker", line)
	}
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == len(want) {
			break
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for line, level := range want {
		if got[line] != level {
			t.Errorf("%s: level %q, want %q", line, got[line], level)
		}
	}
}

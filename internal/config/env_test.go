package config

import (
	"testing"
	"time"
)

func TestDuration(t *testing.T) {
	e := FromMap(map[string]string{"A": "604800", "B": "7d", "C": "36h", "D": "soon", "E": ""})
	for k, want := range map[string]time.Duration{"A": 7 * 24 * time.Hour, "B": 7 * 24 * time.Hour, "C": 36 * time.Hour, "E": 0, "F": 0} {
		if got := e.Duration(k); got != want {
			t.Errorf("%s: %v, want %v", k, got, want)
		}
	}
	if e.Duration("D") != 0 || len(e.Problems) != 1 {
		t.Fatalf("a bad value: %v", e.Problems)
	}
}

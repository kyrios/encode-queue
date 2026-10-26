package queue

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// The README's metrics table is MetricHelp, so the console and the alerts read true names.
func TestReadmeListsTheMetrics(t *testing.T) {
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	start := strings.Index(s, "<!-- metrics:")
	end := strings.Index(s, "<!-- /metrics -->")
	if start < 0 || end < start {
		t.Fatal("the README has no metrics table")
	}
	var b strings.Builder
	b.WriteString("| series | type | what it is |\n|---|---|---|\n")
	for _, m := range MetricHelp {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", m.Name, m.Type, m.Help)
	}
	table := s[strings.Index(s[start:], "\n")+start+1 : end]
	if table != b.String() {
		t.Fatalf("the README's metrics table differs; it should be:\n%s", b.String())
	}
}

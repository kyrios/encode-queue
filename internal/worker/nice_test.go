package worker

import (
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// Every tool runs at nice 19, from whichever thread the runtime starts it: twenty children
// started from busy goroutines all read 19 from ps.
func TestToolsRunAtNice19(t *testing.T) {
	if _, err := exec.LookPath("ps"); err != nil {
		t.Skip("no ps")
	}
	var wg sync.WaitGroup
	errs := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command("sh", "-c", "ps -o nice= -p $$")
			var out strings.Builder
			cmd.Stdout = &out
			if err := start(cmd); err != nil {
				errs <- err.Error()
				return
			}
			cmd.Wait()
			if got := strings.TrimSpace(out.String()); got != "19" {
				errs <- "nice " + got
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

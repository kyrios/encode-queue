package worker

import (
	"os/exec"
	"runtime"
)

// **Realtime before normal, by the system's own scheduler** (v0.2.20; Thorsten, 2026-10-28:
// "Instead of throttling manually can we simply use the OS task scheduler and give the realtime
// jobs a slightly higher CPU priority?"). A job of priority now is realtime; every other job, of
// stage 1, stage 2 or archive, is normal. A normal job keeps its lease and its progress, and every
// idle cycle: only its share of a contended CPU changes.
//
//   - Linux: a normal job's tools start at nice 19 and stay there, a realtime job's at the
//     worker's own nice, 0. CFS weighs nice 0 at 1024 and nice 19 at 15, so a realtime tool gets
//     98.5 % of a CPU both want.
//   - macOS: nice alone does little there, so a normal job's tools also start under the utility
//     QoS clamp (taskpolicy -c utility), and go into the background QoS (PRIO_DARWIN_BG, the
//     efficiency cores) while a realtime job runs on this worker, out again when the last one ends
//     (realtime.go). A realtime job's tools start unclamped.
//
// **The worker never renices itself**: an unprivileged process cannot lower a nice value, so a
// worker at 19 could start no tool at 0. Before v0.2.20 every tool ran at 19, realtime ones too.
const Nice = 19

// realtime says whether a job of priority p is realtime.
func realtime(p string) bool { return p == "now" }

// normalPrefix is the command a normal job's tool starts through: nice 19, and on macOS the
// utility QoS clamp; both exec the tool in their place, so its pid stays the one the pause and
// the throttle signal. Empty where neither is there.
var normalPrefix = func() []string {
	nice, err := exec.LookPath("nice")
	if err != nil {
		return nil
	}
	p := []string{nice, "-n", "19"}
	if runtime.GOOS == "darwin" {
		if tp, err := exec.LookPath("/usr/sbin/taskpolicy"); err == nil {
			p = append([]string{tp, "-c", "utility"}, p...)
		}
	}
	return p
}()

// launch is the command line a tool starts with: a normal job's through normalPrefix, a realtime
// job's as it is. A tool that is not there keeps its own line, so its start fails as tool_missing.
func launch(args []string, rt bool) []string {
	if rt || len(args) == 0 || len(normalPrefix) == 0 {
		return args
	}
	if _, err := exec.LookPath(args[0]); err != nil {
		return args
	}
	return append(append([]string(nil), normalPrefix...), args...)
}

func start(cmd *exec.Cmd) error {
	return cmd.Start()
}

package worker

import (
	"os/exec"
	"runtime"

	"golang.org/x/sys/unix"
)

// Nice is the priority every tool runs at.
const Nice = 19

// start starts a tool at nice 19. On Linux a priority belongs to a thread, and a child takes
// the priority of the thread that forked it, so the fork happens from a locked thread set to
// 19 first; setting it once for the process reached only the main thread, and most tools then
// ran at 0. On macOS the priority is the process's, and this sets it again.
func start(cmd *exec.Cmd) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	unix.Setpriority(unix.PRIO_PROCESS, 0, Nice)
	return cmd.Start()
}

// Renice puts the worker's own process at nice 19 too (its main thread on Linux).
func Renice() error {
	return unix.Setpriority(unix.PRIO_PROCESS, 0, Nice)
}

package worker

import "golang.org/x/sys/unix"

// setBackground puts process pid into macOS's background QoS (PRIO_DARWIN_BG, as
// `taskpolicy -b -p`), which keeps all its threads on the efficiency cores, or takes it out.
func setBackground(pid int, on bool) error {
	prio := 0
	if on {
		prio = unix.PRIO_DARWIN_BG
	}
	return unix.Setpriority(unix.PRIO_DARWIN_PROCESS, pid, prio)
}

// coreLevels is the machine's performance and efficiency cores; 0, 0 when it has one kind.
func coreLevels() (perf, eff int) {
	if n, err := unix.SysctlUint32("hw.nperflevels"); err != nil || n < 2 {
		return 0, 0
	}
	p, err1 := unix.SysctlUint32("hw.perflevel0.logicalcpu")
	e, err2 := unix.SysctlUint32("hw.perflevel1.logicalcpu")
	if err1 != nil || err2 != nil {
		return 0, 0
	}
	return int(p), int(e)
}

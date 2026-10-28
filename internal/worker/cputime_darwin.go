package worker

import (
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// rusageInfoV0 is <sys/resource.h>'s struct rusage_info_v0; its times are Mach ticks.
type rusageInfoV0 struct {
	UUID       [16]byte
	UserTime   uint64
	SystemTime uint64
	_          [8]uint64
}

const (
	procInfoCallPIDRusage = 9 // PROC_INFO_CALL_PIDRUSAGE, libproc's proc_pid_rusage
	rusageInfoV0Flavor    = 0 // RUSAGE_INFO_V0
)

// tbFrequency is the Mach ticks per second: 24 MHz on Apple silicon, 1 GHz on Intel.
var tbFrequency = sync.OnceValue(func() float64 {
	if f, err := unix.SysctlUint64("hw.tbfrequency"); err == nil && f > 0 {
		return float64(f)
	}
	return 1e9
})

// groupCPU is the CPU time the processes of group pgid have used: proc_pid_rusage's user and
// system time of each member, through the proc_info syscall, since the worker builds without
// cgo and so without libproc.
func groupCPU(pgid int) (time.Duration, bool) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pgid)
	if err != nil || len(procs) == 0 {
		return 0, false
	}
	var ticks uint64
	for _, p := range procs {
		var ri rusageInfoV0
		_, _, e := unix.Syscall6(unix.SYS_PROC_INFO, procInfoCallPIDRusage, uintptr(p.Proc.P_pid), rusageInfoV0Flavor, 0,
			uintptr(unsafe.Pointer(&ri)), 0)
		if e != 0 {
			continue // gone since the list
		}
		ticks += ri.UserTime + ri.SystemTime
	}
	return time.Duration(float64(ticks) / tbFrequency() * float64(time.Second)), true
}

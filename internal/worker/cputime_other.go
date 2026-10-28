//go:build !darwin

package worker

import "time"

// groupCPU is the CPU time the processes of group pgid have used, from /proc (busy.go).
func groupCPU(pgid int) (time.Duration, bool) {
	s, ok := procCPUSeconds([]int{pgid})
	return time.Duration(s * float64(time.Second)), ok
}

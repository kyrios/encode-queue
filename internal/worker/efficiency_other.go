//go:build !darwin

package worker

import "errors"

// setBackground: only macOS has efficiency cores to keep a tool on.
func setBackground(pid int, on bool) error { return errors.ErrUnsupported }

func coreLevels() (perf, eff int) { return 0, 0 }

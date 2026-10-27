package worker

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestOSRelease(t *testing.T) {
	name, version := osRelease("PRETTY_NAME=\"Debian GNU/Linux 13 (trixie)\"\nNAME=\"Debian GNU/Linux\"\nVERSION_ID=\"13\"\nID=debian\n")
	if name != "Debian GNU/Linux" || version != "13" {
		t.Fatalf("%q %q", name, version)
	}
}

func TestCPUInfoCountsPhysicalCores(t *testing.T) {
	// Two cores with two threads each.
	data := ""
	for _, c := range []string{"0", "0", "1", "1"} {
		data += "processor\t: x\nmodel name\t: AMD Ryzen 9 7950X 16-Core Processor\nphysical id\t: 0\ncore id\t\t: " + c + "\n\n"
	}
	model, cores := cpuInfo(data)
	if model != "AMD Ryzen 9 7950X 16-Core Processor" || cores != 2 {
		t.Fatalf("%q %d", model, cores)
	}
	if _, cores := cpuInfo("processor\t: 0\nBogoMIPS\t: 108\n"); cores != 0 {
		t.Fatalf("no core ids: %d", cores)
	}
}

func TestHardwareSaysWhatThePageShows(t *testing.T) {
	var hw map[string]any
	if err := json.Unmarshal(hardware(), &hw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"cpus", "cpu_threads", "arch", "kernel", "os_name", "memory_bytes"} {
		if _, ok := hw[k]; !ok {
			t.Errorf("no %s in %v", k, hw)
		}
	}
}

func TestMemoryUsed(t *testing.T) {
	vm := "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free:      1000.\nPages active:     100.\nPages wired down:  50.\nPages occupied by compressor: 10.\n"
	if got := vmStatUsed(vm); got != 160*16384 {
		t.Fatalf("vm_stat: %d", got)
	}
	mi := "MemTotal:       8000 kB\nMemFree:  1000 kB\nMemAvailable:   3000 kB\n"
	if got := meminfoUsed(mi); got != 5000*1024 {
		t.Fatalf("meminfo: %d", got)
	}
	if memoryUsed() <= 0 {
		t.Fatal("no memory in use on this machine")
	}
}

func TestKilledToolIsKilled(t *testing.T) {
	err := exec.Command("sh", "-c", "kill -9 $$").Run()
	var f *failure
	if !errors.As(toolFailure("ffmpeg", err, "last line"), &f) || f.code != "killed" || f.extra["signal"] != "SIGKILL" ||
		!strings.Contains(f.msg, "killed by SIGKILL") {
		t.Fatalf("%+v", f)
	}
	if _, ok := f.extra["memory_available_bytes"]; !ok {
		t.Errorf("no memory note: %+v", f.extra)
	}
	err = exec.Command("sh", "-c", "exit 3").Run()
	if !errors.As(toolFailure("ffmpeg", err, ""), &f) || f.code != "encode_failed" || f.extra["exit_code"] != 3 {
		t.Fatalf("an exit: %+v", f)
	}
}

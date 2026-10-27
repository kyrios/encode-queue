package worker

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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

// Toolchain 3: libass's version from versions.txt, the toolchain's fc-list, and FONTCONFIG_FILE
// on its fonts.conf so every tool sees only Inter.
func TestToolchainFonts(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir()) // macOS: /var is /private/var
	bin := filepath.Join(dir, "bin")
	os.MkdirAll(bin, 0o755)
	os.MkdirAll(filepath.Join(dir, "fonts"), 0o755)
	os.WriteFile(filepath.Join(bin, "ffmpeg"), []byte("#!/bin/sh\n"), 0o755)
	os.WriteFile(filepath.Join(bin, "fc-list"), []byte("#!/bin/sh\n"), 0o755)
	os.WriteFile(filepath.Join(bin, "versions.txt"), []byte("toolchain 3\nffmpeg 7.1.2\nlibass 0.17.5\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "fonts", "fonts.conf"), []byte("<fontconfig/>"), 0o644)
	t.Setenv("FONTCONFIG_FILE", "")
	if v := toolchainVersion(filepath.Join(bin, "ffmpeg"), "libass"); v != "0.17.5" {
		t.Fatalf("libass %q", v)
	}
	if fc := toolchainFonts(filepath.Join(bin, "ffmpeg")); fc != filepath.Join(bin, "fc-list") {
		t.Fatalf("fc-list %q", fc)
	}
	if got := os.Getenv("FONTCONFIG_FILE"); got != filepath.Join(dir, "fonts", "fonts.conf") {
		t.Fatalf("FONTCONFIG_FILE %q", got)
	}
	// One set already (the image's) stays.
	t.Setenv("FONTCONFIG_FILE", "/etc/fonts/encode.conf")
	toolchainFonts(filepath.Join(bin, "ffmpeg"))
	if got := os.Getenv("FONTCONFIG_FILE"); got != "/etc/fonts/encode.conf" {
		t.Fatalf("overwrote FONTCONFIG_FILE: %q", got)
	}
	if fc := toolchainFonts("/nonexistent/ffmpeg"); fc != "fc-list" {
		t.Fatalf("without the toolchain: %q", fc)
	}
}

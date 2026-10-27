package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// **--toolchain-versions** (v0.2.8): in Slipstream's signed bundle ffmpeg and whisper-cli sit in
// Contents/Helpers and versions.txt in Contents/Resources, so the file is named, not found.
func TestToolchainVersionsFile(t *testing.T) {
	dir := t.TempDir()
	helpers, resources := filepath.Join(dir, "Contents", "Helpers"), filepath.Join(dir, "Contents", "Resources")
	for _, d := range []string{helpers, resources} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cli := filepath.Join(helpers, "whisper-cli")
	if err := os.WriteFile(cli, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	versions := filepath.Join(resources, "versions.txt")
	if err := os.WriteFile(versions, []byte("toolchain 3\nffmpeg 9.0.2\nlibass 0.17.5\nwhisper.cpp v1.9.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ffmpeg := filepath.Join(helpers, "ffmpeg")

	// Without the flag nothing is found beside the binaries.
	if v := toolVersion(Config{}, ffmpeg, "libass"); v != "" {
		t.Errorf("libass beside ffmpeg: %q", v)
	}
	cfg := Config{ToolchainVersions: versions}
	if v := toolVersion(cfg, ffmpeg, "libass"); v != "0.17.5" {
		t.Errorf("libass from the file: %q", v)
	}
	if v := toolVersion(cfg, ffmpeg, "x265"); v != "" {
		t.Errorf("a tool the file does not name: %q", v)
	}
	tools, _ := DetectTools(context.Background(), Config{FFmpeg: filepath.Join(dir, "no-ffmpeg"), WhisperCLI: cli,
		ToolchainVersions: versions})
	if !hasTool(tools, "whisper.cpp", "v1.9.5") {
		t.Errorf("whisper.cpp from the file: %+v", tools)
	}
	// --whisper-version still wins, as before.
	tools, _ = DetectTools(context.Background(), Config{FFmpeg: filepath.Join(dir, "no-ffmpeg"), WhisperCLI: cli,
		ToolchainVersions: versions, WhisperVersion: "v1.9.6"})
	if !hasTool(tools, "whisper.cpp", "v1.9.6") {
		t.Errorf("--whisper-version: %+v", tools)
	}
	// A versions.txt beside the binary, as in the image, still counts without the flag.
	if err := os.WriteFile(filepath.Join(helpers, "versions.txt"), []byte("whisper.cpp v1.9.4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tools, _ = DetectTools(context.Background(), Config{FFmpeg: filepath.Join(dir, "no-ffmpeg"), WhisperCLI: cli})
	if !hasTool(tools, "whisper.cpp", "v1.9.4") {
		t.Errorf("versions.txt beside whisper-cli: %+v", tools)
	}
}

func hasTool(tools []Tool, name, version string) bool {
	for _, t := range tools {
		if t.Name == name && t.Version == version {
			return true
		}
	}
	return false
}

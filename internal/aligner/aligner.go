// Package aligner is the align job's model and engines (v0.2.16; Thorsten approved
// video-pipeline's docs/aligner-timing.md on 2026-10-28): Qwen3-ForcedAligner-0.6B times the
// words of a text that already exists in its sound. On a Mac through MLX and mlx-qwen3-asr, as
// that measurement ran it, set up by the worker itself; on Linux through PyTorch and qwen-asr on
// the CPU, in the image: "The macs are not always there. We need a fallback that works"
// (Thorsten). The queue and the worker share what is here.
package aligner

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// The model: the official repository at a pinned revision, bfloat16 as published (1.84 GB of
// weights). Its sha256 is its manifest's (Manifest): a job names it as a whisper job names its
// model, and a worker that holds every file reports it under it.
const (
	Name     = "Qwen3-ForcedAligner-0.6B"
	Repo     = "Qwen/Qwen3-ForcedAligner-0.6B"
	Revision = "c7cbfc2048c462b0d63a45797104fc9db3ad62b7"
	// Dir is the model's folder in a worker's models folder (encoder's: infra puts it there).
	Dir    = "qwen3-forced-aligner-0.6b-c7cbfc20"
	SHA256 = "32f47d21e29b4029b9911c4ff8549e5187b099d52340f675f9717bde1f37ad4e"
)

// File is one file of the model at the revision, as its tree lists it (read 2026-10-28); the
// README and .gitattributes are left out.
type File struct {
	Name   string
	SHA256 string
	Bytes  int64
}

// URL is the file at the pinned revision, never a branch.
func (f File) URL() string {
	return "https://huggingface.co/" + Repo + "/resolve/" + Revision + "/" + f.Name
}

// Files are the model's files.
var Files = []File{
	{"chat_template.json", "75a8cfca24f00de72d796fbfed6858fc9614ef3dabd8696684cc3bc03a9c58ff", 1161},
	{"config.json", "d616c65d46c4b90bdc651b0a0963ea932732241140f337f9bb6b0335a9c8ef09", 5982},
	{"generation_config.json", "948d089b23bca1d214e768d59c4438365665f52ec6d33678f4062206b3fbbb8c", 115},
	{"merges.txt", "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5", 1671853},
	{"model.safetensors", "47831d0e82f96b20e9034dba01a075ee06436654719f6a68289e49f1b65ce0e7", 1835544544},
	{"preprocessor_config.json", "45e120a4eda2c20c5d7f2ea9354e63536bf35e27aa573fb7cdf78017b378770d", 330},
	{"tokenizer_config.json", "3ab80063f8511deb9566e6ad438d17b7a6277fcffd52d92854112f19d36bd81c", 12666},
	{"vocab.json", "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910", 2776833},
}

// Manifest is the sha256 of the lines "<sha256>  <name>\n" of the files, sorted by name.
func Manifest(files []File) string {
	lines := make([]string, len(files))
	for i, f := range files {
		lines[i] = f.SHA256 + "  " + f.Name + "\n"
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i][66:] < lines[j][66:] })
	sum := sha256.Sum256([]byte(strings.Join(lines, "")))
	return hex.EncodeToString(sum[:])
}

// Bytes is the model's size.
func Bytes() int64 {
	var n int64
	for _, f := range Files {
		n += f.Bytes
	}
	return n
}

// The engines, best first: a worker reports the one it has as a tool with its version, and a job
// goes to a Mac's MLX before encoder's CPU, as a better whisper model goes first.
const (
	EngineMLX   = "mlx-qwen3-asr"
	EngineTorch = "qwen-asr"
)

var Engines = []string{EngineMLX, EngineTorch}

// Rank is an engine's place in Engines, or -1.
func Rank(engine string) int {
	for i, e := range Engines {
		if e == engine {
			return i
		}
	}
	return -1
}

const gib = 1 << 30

// PeakBytes is a job's expected peak on an engine, for the RAM gate and the slots. MLX: 3.0 to
// 3.4 GiB on every episode of #24 with its cache freed after each piece. PyTorch: the lean
// precision's, 2.5 to 2.9 GB (0.8 GB of its own and 1.7 GB of the weights' pages, encoder,
// 2026-10-28), which the worker uses whenever float32's 4.4 to 4.8 GB do not fit
// (TorchFloat32Bytes): beside a 4K `slower` archive encode on encoder (10 GB, 5.5 GB for the
// encode) it fits, and the queue takes no archive lease back for it.
var PeakBytes = map[string]int64{EngineMLX: 7 * gib / 2, EngineTorch: 3 * gib}

// TorchFloat32Bytes is the memory beside what runs that a CPU worker needs to align in float32
// (4.4 to 4.8 GB at its peak in its cgroup on encoder; killed in a 4.5 GB cap) with the
// headroom; below it, it aligns lean, at 0.57 of the audio's length instead of 0.15, the same
// times to within 80 ms for 1613 of 1641 words against 1612 (KW41's first ten minutes).
const TorchFloat32Bytes = 6 * gib

// Speed is an engine's seconds of audio per second of work, and LoadS its start, for the
// expected completion time before a worker has results: MLX 0.011 of the audio's length and
// 1.4 s to load on the Mac Studio (#24); PyTorch on encoder's six cores in float32 0.15 and 7 s
// (2026-10-28). Each worker's results correct them (ect.go's learn).
var (
	Speed = map[string]float64{EngineMLX: 90, EngineTorch: 6.7}
	LoadS = map[string]float64{EngineMLX: 1.5, EngineTorch: 8}
)

// PieceS is the longest piece of sound one call aligns (align.py's PIECE_S).
const PieceS = 30

// Languages are the aligner's languages by whisper's codes (align.py's LANGUAGES). Japanese and
// Korean need tokenizers the Mac's engine leaves out.
var Languages = map[string]string{"zh": "Chinese", "en": "English", "yue": "Cantonese", "fr": "French",
	"de": "German", "it": "Italian", "pt": "Portuguese", "ru": "Russian", "es": "Spanish"}

// MaxWordsBytes is the most a words file may weigh (align.py takes up to 100,000 words).
const MaxWordsBytes = 8 << 20

// Script is align.py, which the worker writes to its state folder and runs with the engine's
// Python.
//
//go:embed align.py
var Script []byte

// RequirementsMLX is the Mac's engine, every package by its hashes.
//
//go:embed requirements-mlx.txt
var RequirementsMLX []byte

// Python is a python-build-standalone CPython for the Mac's engine: fully relocatable, the same
// 3.12.12 uv put under video-pipeline's measurement (build 20260114).
type Python struct {
	URL    string
	SHA256 string
	Bytes  int64
}

// PythonDarwinArm64 is the one the worker fetches on an Apple-silicon Mac.
var PythonDarwinArm64 = Python{
	URL:    "https://github.com/astral-sh/python-build-standalone/releases/download/20260114/cpython-3.12.12%2B20260114-aarch64-apple-darwin-install_only_stripped.tar.gz",
	SHA256: "ed1f300bd3b45aa481d887b2dd8e12f989b583f67755c219a6092756a09b609f",
	Bytes:  17661947,
}

// EngineID names an engine's setup: the Python's and the requirements' sha256, so a new one is
// installed again.
func EngineID(py Python, requirements []byte) string {
	sum := sha256.Sum256(append([]byte(py.SHA256+"\n"), requirements...))
	return fmt.Sprintf("%x", sum[:8])
}

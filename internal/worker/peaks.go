package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// **The peaks job** (v0.2.22; Thorsten, 2026-10-28: "0 encoding/calculating/handling of media is
// happening outside of Slipstream instances"): the sound of an upload as the peaks Funkhaus's
// timeline draws its waveform from. ffmpeg reads the first sound, mono at 8 kHz, 16-bit; the
// worker keeps the lowest and highest sample of every 80 as signed bytes and four coarser levels,
// each the min and max of four slices of the one before. The file is upload-center's `makePeaks`
// (src/lib/peaks.ts) to the byte, and video-pipeline's port before this job (its golden file is
// testdata/peaks-lcg-3s.peaks); upload-center's docs/handoff.md, "Audio peaks", has the format.

const (
	peaksMagic   = "FKPK"
	peaksVersion = 1
	PeaksRate    = 8000 // samples a second, mono
	PeaksSlice   = 80   // samples a slice at level 0: 100 slices a second
	peaksFactor  = 4    // each level has a quarter of the slices of the one before
	peaksLevels  = 5    // 100, 25, 6.25, 1.5625 and 0.39 slices a second
)

// peaksArgs is ffmpeg reading the source's first sound as raw 16-bit mono at PeaksRate into
// pcm, one thread, with its progress on stdout.
func peaksArgs(ffmpeg, src, pcm string) []string {
	return []string{ffmpeg, "-hide_banner", "-nostdin", "-nostats", "-y", "-v", "error",
		"-progress", "pipe:1", "-threads", "1", "-i", src, "-map", "0:a:0", "-vn", "-sn", "-dn",
		"-ac", "1", "-ar", fmt.Sprint(PeaksRate), "-f", "s16le", pcm}
}

// peakByte is a sample as a signed byte, rounded as JavaScript's Math.round rounds (half up,
// never to even): floor(s/256 + 0.5), which for a whole s is an arithmetic shift.
func peakByte(s int) int8 {
	return int8(min(127, (s+128)>>8))
}

// PeaksSlices is level 0 of a 16-bit little-endian mono signal: min, max per PeaksSlice samples,
// the last slice as long as what is left (an odd last byte is dropped).
func PeaksSlices(r io.Reader) ([]int8, error) {
	br := bufio.NewReaderSize(r, 2*PeaksSlice*4096)
	var out []int8
	buf := make([]byte, 2*PeaksSlice)
	for {
		n, err := io.ReadFull(br, buf)
		if n >= 2 {
			lo, hi := 32767, -32768
			for i := 0; i+1 < n; i += 2 {
				s := int(int16(binary.LittleEndian.Uint16(buf[i:])))
				lo, hi = min(lo, s), max(hi, s)
			}
			out = append(out, peakByte(lo), peakByte(hi))
		}
		switch err {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF:
			return out, nil
		default:
			return nil, err
		}
	}
}

// PeaksFile is the peaks file of level 0 (PeaksSlices): the header (magic, version, levels,
// factor, rate, slice, then each level's slice count), then every level, level 0 first.
func PeaksFile(level0 []int8) []byte {
	per := [][]int8{level0}
	for range peaksLevels - 1 {
		prev := per[len(per)-1]
		var next []int8
		for i := 0; i < len(prev); i += 2 * peaksFactor {
			end := min(len(prev), i+2*peaksFactor)
			lo, hi := int8(127), int8(-128)
			for j := i; j < end; j += 2 {
				lo, hi = min(lo, prev[j]), max(hi, prev[j+1])
			}
			next = append(next, lo, hi)
		}
		per = append(per, next)
	}
	out := make([]byte, 0, 16+4*peaksLevels+2*len(level0)*4/3+64)
	out = append(out, peaksMagic...)
	out = append(out, peaksVersion, peaksLevels)
	out = binary.LittleEndian.AppendUint16(out, peaksFactor)
	out = binary.LittleEndian.AppendUint32(out, PeaksRate)
	out = binary.LittleEndian.AppendUint32(out, PeaksSlice)
	for _, p := range per {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(p)/2))
	}
	for _, p := range per {
		for _, b := range p {
			out = append(out, byte(b))
		}
	}
	return out
}

// workPeaks runs a peaks job: the sound into a raw file in the lease's directory (16 kB a second
// of sound), then its peaks into out. A source without sound fails as decode_failed, at once.
func (r *run) workPeaks(ctx context.Context, src, out string) ([]Tool, error) {
	pcm := filepath.Join(r.dir, "sound.s16le")
	var errs []string
	err := r.runToolTo(ctx, peaksArgs(r.w.cfg.FFmpeg, src, pcm), "ffmpeg", r.ffmpegProgress(), nil,
		func(line string) { errs = append(errs, line) })
	if err != nil {
		if f, ok := err.(*failure); ok && f.code == "encode_failed" && noSound(errs) {
			return nil, fail("decode_failed", "the source has no sound")
		}
		return nil, err
	}
	f, err := os.Open(pcm)
	if err != nil {
		return nil, fail("decode_failed", "ffmpeg wrote no sound: %v", err)
	}
	level0, err := PeaksSlices(f)
	f.Close()
	os.Remove(pcm)
	if err != nil {
		return nil, fail("other", "reading the sound: %v", err)
	}
	if len(level0) == 0 {
		return nil, fail("decode_failed", "the sound is empty")
	}
	if err := os.WriteFile(out, PeaksFile(level0), 0o644); err != nil {
		return nil, fail("other", "%v", err)
	}
	return r.w.toolsNamed("ffmpeg"), nil
}

// noSound says whether ffmpeg's lines say the source has no sound to map.
func noSound(lines []string) bool {
	for _, l := range lines {
		if strings.Contains(l, "matches no streams") || strings.Contains(l, "does not contain any stream") {
			return true
		}
	}
	return false
}

package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

// lcgPCM is the golden file's signal, made the same way in TypeScript, Python and here: three
// seconds at 8 kHz of a 32-bit LCG, loud for a second, silent for half, a quarter as loud after,
// and an odd tail of 37 samples, as 16-bit little-endian.
func lcgPCM() []byte {
	x := uint32(20261028)
	var out []byte
	for i := range 3*8000 + 37 {
		x = 1664525*x + 1013904223
		v := int32(x>>16) - 32768
		switch {
		case i < 8000:
		case i < 12000:
			v = 0
		default:
			v >>= 2
		}
		out = binary.LittleEndian.AppendUint16(out, uint16(int16(v)))
	}
	return out
}

func peaksGolden(t *testing.T) []byte {
	b, err := os.ReadFile("testdata/peaks-lcg-3s.peaks")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPeaksGolden: upload-center's makePeaks wrote testdata/peaks-lcg-3s.peaks over lcgPCM;
// the worker writes the same bytes.
func TestPeaksGolden(t *testing.T) {
	level0, err := PeaksSlices(bytes.NewReader(lcgPCM()))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := PeaksFile(level0), peaksGolden(t); !bytes.Equal(got, want) {
		t.Fatalf("%d bytes, the reference %d; first difference at %d", len(got), len(want), firstDiff(got, want))
	}
}

// TestPeaksHoweverTheSoundArrives: reads of one byte, or of odd sizes that split samples, give
// the same file.
func TestPeaksHoweverTheSoundArrives(t *testing.T) {
	for name, r := range map[string]io.Reader{
		"one byte": iotest.OneByteReader(bytes.NewReader(lcgPCM())),
		"halves":   iotest.HalfReader(bytes.NewReader(lcgPCM())),
	} {
		level0, err := PeaksSlices(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(PeaksFile(level0), peaksGolden(t)) {
			t.Errorf("%s: not the reference's bytes", name)
		}
	}
}

func TestPeaksHeader(t *testing.T) {
	b := peaksGolden(t)
	if string(b[:4]) != "FKPK" || b[4] != 1 || b[5] != 5 || binary.LittleEndian.Uint16(b[6:]) != 4 ||
		binary.LittleEndian.Uint32(b[8:]) != 8000 || binary.LittleEndian.Uint32(b[12:]) != 80 {
		t.Fatalf("header % x", b[:16])
	}
	var counts []uint32
	sum := 0
	for i := range 5 {
		c := binary.LittleEndian.Uint32(b[16+4*i:])
		counts = append(counts, c)
		sum += int(c)
	}
	if want := []uint32{301, 76, 19, 5, 2}; !slicesEqual(counts, want) { // 300 whole slices and the tail
		t.Errorf("counts %v, want %v", counts, want)
	}
	if len(b) != 16+4*5+2*sum {
		t.Errorf("%d bytes", len(b))
	}
}

// TestPeakByteRoundsAsJavaScript: Math.round rounds halves up (-1.5 is -1, 2.5 is 3), not to even.
func TestPeakByteRoundsAsJavaScript(t *testing.T) {
	for _, c := range []struct {
		s    int
		want int8
	}{{-384, -1}, {384, 2}, {640, 3}, {-32768, -128}, {32767, 127}, {0, 0}, {-128, 0}, {127, 0}, {128, 1}} {
		if got := peakByte(c.s); got != c.want {
			t.Errorf("peakByte(%d) = %d, want %d", c.s, got, c.want)
		}
	}
}

// TestRealPeaks: a peaks job on a real file with sound through this machine's ffmpeg, and one
// on a file without sound, which fails as decode_failed with a line that says so.
func TestRealPeaks(t *testing.T) {
	r, ff, src := realRun(t) // two seconds, a 440 Hz tone
	r.l.Job.Source.DurationS = 2
	out := filepath.Join(r.w.cfg.WorkDir, "out.peaks")
	if _, err := r.workPeaks(context.Background(), src, out); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	n0 := binary.LittleEndian.Uint32(b[16:])
	if string(b[:4]) != "FKPK" || n0 < 199 || n0 > 201 {
		t.Fatalf("header % x, %d slices at level 0 for 2 s", b[:20], n0)
	}
	loud := 0
	for i := 0; i < int(n0); i++ {
		lo, hi := int8(b[36+2*i]), int8(b[36+2*i+1])
		if lo < -10 && hi > 10 { // lavfi's sine is an eighth of full scale, ±16
			loud++
		}
	}
	if loud < int(n0)*9/10 {
		t.Errorf("the tone is loud in %d of %d slices", loud, n0)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "sound.s16le")); !os.IsNotExist(err) {
		t.Errorf("the raw sound is left: %v", err)
	}

	silent := filepath.Join(r.w.cfg.WorkDir, "silent.mp4")
	if b, err := exec.Command(ff, "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=96x54:rate=25:duration=1",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", silent).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, b)
	}
	_, err = r.workPeaks(context.Background(), silent, filepath.Join(r.w.cfg.WorkDir, "silent.peaks"))
	f, ok := err.(*failure)
	if !ok || f.code != "decode_failed" || !strings.Contains(f.msg, "no sound") {
		t.Fatalf("a file without sound: %v", err)
	}
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func slicesEqual[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

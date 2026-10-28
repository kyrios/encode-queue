package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestMeasureThrottle is how the throttle was chosen (v0.2.17; Thorsten, 2026-10-28: "3 of 12
// cores" while a 2160 archive encode ran at 978 %). On the calibration graph as a 3840x2160 HEVC
// master it runs the archive's 2160 rung (x265 slower, CRF 29, 5 Mbit/s VBV) for a minute each:
//   - with pools=N, as a tool started at the limit runs;
//   - with pools=12, held to N by the throttle;
//   - with pools=12 under macOS's background policy (taskpolicy -b, PRIO_DARWIN_BG);
//   - with pools=12, free;
//
// and logs the tool's cores by ps's %cpu each second (median, range) and by its CPU time over
// the minute, and the frames per second. Then it encodes a 4 s clip whole, held and freed by
// turns, and compares it with the same encode free: frames, a decode, and the bytes.
//
// It runs only with EQ_MEASURE_THROTTLE=1 and the toolchain's ffmpeg first on PATH;
// EQ_MEASURE_LIMIT is N (3), EQ_MEASURE_CLIP_ONLY=1 skips the minutes.
func TestMeasureThrottle(t *testing.T) {
	if os.Getenv("EQ_MEASURE_THROTTLE") != "1" {
		t.Skip("EQ_MEASURE_THROTTLE=1 measures the throttle against a free and a background encode")
	}
	ff, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	limit := 3
	if n, err := strconv.Atoi(os.Getenv("EQ_MEASURE_LIMIT")); err == nil && n > 0 {
		limit = n
	}
	dir := t.TempDir()
	sh := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v %s", args, err, out)
		}
		return string(out)
	}
	master := filepath.Join(dir, "master.mov")
	sh(ff, "-hide_banner", "-v", "error", "-y", "-f", "lavfi", "-i",
		"testsrc2=size=3840x2160:rate=25:duration=20,noise=alls=4:allf=t:all_seed=528,format=yuv420p",
		"-c:v", "libx265", "-preset", "ultrafast", "-crf", "22", "-pix_fmt", "yuv420p10le", "-tag:v", "hvc1",
		"-x265-params", "log-level=error", master)
	short := filepath.Join(dir, "short.mov")
	sh(ff, "-hide_banner", "-v", "error", "-y", "-i", master, "-t", "4", "-c", "copy", short)
	maxrate := 5000
	p := RungParams{Codec: "libx265", Width: 3840, Height: 2160, Preset: "slower", CRF: 29, VBVMaxrate: &maxrate}
	t.Logf("%s, %d CPUs, %s; limit %d", strings.TrimSpace(sh("sysctl", "-n", "machdep.cpu.brand_string")), runtime.NumCPU(),
		strings.TrimSpace(sh("uptime")), limit)

	// encode runs the rung on src with pools for d (0: to its end), with adjust after the start
	// (the policy, the limit); it returns ps's %cpu each second, the cores by CPU time, the fps.
	encode := func(name, src string, pools int, d time.Duration, adjust func(ctx context.Context, w *Worker, pid int)) ([]float64, float64, float64, string) {
		w, err := New(Config{StateDir: t.TempDir(), FFmpeg: ff}, nil)
		if err != nil {
			t.Fatal(err)
		}
		l := &Lease{ID: "ls_MEASURE_" + name, PauseGiveBackS: 600}
		l.Job.Source.Width, l.Job.Source.Height = 3840, 2160
		r := newRun(w, l, false)
		if err := os.MkdirAll(r.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		w.mu.Lock()
		w.runs[l.ID] = r
		w.mu.Unlock()
		out := filepath.Join(r.dir, name+".mp4")
		args, err := rungArgs(ff, p, src, 3840, 2160, "", out, pools)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var frames int64
		var fmu sync.Mutex
		done := make(chan error, 1)
		go func() {
			done <- r.runTool(ctx, args, "ffmpeg", func(b map[string]string) {
				if f, err := strconv.ParseInt(b["frame"], 10, 64); err == nil {
					fmu.Lock()
					frames = f
					fmu.Unlock()
				}
			})
		}()
		var loops sync.WaitGroup
		loops.Add(1)
		go func() { defer loops.Done(); r.throttleLoop(ctx) }()
		var pid int
		for pid == 0 {
			time.Sleep(10 * time.Millisecond)
			r.mu.Lock()
			if r.cmd != nil && r.cmd.Process != nil {
				pid = r.cmd.Process.Pid
			}
			r.mu.Unlock()
		}
		if adjust != nil {
			adjust(ctx, w, pid)
		}
		c0, _ := groupCPU(pid)
		t0 := time.Now()
		var pcpu []float64
		tick := time.NewTicker(time.Second)
	loop:
		for {
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				break loop
			case <-tick.C:
				if o, err := exec.Command("ps", "-o", "pcpu=", "-p", strconv.Itoa(pid)).Output(); err == nil {
					if v, err := strconv.ParseFloat(strings.TrimSpace(string(o)), 64); err == nil {
						pcpu = append(pcpu, v)
					}
				}
				if d > 0 && time.Since(t0) >= d {
					break loop
				}
			}
		}
		tick.Stop()
		el := time.Since(t0).Seconds()
		c1, _ := groupCPU(pid)
		fmu.Lock()
		f := frames
		fmu.Unlock()
		cancel()
		loops.Wait()
		if d > 0 {
			<-done
		}
		return pcpu, (c1 - c0).Seconds() / el, float64(f) / el, out
	}
	report := func(name string, pcpu []float64, cores, fps float64) {
		s := append([]float64(nil), pcpu...)
		sort.Float64s(s)
		if len(s) == 0 {
			t.Logf("%-30s no ps samples", name)
			return
		}
		t.Logf("%-30s ps %%cpu median %4.0f, %4.0f to %4.0f (%d samples); by CPU time %.2f cores; %.2f fps",
			name, s[len(s)/2], s[0], s[len(s)-1], len(s), cores, fps)
	}

	const minute = 60 * time.Second
	if os.Getenv("EQ_MEASURE_CLIP_ONLY") != "1" {
		pc, c, f, _ := encode("pools-limit", master, limit, minute, nil)
		report(fmt.Sprintf("pools=%d, free", limit), pc, c, f)
		pc, c, f, _ = encode("held", master, 12, minute, func(_ context.Context, w *Worker, _ int) { w.SetLimits(limit, false, 1, nil) })
		report(fmt.Sprintf("pools=12, held to %d", limit), pc, c, f)
		pc, c, f, _ = encode("darwin-bg", master, 12, minute, func(_ context.Context, _ *Worker, pid int) {
			sh("taskpolicy", "-b", "-p", strconv.Itoa(pid))
		})
		report("pools=12, taskpolicy -b", pc, c, f)
		pc, c, f, _ = encode("free", master, 12, minute, nil)
		report("pools=12, free", pc, c, f)
	}

	// The whole short clip, held and freed by turns, against the same encode free, twice: x265
	// with frame threads and VBV need not give the same bytes twice.
	_, _, _, free := encode("short-free", short, 12, 0, nil)
	_, _, _, free2 := encode("short-free2", short, 12, 0, nil)
	_, _, _, held := encode("short-held", short, 12, 0, func(ctx context.Context, w *Worker, _ int) {
		go func() {
			for i := 0; ; i++ {
				n := limit
				if i%2 == 1 {
					n = 0
				}
				w.SetLimits(n, false, 1, nil)
				if !sleep(ctx, 5*time.Second) {
					return
				}
			}
		}()
	})
	count := func(file string) string {
		return strings.TrimSpace(sh("ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0",
			"-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", file))
	}
	decode := func(file string) string {
		out, err := exec.Command(ff, "-hide_banner", "-v", "error", "-xerror", "-i", file, "-f", "null", "-").CombinedOutput()
		if err != nil || len(strings.TrimSpace(string(out))) > 0 {
			return fmt.Sprintf("errors: %v %s", err, out)
		}
		return "no errors"
	}
	sum := func(file string) string {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%x", sha256.Sum256(data))[:16]
	}
	ssim := func(file string) float64 {
		out := sh(ff, "-hide_banner", "-nostats", "-i", file, "-i", short, "-lavfi", "[0:v]format=yuv420p10le[a];[1:v]format=yuv420p10le[b];[a][b]ssim", "-f", "null", "-")
		i := strings.LastIndex(out, "All:")
		if i < 0 {
			t.Fatalf("no SSIM: %s", out)
		}
		v, _ := strconv.ParseFloat(strings.Fields(out[i+4:])[0], 64)
		return v
	}
	size := func(file string) int64 {
		fi, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	want := count(short)
	for _, c := range []struct{ name, file string }{{"free", free}, {"free again", free2}, {"held and freed by turns", held}} {
		t.Logf("4 s clip (%s frames), %-24s %s frames, decode %s, %d bytes, SSIM %.5f, sha256 %s",
			want, c.name+":", count(c.file), decode(c.file), size(c.file), ssim(c.file), sum(c.file))
	}
	if count(held) != want || decode(held) != "no errors" {
		t.Fatal("the held encode is not whole")
	}
	if d := ssim(held) - ssim(free); d < -0.002 || d > 0.002 {
		t.Fatalf("the held encode's SSIM is %.5f off the free one's", d)
	}
}

// TestMeasureThrottleWhisper holds whisper-cli to the limit as the throttle holds x265: with
// EQ_MEASURE_THROTTLE=1 and EQ_MEASURE_WHISPER_MODEL (small's .bin), it transcribes the
// calibration's speech clip with 12 threads, on the CPU and with the GPU, free and held to the
// limit, and compares each held transcript with the free one.
func TestMeasureThrottleWhisper(t *testing.T) {
	model := os.Getenv("EQ_MEASURE_WHISPER_MODEL")
	if os.Getenv("EQ_MEASURE_THROTTLE") != "1" || model == "" {
		t.Skip("EQ_MEASURE_THROTTLE=1 and EQ_MEASURE_WHISPER_MODEL hold whisper-cli to the limit")
	}
	cli, err := exec.LookPath("whisper-cli")
	if err != nil {
		t.Fatal(err)
	}
	limit := 3
	if n, err := strconv.Atoi(os.Getenv("EQ_MEASURE_LIMIT")); err == nil && n > 0 {
		limit = n
	}
	dir := t.TempDir()
	wav := filepath.Join(dir, "speech.wav")
	if err := os.WriteFile(wav, speechClip, 0o600); err != nil {
		t.Fatal(err)
	}
	children := func() time.Duration {
		var ru unix.Rusage
		if err := unix.Getrusage(unix.RUSAGE_CHILDREN, &ru); err != nil {
			t.Fatal(err)
		}
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	// transcribe returns the transcript's sha256, the cores it used and how long it took.
	transcribe := func(name string, gpu, held bool) (string, float64, time.Duration) {
		w, err := New(Config{StateDir: t.TempDir(), WhisperCLI: cli}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if held {
			w.SetLimits(limit, gpu, 1, nil)
		}
		r := newRun(w, &Lease{ID: "ls_MEASURE_" + name, PauseGiveBackS: 600}, false)
		if err := os.MkdirAll(r.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		base := filepath.Join(r.dir, "out")
		args, err := whisperArgs(cli, model, WhisperParams{Language: "de"}, wav, base, 12, gpu)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		var loops sync.WaitGroup
		loops.Add(1)
		go func() { defer loops.Done(); r.throttleLoop(ctx) }()
		c0, t0 := children(), time.Now()
		err = r.runToolTo(ctx, args, "whisper", nil, nil, nil)
		el, c1 := time.Since(t0), children()
		cancel()
		loops.Wait()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		data, err := os.ReadFile(base + ".json")
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%x", sha256.Sum256(data))[:16], (c1 - c0).Seconds() / el.Seconds(), el
	}
	for _, gpu := range []bool{false, true} {
		free, c, el := transcribe(fmt.Sprintf("free-gpu-%v", gpu), gpu, false)
		t.Logf("gpu %v, free: %.1f s, %.2f cores, transcript %s", gpu, el.Seconds(), c, free)
		held, c, el := transcribe(fmt.Sprintf("held-gpu-%v", gpu), gpu, true)
		t.Logf("gpu %v, held to %d: %.1f s, %.2f cores, transcript %s", gpu, limit, el.Seconds(), c, held)
		if held != free {
			t.Errorf("gpu %v: the held transcript differs from the free one", gpu)
		}
	}
}

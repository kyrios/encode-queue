package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// failure is a job that failed here, with helper.yaml's code.
type failure struct {
	code  string
	msg   string
	extra map[string]any // signal, exit_code, memory_available_bytes
}

func (f *failure) Error() string { return f.code + ": " + f.msg }

func fail(code, format string, args ...any) error {
	return &failure{code: code, msg: fmt.Sprintf(format, args...)}
}

type sample struct {
	at    time.Time
	frame int64
}

// run is one lease being worked.
type run struct {
	w       *Worker
	l       *Lease
	dir     string
	started time.Time
	slot    string // normal or high, the claim loop's (v0.2.14)

	mu          sync.Mutex
	flushMu     sync.Mutex
	prog        Progress
	logBuf      []LogLine
	tail        []string
	stopCode    string // why the run was stopped from outside: a 410 code, "paused"
	cancel      context.CancelFunc
	cmd         *exec.Cmd
	suspended   bool
	pausedSince *time.Time
	samples     []sample
	encodeStart time.Time
	// rate: the rung's fps (v0.2.13), so the check's comparison takes the source at that rate.
	rate       FPS
	encodeTime time.Duration
}

func newRun(w *Worker, l *Lease, high bool) *run {
	r := &run{w: w, l: l, dir: filepath.Join(w.cfg.WorkDir, l.ID), started: time.Now(), prog: Progress{Phase: "downloading"}, slot: "normal"}
	if high {
		r.slot = "high"
	}
	return r
}

func (r *run) status() JobStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := JobStatus{LeaseID: r.l.ID, JobID: r.l.Job.ID, Title: r.l.Job.Title, Kind: r.l.Job.Kind, Slot: r.slot, Attempt: r.l.Attempt,
		Started: r.started, Progress: r.report(), Log: append([]string(nil), r.tail...)}
	if r.l.Job.Kind == "rung" {
		var p RungParams
		if json.Unmarshal(r.l.Params, &p) == nil {
			st.Rung = p.Rung
		}
	}
	return st
}

// report is the progress as the queue gets it; call with r.mu held.
func (r *run) report() Progress {
	p := r.prog
	if r.pausedSince != nil {
		p.Phase = "paused"
		p.PausedSince = r.pausedSince
	}
	return p
}

func (r *run) log(level, source, msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	r.logBuf = append(r.logBuf, LogLine{Time: time.Now().UTC(), Level: level, Source: source, Message: msg})
	if len(r.logBuf) > 5000 { // the queue is away: keep the newest
		r.logBuf = r.logBuf[len(r.logBuf)-5000:]
	}
	r.tail = append(r.tail, msg)
	if len(r.tail) > 50 {
		r.tail = r.tail[len(r.tail)-50:]
	}
	// Only a lease's lines go to the app. A calibration's run has none (v0.2.12: with Events set,
	// in Slipstream, its first stderr line was a nil dereference, at every start).
	if r.w.cfg.Events != nil && r.l != nil {
		go r.w.emit("log", map[string]any{"lease_id": r.l.ID, "line": msg})
	}
}

// eventLoop tells the app about the job once a second.
func (r *run) eventLoop(ctx context.Context) {
	if r.w.cfg.Events == nil {
		return
	}
	var last time.Time
	phase := ""
	for {
		st := r.status()
		p := st.Progress
		if p.Phase != phase || time.Since(last) >= time.Second {
			phase, last = p.Phase, time.Now()
			r.w.emit("job", map[string]any{"lease_id": st.LeaseID, "title": st.Title, "kind": st.Kind, "rung": st.Rung, "slot": st.Slot,
				"phase": p.Phase, "fraction": p.Fraction, "fps": p.FPS, "speed": p.Speed, "eta_s": p.ETAS})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (r *run) stop(code string) {
	r.mu.Lock()
	if r.stopCode == "" {
		r.stopCode = code
	}
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *run) setPhase(phase string) {
	r.mu.Lock()
	r.prog = Progress{Phase: phase}
	r.mu.Unlock()
}

// execute works the lease and ends it: delivered, failed, given back or stopped.
func (r *run) execute(parent context.Context) Finished {
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()
	defer cancel()
	if err := os.MkdirAll(filepath.Join(r.dir, "out"), 0o700); err != nil {
		return r.end(parent, err)
	}
	loops, loopsCtx := sync.WaitGroup{}, ctx
	loops.Add(4)
	go func() { defer loops.Done(); r.eventLoop(loopsCtx) }()
	go func() { defer loops.Done(); r.renewLoop(loopsCtx) }()
	go func() { defer loops.Done(); r.logLoop(loopsCtx) }()
	go func() { defer loops.Done(); r.pauseLoop(loopsCtx) }()
	err := r.work(ctx)
	cancel()
	loops.Wait()
	return r.end(parent, err)
}

func (r *run) end(parent context.Context, err error) Finished {
	bg, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.mu.Lock()
	code := r.stopCode
	r.mu.Unlock()
	api := r.w.api
	switch {
	case err == nil:
		// Delivered. A renewal racing the delivery may have heard 410 for the lease it just
		// ended; that changes nothing.
		return Finished{Outcome: "checking"}
	case code == "paused":
		r.flushLogs(bg)
		api.Release(bg, r.l.ID, "paused")
		return Finished{Outcome: "given_back", Reason: "paused"}
	case code != "":
		return Finished{Outcome: "stopped", Reason: code}
	case parent.Err() != nil:
		r.flushLogs(bg)
		api.Release(bg, r.l.ID, "quit")
		return Finished{Outcome: "given_back", Reason: "quit"}
	}
	if c, ok := gone(err); ok {
		return Finished{Outcome: "stopped", Reason: c}
	}
	var f *failure
	if !errors.As(err, &f) {
		f = &failure{code: "other", msg: err.Error()}
	}
	r.log("error", "worker", f.Error())
	r.flushLogs(bg)
	body := map[string]any{"outcome": "failed", "code": f.code, "message": oneLine(f.msg)}
	for k, v := range f.extra {
		body[k] = v
	}
	if _, perr := api.PutResult(bg, r.l.ID, body); perr != nil {
		if c, ok := gone(perr); ok {
			return Finished{Outcome: "stopped", Reason: c}
		}
		api.Release(bg, r.l.ID, "user")
	}
	return Finished{Outcome: "failed", Reason: f.Error()}
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 500 {
		s = s[:500]
	}
	return s
}

func (r *run) renewLoop(ctx context.Context) {
	every := time.Duration(r.l.RenewEveryS) * time.Second
	if every <= 0 {
		every = 30 * time.Second
	}
	for {
		r.mu.Lock()
		p := r.report()
		r.mu.Unlock()
		rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		l, err := r.w.api.Renew(rctx, r.l.ID, p)
		cancel()
		r.w.contact(err)
		if err == nil {
			r.w.setPause(l.PausedUntil, true)
		} else if code, ok := gone(err); ok {
			r.w.cfg.Log.Info("the lease is gone", "lease", r.l.ID, "code", code)
			r.stop(code)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

func (r *run) flushLogs(ctx context.Context) {
	// One flush at a time: the log loop and the end of the job both flush, and each cuts what
	// it sent off the front of the buffer.
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	for {
		r.mu.Lock()
		n := len(r.logBuf)
		if n == 0 {
			r.mu.Unlock()
			return
		}
		if n > 500 {
			n = 500
		}
		batch := append([]LogLine(nil), r.logBuf[:n]...)
		r.mu.Unlock()
		if err := r.w.api.Logs(ctx, r.l.ID, batch); err != nil {
			return
		}
		r.mu.Lock()
		r.logBuf = r.logBuf[n:]
		r.mu.Unlock()
	}
}

func (r *run) logLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.w.cfg.LogEvery):
		}
		r.flushLogs(ctx)
	}
}

// pauseLoop suspends the tool while the worker is paused and gives the job back once the
// pause held for pause_give_back_s.
func (r *run) pauseLoop(ctx context.Context) {
	giveBack := time.Duration(r.l.PauseGiveBackS) * time.Second
	if giveBack <= 0 {
		giveBack = 60 * time.Second
	}
	for {
		paused := r.w.paused()
		r.mu.Lock()
		switch {
		case paused && r.pausedSince == nil:
			now := time.Now().UTC()
			r.pausedSince = &now
			r.signal(syscall.SIGSTOP)
			r.suspended = true
			r.mu.Unlock()
			r.log("info", "worker", "paused")
			r.mu.Lock()
		case paused && time.Since(*r.pausedSince) >= giveBack:
			r.mu.Unlock()
			r.log("info", "worker", "the pause holds: giving the job back")
			r.stop("paused")
			return
		case !paused && r.pausedSince != nil:
			r.pausedSince = nil
			if r.suspended {
				r.signal(syscall.SIGCONT)
				r.suspended = false
			}
			r.mu.Unlock()
			r.log("info", "worker", "resumed")
			r.mu.Lock()
		}
		r.mu.Unlock()
		r.w.mu.Lock()
		ch := r.w.pauseCh
		r.w.mu.Unlock()
		select {
		case <-ctx.Done():
			r.mu.Lock()
			if r.suspended {
				r.signal(syscall.SIGCONT)
				r.suspended = false
			}
			r.mu.Unlock()
			return
		case <-ch:
		case <-time.After(time.Second):
		}
	}
}

// signal sends sig to the running tool's process group; call with r.mu held.
func (r *run) signal(sig syscall.Signal) {
	if r.cmd != nil && r.cmd.Process != nil {
		syscall.Kill(-r.cmd.Process.Pid, sig)
	}
}

func (r *run) work(ctx context.Context) error {
	l := r.l
	if l.Job.Kind == "check" {
		return r.workCheck(ctx)
	}
	wait := r.w.waitUnpaused
	src := filepath.Join(r.dir, "source")
	total := l.Job.Source.Bytes
	err := r.w.api.Download(ctx, l.Job.Source.FileRef, src, func(n int64) {
		r.mu.Lock()
		f := float64(n) / float64(max64(total, 1))
		r.prog = Progress{Phase: "downloading", BytesDone: &n, BytesTotal: &total, Fraction: &f}
		r.mu.Unlock()
	}, wait)
	if err != nil {
		var ce *checksumError
		if errors.As(err, &ce) {
			return fail("source_checksum", "%v", err)
		}
		return err
	}
	inputs := map[string]string{}
	for name, f := range l.Job.Inputs {
		p := filepath.Join(r.dir, "input-"+name)
		if err := r.w.api.Download(ctx, f, p, func(int64) {}, wait); err != nil {
			var ce *checksumError
			if errors.As(err, &ce) {
				return fail("source_checksum", "input %s: %v", name, err)
			}
			return err
		}
		inputs[name] = p
	}
	r.log("info", "worker", fmt.Sprintf("source %d bytes, sha256 checked", total))

	out := filepath.Join(r.dir, "out", l.Result.Name)
	var tools []Tool
	// The check after the encode (v0.2.8), and what it needs to know of the result.
	var check *CheckSettings
	var levels *AudioLevels
	w, h := l.Job.Source.Width, l.Job.Source.Height
	sameFrames := false // the result has the source's frames: a rung's frame count is the source's
	r.mu.Lock()
	r.prog = Progress{Phase: "encoding"}
	r.encodeStart = time.Now()
	r.mu.Unlock()
	switch l.Job.Kind {
	case "rung":
		var p RungParams
		if err := json.Unmarshal(l.Params, &p); err != nil {
			return fail("other", "params: %v", err)
		}
		ass := ""
		if p.Captions != nil {
			in := inputs[p.Captions.Input]
			if in == "" {
				return fail("file_not_found", "input %s", p.Captions.Input)
			}
			// libass gets the file by its name in the lease's folder, where ffmpeg runs (runToolTo):
			// the work dir's own path never goes into the filter graph, so any work dir works. A
			// Mac's sits under "Application Support" (v0.2.14: Vimes refused every captions job).
			ass = filepath.Base(in)
		}
		if p.Audio != nil && p.Audio.Mono.Measure {
			var err error
			if levels, err = r.measureMono(ctx, src, p.Audio); err != nil {
				return err
			}
		}
		check, w, h, sameFrames = p.Check, p.Width, p.Height, true
		frames := l.Job.Source.Frames
		if p.FPS != nil {
			// v0.2.13: at another rate the file has its own frames, not the source's, so its check
			// reports no source_frames, and the progress counts the rate's frames.
			sameFrames, frames, r.rate = false, p.FPS.frames(l.Job.Source.DurationS), *p.FPS
		}
		args, err := rungArgs(r.w.cfg.FFmpeg, p, src, l.Job.Source.Width, l.Job.Source.Height, ass, out, r.w.cfg.CPUThreads)
		if err != nil {
			return fail("other", "%v", err)
		}
		r.log("info", "worker", strings.Join(args, " "))
		if err := r.runTool(ctx, args, "ffmpeg", r.progressOf(frames)); err != nil {
			return err
		}
		tools = r.w.toolsNamed("ffmpeg", "libx265", "libx264")
	case "cut":
		var p CutParams
		if err := json.Unmarshal(l.Params, &p); err != nil {
			return fail("other", "params: %v", err)
		}
		args, err := cutArgs(r.w.cfg.FFmpeg, p, src, out)
		if err != nil {
			return fail("other", "%v", err)
		}
		check = p.Check
		r.log("info", "worker", strings.Join(args, " "))
		if err := r.runTool(ctx, args, "ffmpeg", r.ffmpegProgress()); err != nil {
			return err
		}
		tools = r.w.toolsNamed("ffmpeg", "libx264")
	case "remux":
		var p RemuxParams
		if err := json.Unmarshal(l.Params, &p); err != nil {
			return fail("other", "params: %v", err)
		}
		check, sameFrames = p.Check, true
		args := remuxArgs(r.w.cfg.FFmpeg, p, src, out)
		r.log("info", "worker", strings.Join(args, " "))
		if err := r.runTool(ctx, args, "ffmpeg", r.ffmpegProgress()); err != nil {
			return err
		}
		tools = r.w.toolsNamed("ffmpeg")
	case "whisper":
		var p WhisperParams
		if err := json.Unmarshal(l.Params, &p); err != nil {
			return fail("other", "params: %v", err)
		}
		model := r.w.modelPath(p.ModelSHA256)
		if model == "" {
			return fail("model_missing", "no model with sha256 %s", p.ModelSHA256)
		}
		wav := src
		names := []string{"whisper.cpp"}
		if l.Job.Source.Container != "wav" {
			wav = filepath.Join(r.dir, "speech.wav")
			if err := r.runTool(ctx, speechArgs(r.w.cfg.FFmpeg, src, wav), "ffmpeg", nil); err != nil {
				return err
			}
			names = append(names, "ffmpeg")
		}
		threads := p.Threads
		if threads <= 0 {
			threads = 4
		}
		if c := r.w.cfg.CPUThreads; c > 0 && threads > c {
			threads = c
		}
		base := filepath.Join(r.dir, "out", "whisper")
		args, err := whisperArgs(r.w.cfg.WhisperCLI, model, p, wav, base, threads, r.w.cfg.GPU)
		if err != nil {
			return fail("other", "%v", err)
		}
		r.log("info", "worker", strings.Join(args, " "))
		if err := r.runTool(ctx, args, "whisper", nil); err != nil {
			return err
		}
		if base+".json" != out {
			if err := os.Rename(base+".json", out); err != nil {
				return fail("encode_failed", "whisper wrote no JSON: %v", err)
			}
		}
		tools = r.w.toolsNamed(names...)
	default:
		return fail("other", "job kind %q unknown", l.Job.Kind)
	}
	r.mu.Lock()
	r.encodeTime = time.Since(r.encodeStart)
	frames := int64(0)
	if r.prog.FramesDone != nil {
		frames = *r.prog.FramesDone
	}
	r.mu.Unlock()
	fi, err := os.Stat(out)
	if err != nil || fi.Size() == 0 {
		return fail("local_check_failed", "the tool made no file")
	}
	if l.Result.MaxBytes > 0 && fi.Size() > l.Result.MaxBytes {
		return fail("local_check_failed", "the file has %d bytes, more than the %d allowed", fi.Size(), l.Result.MaxBytes)
	}
	r.log("info", "worker", fmt.Sprintf("made %s, %d bytes in %.1f s", l.Result.Name, fi.Size(), r.encodeTime.Seconds()))

	var figures *CheckFigures
	if check != nil {
		var srcFrames *int64
		if sameFrames && frames > 0 {
			srcFrames = &frames
		}
		if figures, err = r.checkResult(ctx, check, out, src, w, h, sameFrames, srcFrames, true); err != nil {
			return err
		}
	}
	if levels != nil {
		if figures == nil {
			figures = &CheckFigures{}
		}
		figures.AudioLevels = levels
	}

	size := fi.Size()
	r.setPhase("uploading")
	loc, sum, err := r.w.api.Upload(ctx, l.ID, out, r.w.cfg.ChunkBytes, func(n int64) {
		r.mu.Lock()
		f := float64(n) / float64(max64(size, 1))
		r.prog = Progress{Phase: "uploading", BytesDone: &n, BytesTotal: &size, Fraction: &f}
		r.mu.Unlock()
	}, wait)
	if err != nil {
		return err
	}
	r.flushLogs(ctx)
	body := map[string]any{"outcome": "file", "file": loc, "sha256": sum, "encode_s": round1(r.encodeTime.Seconds()), "tools": tools}
	if frames > 0 {
		body["frames"] = frames
	}
	if figures != nil {
		body["check"] = figures
	}
	res, err := r.w.api.PutResult(ctx, l.ID, body)
	if err != nil {
		return err
	}
	r.log("info", "worker", "delivered; the verdict is "+res.Verdict)
	return nil
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (w *Worker) toolsNamed(names ...string) []Tool {
	var out []Tool
	for _, n := range names {
		for _, t := range w.caps.Tools {
			if t.Name == n {
				out = append(out, t)
			}
		}
	}
	return out
}

var whisperProgressRe = regexp.MustCompile(`progress\s*=\s*(\d+)\s*%`)

// ffmpegProgress reads ffmpeg's -progress blocks into the run's progress, with frames per
// second over the last 10 s.
func (r *run) ffmpegProgress() func(map[string]string) { return r.progressOf(r.l.Job.Source.Frames) }

// progressOf is ffmpegProgress for a file of total frames; 0 when unknown, and the time decides.
func (r *run) progressOf(total int64) func(map[string]string) {
	dur := r.l.Job.Source.DurationS
	return func(b map[string]string) {
		now := time.Now()
		frame, _ := strconv.ParseInt(b["frame"], 10, 64)
		outUS, _ := strconv.ParseInt(b["out_time_us"], 10, 64)
		speed, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(b["speed"]), "x"), 64)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.samples = append(r.samples, sample{now, frame})
		for len(r.samples) > 2 && now.Sub(r.samples[0].at) > 10*time.Second {
			r.samples = r.samples[1:]
		}
		p := Progress{Phase: "encoding", FramesDone: &frame}
		if outUS > 0 {
			ot := float64(outUS) / 1e6
			p.OutTimeS = &ot
		}
		if n, err := strconv.ParseInt(b["total_size"], 10, 64); err == nil {
			p.TotalSize = &n
		}
		if v, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(b["bitrate"]), "kbits/s"), 64); err == nil {
			p.BitrateKbps = &v
		}
		if s0 := r.samples[0]; now.Sub(s0.at) > 500*time.Millisecond {
			fps := float64(frame-s0.frame) / now.Sub(s0.at).Seconds()
			fps = float64(int64(fps*100+0.5)) / 100
			p.FPS = &fps
		} else if v, err := strconv.ParseFloat(b["fps"], 64); err == nil {
			p.FPS = &v
		}
		if speed > 0 {
			p.Speed = &speed
		}
		var frac float64
		switch {
		case total > 0:
			p.FramesTotal = &total
			frac = float64(frame) / float64(total)
		case dur > 0:
			frac = float64(outUS) / 1e6 / dur
		}
		if frac > 1 {
			frac = 1
		}
		if frac > 0 {
			p.Fraction = &frac
			if el := now.Sub(r.encodeStart).Seconds(); el > 1 {
				eta := int64(el/frac - el)
				p.ETAS = &eta
			}
		}
		if p.FPS != nil && *p.FPS > 0 && total > frame {
			eta := int64(float64(total-frame) / *p.FPS)
			p.ETAS = &eta
		}
		r.prog = p
	}
}

// runTool runs a tool in its own process group, stdout as -progress blocks when onBlock is
// set, stderr as log lines. A cancelled ctx kills the group.
func (r *run) runTool(ctx context.Context, args []string, source string, onBlock func(map[string]string)) error {
	return r.runToolTo(ctx, args, source, onBlock, nil, nil)
}

// runToolTo is runTool with stdout's lines to outLine instead of the log (when there is no
// onBlock), and every stderr line to errLine as well as the log.
func (r *run) runToolTo(ctx context.Context, args []string, source string, onBlock func(map[string]string),
	outLine, errLine func(string)) error {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = r.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), "OMP_WAIT_POLICY=passive")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := start(cmd); err != nil {
		return fail("tool_missing", "%s: %v", args[0], err)
	}
	r.mu.Lock()
	r.cmd = cmd
	if r.pausedSince != nil { // paused between two tools
		r.signal(syscall.SIGSTOP)
		r.suspended = true
	}
	r.mu.Unlock()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.signal(syscall.SIGKILL)
			r.mu.Unlock()
		case <-done:
		}
	}()
	var last []string
	var lmu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		block := map[string]string{}
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if onBlock == nil {
				switch {
				case outLine != nil:
					outLine(line)
				case line != "":
					r.log("info", source, line)
				}
				continue
			}
			if k, v, ok := strings.Cut(line, "="); ok {
				block[k] = v
				if k == "progress" {
					onBlock(block)
					block = map[string]string{}
				}
			}
		}
		io.Copy(io.Discard, stdout)
	}()
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			if errLine != nil {
				errLine(line)
			}
			if source == "whisper" {
				if m := whisperProgressRe.FindStringSubmatch(line); m != nil {
					pct, _ := strconv.Atoi(m[1])
					f := float64(pct) / 100
					r.mu.Lock()
					r.prog = Progress{Phase: "encoding", Fraction: &f}
					r.mu.Unlock()
					continue
				}
			}
			r.log("info", source, line)
			lmu.Lock()
			last = append(last, line)
			if len(last) > 5 {
				last = last[1:]
			}
			lmu.Unlock()
		}
		io.Copy(io.Discard, stderr)
	}()
	wg.Wait()
	err = cmd.Wait()
	close(done)
	r.mu.Lock()
	r.cmd = nil
	r.suspended = false
	r.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		lmu.Lock()
		msg := strings.Join(last, " | ")
		lmu.Unlock()
		return toolFailure(filepath.Base(args[0]), err, msg)
	}
	return nil
}

// toolFailure is how a tool ended: killed by a signal (the OOM killer's SIGKILL, mostly) is
// "killed", which the queue retries; an exit code is "encode_failed".
func toolFailure(tool string, err error, msg string) error {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			extra := map[string]any{"signal": unix.SignalName(ws.Signal())}
			text := fmt.Sprintf("%s: killed by %s", tool, unix.SignalName(ws.Signal()))
			if avail := memoryAvailable(); avail > 0 {
				extra["memory_available_bytes"] = avail
				text += fmt.Sprintf(", %d MiB memory available afterwards", avail>>20)
			}
			return &failure{code: "killed", msg: text + ": " + msg, extra: extra}
		}
		return &failure{code: "encode_failed", msg: fmt.Sprintf("%s: %v: %s", tool, err, msg),
			extra: map[string]any{"exit_code": ee.ExitCode()}}
	}
	return fail("encode_failed", "%s: %v: %s", tool, err, msg)
}

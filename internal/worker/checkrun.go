package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// measureMono measures a two-channel sound's mid and side (audio.mono "measure") and decides
// the downmix as the pipeline did; a level it cannot read leaves the sound stereo.
func (r *run) measureMono(ctx context.Context, src string, a *AudioParams) (*AudioLevels, error) {
	var lines []string
	var mu sync.Mutex
	err := r.runToolTo(ctx, midSideArgs(r.w.cfg.FFmpeg, src), "ffmpeg", nil, nil, func(l string) {
		mu.Lock()
		lines = append(lines, l)
		mu.Unlock()
	})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	below := 40.0
	if a.MonoSideBelowDB != nil {
		below = *a.MonoSideBelowDB
	}
	lv := &AudioLevels{}
	if err == nil {
		if mid, side, ok := parseMidSide(lines); ok {
			lv.MidDB, lv.SideDB, lv.Mono = dbFigure(mid), dbFigure(side), dualMono(mid, side, below)
		}
	} else {
		r.log("warning", "worker", "the sound's levels could not be measured; it stays stereo: "+err.Error())
	}
	a.Mono = Mono{On: lv.Mono}
	r.log("info", "worker", fmt.Sprintf("mid %v dB, side %v dB: mono %v", lv.MidDB, lv.SideDB, lv.Mono))
	return lv, nil
}

// checkResult is the check after the encode: a full decode of the result, its frames and
// length, the comparison with the source and the packets of a remux, each as asked. result
// and source are paths or URLs ffmpeg reads; w and h are the result's size. sameFrames: the
// result should have the source's frames (a rung, a remux, a check), so source_frames is
// reported, from srcFrames when the encode already counted them, else by decoding the source;
// a cut's source is longer and is not counted. audio says whether framemd5 lists the sound.
// Figures below a floor are no failure here: a tool that cannot run is.
func (r *run) checkResult(ctx context.Context, c *CheckSettings, result, source string, w, h int,
	sameFrames bool, srcFrames *int64, audio bool) (*CheckFigures, error) {
	t0 := time.Now()
	r.setPhase("checking") // v0.2.8: the queue and the Encoder page show the check step
	r.log("info", "worker", "checking the result")
	f := &CheckFigures{}
	ff := r.w.cfg.FFmpeg
	if c.decode() || c.frames() {
		var errs []string
		var mu sync.Mutex
		var frame int64
		var outUS int64
		err := r.runToolTo(ctx, decodeArgs(ff, result, false), "ffmpeg", func(b map[string]string) {
			frame, _ = strconv.ParseInt(b["frame"], 10, 64)
			outUS, _ = strconv.ParseInt(b["out_time_us"], 10, 64)
		}, nil, func(l string) {
			mu.Lock()
			errs = append(errs, l)
			mu.Unlock()
		})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		errs = decodeErrors(errs)
		if err != nil && len(errs) == 0 {
			errs = []string{err.Error()}
		}
		if c.decode() {
			d := &DecodeFigures{Errors: len(errs)}
			if len(errs) > 20 {
				errs = errs[:20]
			}
			d.Lines = errs
			f.Decode = d
		}
		if c.frames() {
			f.Frames = &frame
			if outUS > 0 {
				dur := float64(outUS) / 1e6
				f.DurationS = &dur
			}
			if sameFrames && srcFrames == nil && source != "" && source != result {
				var n int64
				if err := r.runToolTo(ctx, decodeArgs(ff, source, true), "ffmpeg", func(b map[string]string) {
					n, _ = strconv.ParseInt(b["frame"], 10, 64)
				}, nil, nil); err != nil {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
					return nil, fail("check_failed", "the source's frames: %v", err)
				}
				srcFrames = &n
			}
			if sameFrames {
				f.SourceFrames = srcFrames
			}
		}
	}
	if c.Compare != nil && source != "" {
		args, err := compareArgs(ff, *c.Compare, result, source, w, h, r.l.Job.Source.Width, r.l.Job.Source.Height)
		if err != nil {
			return nil, fail("other", "compare: %v", err)
		}
		var text strings.Builder
		var mu sync.Mutex
		var pairs int64
		err = r.runToolTo(ctx, args, "ffmpeg", func(b map[string]string) {
			pairs, _ = strconv.ParseInt(b["frame"], 10, 64)
		}, nil, func(l string) {
			mu.Lock()
			text.WriteString(l + "\n")
			mu.Unlock()
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fail("check_failed", "the comparison: %v", err)
		}
		every := c.Compare.Every
		if every < 1 {
			every = 1
		}
		cf := &CompareFigures{Every: every, Pairs: pairs}
		cf.SSIM, cf.PSNRY, cf.XPSNR, cf.VMAF = parseScores(text.String())
		if c.Compare.VMAF != nil {
			cf.VMAFModel = c.Compare.VMAF.Model
		}
		f.Compare = cf
	}
	if c.Framemd5 && source != "" {
		lists := make([][]packet, 2)
		for i, path := range []string{source, result} {
			var text strings.Builder
			if err := r.runToolTo(ctx, framemd5Args(ff, path, audio), "ffmpeg", nil, func(l string) {
				text.WriteString(l + "\n")
			}, nil); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fail("check_failed", "framemd5: %v", err)
			}
			p, err := parseFramemd5(text.String())
			if err != nil {
				return nil, fail("check_failed", "framemd5: %v", err)
			}
			lists[i] = p
		}
		d := comparePackets(lists[0], lists[1])
		f.Framemd5 = &Framemd5Figures{Match: d == nil, Packets: len(lists[1]), FirstDifference: d}
	}
	f.CheckS = round1(time.Since(t0).Seconds())
	b, _ := json.Marshal(f)
	r.log("info", "worker", "check: "+string(b))
	return f, nil
}

// workCheck is a `check` job (v0.2.8): a file no worker encoded, checked as it streams from
// the queue. Nothing is downloaded and nothing delivered but the figures.
func (r *run) workCheck(ctx context.Context) error {
	l := r.l
	var p CheckParams
	if err := json.Unmarshal(l.Params, &p); err != nil {
		return fail("other", "params: %v", err)
	}
	if p.Check == nil {
		return fail("other", "params: no check")
	}
	name := p.Candidate
	if name == "" {
		name = "candidate"
	}
	refs := map[string]FileRef{"source": l.Job.Source.FileRef}
	cand, candidate := l.Job.Inputs[name], true
	if cand.URL == "" {
		// A decode-only check sends the file as the source.
		cand, candidate = l.Job.Source.FileRef, false
	}
	refs["candidate"] = cand
	px, err := r.w.api.serveLease(refs)
	if err != nil {
		return fail("other", "the proxy for ffmpeg: %v", err)
	}
	defer px.Close()
	source := px.URL("source")
	if !candidate {
		source = ""
	}
	r.mu.Lock()
	r.encodeStart = time.Now()
	r.mu.Unlock()
	audio := p.Audio == nil || *p.Audio
	f, err := r.checkResult(ctx, p.Check, px.URL("candidate"), source, p.Width, p.Height, true, nil, audio)
	if err != nil {
		return err
	}
	r.flushLogs(ctx)
	res, err := r.w.api.PutResult(ctx, l.ID, map[string]any{"outcome": "figures", "check": f,
		"tools": r.w.toolsNamed("ffmpeg")})
	if err != nil {
		return err
	}
	r.log("info", "worker", "delivered the figures; the verdict is "+res.Verdict)
	return nil
}

// leaseProxy serves one lease's files to ffmpeg on 127.0.0.1 (v0.2.8). The tunnel to the
// queue lives inside the worker, so a child process cannot reach the queue's URLs; this
// passes GET and HEAD with their Range through the worker's own client. The path carries a
// random token: another process on the machine cannot guess it.
type leaseProxy struct {
	ln    net.Listener
	srv   *http.Server
	token string
}

func (c *Client) serveLease(refs map[string]FileRef) (*leaseProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		ln.Close()
		return nil, err
	}
	px := &leaseProxy{ln: ln, token: hex.EncodeToString(b)}
	mux := http.NewServeMux()
	for name, ref := range refs {
		target := c.url(ref.URL)
		mux.HandleFunc("/"+px.token+"/"+name, func(w http.ResponseWriter, q *http.Request) {
			if q.Method != http.MethodGet && q.Method != http.MethodHead {
				http.Error(w, "GET or HEAD", http.StatusMethodNotAllowed)
				return
			}
			out, err := http.NewRequestWithContext(q.Context(), q.Method, target, nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			if rg := q.Header.Get("Range"); rg != "" {
				out.Header.Set("Range", rg)
			}
			resp, err := c.HTTP.Do(out)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			for _, k := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "Content-Type"} {
				if v := resp.Header.Get(k); v != "" {
					w.Header().Set(k, v)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
		})
	}
	px.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go px.srv.Serve(ln)
	return px, nil
}

func (p *leaseProxy) URL(name string) string {
	return "http://" + p.ln.Addr().String() + "/" + p.token + "/" + name
}

func (p *leaseProxy) Close() { p.srv.Close() }

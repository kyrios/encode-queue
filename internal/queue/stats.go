package queue

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	transferWeight = 0.25    // the newest sample's weight in a transfer's moving average
	minTransfer    = 1 << 20 // a request that moves less is no sample
	recentTransfer = 60 * time.Second
	speedResults   = 5 // a class's speed is the median of this many results
	keepClasses    = 16
	tunnelUp       = 180 * time.Second
)

// TransferStats is how fast a worker moved files one way.
type TransferStats struct {
	LastBps float64    `json:"last_bps,omitempty"`
	AvgBps  float64    `json:"avg_bps,omitempty"`
	LastAt  *time.Time `json:"last_at,omitempty"`
	Samples int64      `json:"samples,omitempty"`

	live   float64 // from an attempt's progress, while it transfers
	liveAt time.Time
}

func (t *TransferStats) add(bps float64, at time.Time) {
	if t.Samples == 0 {
		t.AvgBps = bps
	} else {
		t.AvgBps += transferWeight * (bps - t.AvgBps)
	}
	t.LastBps = bps
	t.LastAt = &at
	t.Samples++
}

// current is the rate now: the live one while it is fresh, the latest sample for a minute.
func (t *TransferStats) current(now time.Time, fresh time.Duration) float64 {
	if !t.liveAt.IsZero() && now.Sub(t.liveAt) <= fresh {
		return t.live
	}
	if t.LastAt != nil && now.Sub(*t.LastAt) <= recentTransfer {
		return t.LastBps
	}
	return 0
}

// Transfer directions.
const (
	DirDownload = "download"
	DirUpload   = "upload"
)

// NoteTransfer records one request that moved bytes in d for a worker.
func (s *Store) NoteTransfer(workerID, dir string, bytes int64, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noteTransfer(workerID, dir, bytes, d)
}

func (s *Store) noteTransfer(workerID, dir string, bytes int64, d time.Duration) {
	w := s.st.Workers[workerID]
	if w == nil || bytes < minTransfer || d <= 0 {
		return
	}
	bps := float64(bytes) / d.Seconds()
	if dir == DirUpload {
		w.Upload.add(bps, s.cfg.Now())
	} else {
		w.Download.add(bps, s.cfg.Now())
	}
	s.touch(false)
}

// noteLive takes the rate between two progress reports of one transfer phase.
func (s *Store) noteLive(w *Worker, prev *Progress, prevAt time.Time, p Progress) {
	if prev == nil || prev.Phase != p.Phase || prev.BytesDone == nil || p.BytesDone == nil {
		return
	}
	now := s.cfg.Now()
	dt := now.Sub(prevAt).Seconds()
	moved := *p.BytesDone - *prev.BytesDone
	if dt <= 0 || moved < 0 {
		return
	}
	t := &w.Download
	switch p.Phase {
	case "uploading":
		t = &w.Upload
	case "downloading":
	default:
		return
	}
	t.live, t.liveAt = float64(moved)/dt, now
}

// SpeedHist is a worker's last results on one rung class, as fps.
type SpeedHist struct {
	Class     string    `json:"class"`
	Codec     string    `json:"codec"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	Preset    string    `json:"preset"`
	FPS       []float64 `json:"fps"` // oldest first
	UpdatedAt time.Time `json:"updated_at"`
}

func (h *SpeedHist) median() float64 { return median(h.FPS) }

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	if n := len(c); n%2 == 1 {
		return c[n/2]
	} else {
		return (c[n/2-1] + c[n/2]) / 2
	}
}

// rungClass is what makes encodes comparable: codec, output size and preset.
func rungClass(p *RungParams) string {
	return fmt.Sprintf("%s %dx%d %s", p.Codec, p.Width, p.Height, p.Preset)
}

func (w *Worker) speed(class string) *SpeedHist {
	for _, h := range w.Speeds {
		if h.Class == class {
			return h
		}
	}
	return nil
}

// noteResult takes a delivered rung result's fps as a sample of the worker's speed on its
// class: its frames over its encode time, or the attempt's last reported fps. Both are the
// worker's word, so the sample is never faster than the frames over the time it held the lease,
// which the queue saw itself (v0.2.9).
func (s *Store) noteResult(w *Worker, j *Job, l *Lease, frames int64, encodeS float64) {
	if j == nil || j.rung == nil {
		return
	}
	fps := 0.0
	if frames > 0 && encodeS > 0 {
		fps = float64(frames) / encodeS
	} else if p := l.Progress; p != nil && p.FPS != nil {
		fps = *p.FPS
	}
	if fps <= 0 || math.IsInf(fps, 0) {
		return
	}
	if frames <= 0 {
		frames = j.Spec.Source.Frames
	}
	if held := s.cfg.Now().Sub(l.CreatedAt).Seconds(); frames > 0 && held > 0 {
		fps = math.Min(fps, float64(frames)/held)
	}
	class := rungClass(j.rung)
	h := w.speed(class)
	if h == nil {
		r := j.rung
		h = &SpeedHist{Class: class, Codec: r.Codec, Width: r.Width, Height: r.Height, Preset: r.Preset}
	}
	h.FPS = append(h.FPS, fps)
	if len(h.FPS) > speedResults {
		h.FPS = h.FPS[len(h.FPS)-speedResults:]
	}
	h.UpdatedAt = s.cfg.Now()
	list := []*SpeedHist{h}
	for _, o := range w.Speeds {
		if o != h && len(list) < keepClasses {
			list = append(list, o)
		}
	}
	w.Speeds = list
}

func (s *Store) referenceWorker() *Worker {
	if s.cfg.ReferenceWorker == "" {
		return nil
	}
	for _, w := range s.st.Workers {
		if w.Name == s.cfg.ReferenceWorker {
			return w
		}
	}
	return nil
}

// classRelative is the worker's median on a class over the reference's, or nil.
func (s *Store) classRelative(w *Worker, class string) *float64 {
	ref := s.referenceWorker()
	h := w.speed(class)
	if ref == nil || h == nil {
		return nil
	}
	rh := ref.speed(class)
	if rh == nil || rh.median() <= 0 {
		return nil
	}
	r := h.median() / rh.median()
	return &r
}

// relativeSpeed is the median of the worker's class ratios against the reference.
func (s *Store) relativeSpeed(w *Worker) *float64 {
	r, _ := s.relativeSpeedSource(w)
	return r
}

// Where a relative speed comes from (v0.2.5).
const (
	SpeedMeasured   = "measured"
	SpeedCalibrated = "calibrated"
)

// relativeSpeedSource is the median of the measured class ratios when there are any, else of
// the calibrated ones, and which it is; nil and "" without either.
func (s *Store) relativeSpeedSource(w *Worker) (*float64, string) {
	var rs []float64
	for _, h := range w.Speeds {
		if r := s.classRelative(w, h.Class); r != nil {
			rs = append(rs, *r)
		}
	}
	if len(rs) > 0 {
		m := median(rs)
		return &m, SpeedMeasured
	}
	for _, c := range calibratedClasses(w) {
		if r := s.calibratedRelative(w, c); r != nil {
			rs = append(rs, *r)
		}
	}
	if len(rs) == 0 {
		return nil, ""
	}
	m := median(rs)
	return &m, SpeedCalibrated
}

// fpsOn is the worker's expected fps on a class: its own median, or the reference's times
// its relative speed.
func (s *Store) fpsOn(w *Worker, class string) float64 {
	if h := w.speed(class); h != nil {
		return h.median()
	}
	if ref := s.referenceWorker(); ref != nil && s.calibratedRelative(w, class) != nil {
		f, _ := calibratedFPS(w, class) // its own fps on the class, on the reference's clip
		if rf, _ := calibratedFPS(ref, class); f > rf*trustCalibrated {
			f = rf * trustCalibrated // no faster than that until measured
		}
		return f
	}
	ref := s.referenceWorker()
	if ref == nil || ref == w {
		return 0
	}
	rh := ref.speed(class)
	r := s.relativeSpeed(w)
	if rh == nil || r == nil {
		return 0
	}
	return rh.median() * *r
}

// encodeSeconds is how long the worker would take for the job from the start, or 0 when
// unknown.
func (s *Store) encodeSeconds(w *Worker, j *Job) float64 {
	if j.rung == nil || j.Spec.Source.Frames <= 0 {
		return 0
	}
	fps := s.fpsOn(w, rungClass(j.rung))
	if fps <= 0 {
		return 0
	}
	return float64(j.Spec.Source.Frames) / fps
}

// eta is the seconds until an attempt finishes encoding, from frames left and its fps.
func eta(j *Job, p *Progress) *int64 {
	if p == nil || p.Phase != "encoding" || p.FPS == nil || *p.FPS <= 0 || p.FramesDone == nil {
		return nil
	}
	total := j.Spec.Source.Frames
	if p.FramesTotal != nil && *p.FramesTotal > 0 {
		total = *p.FramesTotal
	}
	if total <= 0 {
		return nil
	}
	left := total - *p.FramesDone
	if left < 0 {
		left = 0
	}
	n := int64(math.Ceil(float64(left) / *p.FPS))
	return &n
}

// freeAt is when the worker has a free slot: now, or when its earliest attempt finishes
// encoding; nil when unknown, offline or paused. except is a job whose attempt does not count.
func (s *Store) freeAt(w *Worker, now time.Time, except string) *time.Time {
	if !s.connected(w, now) || s.paused(w, now) {
		return nil
	}
	max := s.maxJobs(w)
	var ends []time.Time
	busy := 0
	for _, l := range s.activeLeases(w.ID) {
		if l.JobID == except || l.Extra {
			continue // a Sofort job in the extra slot holds no regular slot
		}
		busy++
		if j := s.st.Jobs[l.JobID]; j != nil {
			if e := eta(j, l.Progress); e != nil {
				ends = append(ends, now.Add(time.Duration(*e)*time.Second))
			}
		}
	}
	if busy < max {
		return &now
	}
	if len(ends) == 0 {
		return nil
	}
	sort.Slice(ends, func(a, b int) bool { return ends[a].Before(ends[b]) })
	return &ends[0]
}

// forecast is when each unfinished job should finish: an active attempt at now plus its eta;
// the queued jobs in order, each to the worker slot that finishes it first, a slot starting
// when its attempt finishes. Jobs it cannot estimate are missing.
func (s *Store) forecast(now time.Time) map[string]time.Time {
	out := map[string]time.Time{}
	type slot struct {
		w    *Worker
		free time.Time
	}
	var slots []*slot
	var ws []*Worker
	for _, w := range s.st.Workers {
		ws = append(ws, w)
	}
	sort.Slice(ws, func(a, b int) bool { return ws[a].Name < ws[b].Name })
	for _, w := range ws {
		usable := s.connected(w, now) && !s.paused(w, now) && w.Caps != nil
		max := s.maxJobs(w)
		active := s.activeLeases(w.ID)
		for _, l := range active {
			j := s.st.Jobs[l.JobID]
			if j == nil {
				continue
			}
			if e := eta(j, l.Progress); e != nil {
				end := now.Add(time.Duration(*e) * time.Second)
				out[j.ID] = end
				if usable && !l.Extra {
					slots = append(slots, &slot{w, end})
				}
			}
		}
		regular := 0
		for _, l := range active {
			if !l.Extra {
				regular++
			}
		}
		if usable {
			for i := regular; i < max; i++ {
				slots = append(slots, &slot{w, now})
			}
		}
	}
	if s.draining {
		return out
	}
	for _, j := range s.queued() {
		var best *slot
		var bestEnd time.Time
		for _, sl := range slots {
			if j.Pin != nil && j.Pin.WorkerID != sl.w.ID {
				continue
			}
			if acc, _ := s.kinds(sl.w); !contains(acc, j.Spec.Kind) || s.mismatch(sl.w, j) != "" {
				continue
			}
			d := s.encodeSeconds(sl.w, j)
			if d <= 0 {
				continue
			}
			end := sl.free.Add(time.Duration(d * float64(time.Second)))
			if best == nil || end.Before(bestEnd) {
				best, bestEnd = sl, end
			}
		}
		if best != nil {
			out[j.ID] = bestEnd
			best.free = bestEnd
		}
	}
	return out
}

// watchTunnels notes when each worker's tunnel came up: the handshake that made it up after
// it was down or before any. A renewal while it stays up leaves the time as it is.
func (s *Store) watchTunnels(now time.Time) {
	peers := s.peerStatus()
	if peers == nil {
		return
	}
	for _, w := range s.st.Workers {
		p, ok := peers[w.PublicKey]
		up := ok && !p.LastHandshake.IsZero() && now.Sub(p.LastHandshake) <= tunnelUp
		switch {
		case !up:
			w.tunnelSince = nil
		case w.tunnelSince == nil:
			t := p.LastHandshake
			w.tunnelSince = &t
		}
	}
}

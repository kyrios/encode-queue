package queue

import (
	"math"
	"sort"
	"time"
	_ "time/tzdata" // Europe/Berlin in the distroless image, which has no zoneinfo
)

// **Points** (v0.2.15; Thorsten, 2026-10-28). Every job the pipeline accepts earns its worker
// points from a frozen table, "Punkte-Tabelle v1": one point is one minute of the Mac Studio's
// work, by its calibration of 2026-10-28. A job earns the same on every machine, linear in its
// frames or its seconds of audio, so a fast machine earns more by doing more, not by doing the
// same faster. Moving bytes through the tunnel earns nothing. A failed, rejected, given back,
// expired, preempted or moved attempt earns nothing, and neither does a result that arrives after
// its lease ended. The queue prices a job from the pipeline's own figures (the source's frames
// and length) and the job's settings, never from what the worker reports. Points are counted and
// shown; the scoring (ect.go) never reads them. Every worker starts at zero.

// PointsTable names the table the points come from.
const PointsTable = "v1"

// pointsRungFPS is the Mac Studio's calibrated fps per rung class: Slipstream 1.3.3 with worker
// 0.2.12 on toolchain 3 (ffmpeg 9.0.2, libx265 4.3+1-e9b8812, libx264 core 165 r3222 b35605a),
// the calibration clip 01777abf…, the classes measured 2026-10-27 19:09 UTC and sent again with
// its whisper timings on 2026-10-28 (its worker's calibration.json).
var pointsRungFPS = map[string]float64{
	"libx265 3840x2160 ultrafast": 48.42392630208087,
	"libx265 1920x1080 veryfast":  99.6317940996059,
	"libx265 1280x720 veryfast":   131.05255010785342,
	"libx264 1280x720 veryfast":   271.00387277325575,
	"libx265 3840x2160 slower":    2.2835275723167556,
	"libx265 1920x1080 slow":      29.169180609459037,
	"libx265 1280x720 slow":       57.46996902278873,
	"libx264 1280x720 slow":       188.31159354449025,
}

// pointsWhisperSpeed is its whisper.cpp v1.9.5 speed per model, seconds of audio per second (GPU,
// 4 threads, the calibration speech, 2026-10-28). It holds no medium: medium counts 2.5 times
// small's time, the catalogue's Cost, as whisperSpeed guesses a model not yet timed.
var pointsWhisperSpeed = map[string]float64{
	SHASmall:        32.336355149617205,
	SHALargeV3Turbo: 12.761687001891676,
	SHAMedium:       32.336355149617205 / 2.5,
}

// rungPointsPerFrame is what one frame of a rung earns: 1 / (fps × 60) with its class's fps from
// the table. A size the table lacks, of a codec and preset it has, takes the nearest size's fps
// scaled by the pixels (a 3840x1920 master's top rung against 3840x2160). 0 for a codec and preset
// the table lacks.
func rungPointsPerFrame(p *RungParams) float64 {
	if fps, ok := pointsRungFPS[rungClass(p)]; ok {
		return 1 / (fps * 60)
	}
	px := float64(p.Width) * float64(p.Height)
	if px <= 0 {
		return 0
	}
	best, dist := 0.0, math.Inf(1)
	for c, fps := range pointsRungFPS {
		codec, w, h, preset := parseClass(c)
		if codec != p.Codec || preset != p.Preset {
			continue
		}
		cpx := float64(w) * float64(h)
		if d := math.Abs(math.Log(px / cpx)); d < dist || (d == dist && fps*cpx/px < best) {
			best, dist = fps*cpx/px, d
		}
	}
	if best <= 0 {
		return 0
	}
	return 1 / (best * 60)
}

// jobPoints is what j earns for its attempt l, by the table; 0 for a kind the table does not price
// (cut, remux, check) or a job without the figures.
func jobPoints(j *Job, l *Lease) float64 {
	switch {
	case j.rung != nil:
		return float64(j.outFrames()) * rungPointsPerFrame(j.rung)
	case j.whisper != nil:
		m := l.Model
		if m == "" {
			m = j.whisper.acceptable()[0]
		}
		if v := pointsWhisperSpeed[m]; v > 0 && j.Spec.Source.DurationS > 0 {
			return j.Spec.Source.DurationS / (v * 60)
		}
	}
	return 0
}

// PointsLedger is a worker's points: in all and by day (Europe/Berlin), the days of the last two
// weeks.
type PointsLedger struct {
	Total float64            `json:"total"`
	Days  map[string]float64 `json:"days,omitempty"`
}

// berlin is the clock the days and weeks follow: Funkhaus's.
var berlin = func() *time.Location {
	l, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		return time.UTC
	}
	return l
}()

const dayLayout = "2006-01-02"

// award gives the worker of l its points for j, once, when the pipeline accepts l's result. Only a
// result its worker delivered while it held the lease earns.
func (s *Store) award(w *Worker, j *Job, l *Lease) {
	if w == nil || l.EndCode != "" || l.Points > 0 {
		return
	}
	p := jobPoints(j, l)
	if p <= 0 {
		return
	}
	l.Points = p
	if w.Points == nil {
		w.Points = &PointsLedger{}
	}
	if w.Points.Days == nil {
		w.Points.Days = map[string]float64{}
	}
	now := s.cfg.Now().In(berlin)
	w.Points.Total += p
	w.Points.Days[now.Format(dayLayout)] += p
	cut := now.AddDate(0, 0, -14).Format(dayLayout)
	for d := range w.Points.Days {
		if d < cut {
			delete(w.Points.Days, d)
		}
	}
}

// PointsView is a worker's points today, this week (from Monday, Europe/Berlin) and in all.
type PointsView struct {
	Table string  `json:"table"`
	Today float64 `json:"today"`
	Week  float64 `json:"week"`
	Total float64 `json:"total"`
}

func (s *Store) pointsView(w *Worker) PointsView {
	v := PointsView{Table: PointsTable}
	if w.Points == nil {
		return v
	}
	now := s.cfg.Now().In(berlin)
	today := now.Format(dayLayout)
	monday := now.AddDate(0, 0, -((int(now.Weekday()) + 6) % 7)).Format(dayLayout)
	for d, p := range w.Points.Days {
		if d == today {
			v.Today += p
		}
		if d >= monday && d <= today {
			v.Week += p
		}
	}
	v.Total = w.Points.Total
	return v
}

// PointsRank is a worker's place among every paired worker by today's points: 1 is the most,
// workers with equal points share a place.
type PointsRank struct {
	PointsView
	Rank int `json:"rank"`
	Of   int `json:"of"`
}

func (s *Store) pointsRank(w *Worker) PointsRank {
	r := PointsRank{PointsView: s.pointsView(w), Of: len(s.st.Workers)}
	var today []float64
	for _, v := range s.st.Workers {
		today = append(today, s.pointsView(v).Today)
	}
	sort.Float64s(today)
	r.Rank = 1
	for _, t := range today {
		if t > r.Today {
			r.Rank++
		}
	}
	return r
}

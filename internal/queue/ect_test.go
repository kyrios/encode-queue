package queue

import (
	"math"
	"strconv"
	"testing"
	"time"
)

// Thorsten's example (2026-10-27): the Mac Studio encodes three times as fast as encoder, but it
// reaches the queue at 300 Mbit/s, while encoder reads its sources on Funkhaus's own host.

// scored sets up encoder and the Mac: calibrated on 4K slower at 12.5/3 and 12.5 fps, with their
// download rates measured.
func scored(t *testing.T) (*env, string, string) {
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.calibrated(enc, clipA, map[string]float64{k4slower: 12.5 / 3})
	e.calibrated(mac, clipA, map[string]float64{k4slower: 12.5})
	e.s.st.Workers[enc].Download = TransferStats{AvgBps: 1e9, Samples: 5}
	e.s.st.Workers[mac].Download = TransferStats{AvgBps: 300e6 / 8, Samples: 5}
	e.presence(enc, mac)
	return e, enc, mac
}

// sized gives a queued rung job its size and frames: 2 minutes of 4K at 25 fps.
func (e *env) sized(id string, bytes int64) {
	j := e.s.st.Jobs[id]
	j.Spec.Source.Bytes = bytes
	j.Spec.Source.Frames = 3000
}

func TestAnUncompressedSourceGoesToTheMachineBesideIt(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("raw.2160-archive", "archive")
	e.sized("raw.2160-archive", 50e9)
	// The Mac asks first: 22 min to fetch + 4 min to encode, against encoder's 1 + 12.
	if l := e.claim(mac); l != nil {
		t.Fatalf("the Mac took the 50 GB source: %+v", l)
	}
	l := e.claim(enc)
	if l == nil || l.Job.ID != "raw.2160-archive" {
		t.Fatalf("encoder: %+v", l)
	}
	ls := e.s.st.Leases[l.ID]
	if ls.ECT == nil || ls.RunnerUp == nil || ls.RunnerUp.Worker != "mac" {
		t.Fatalf("the lease's scoring: %+v %+v", ls.ECT, ls.RunnerUp)
	}
	if got := ls.ECT.S / 60; math.Abs(got-12.8) > 0.2 {
		t.Fatalf("encoder's ECT %.1f min, want about 13", got)
	}
	if got := ls.RunnerUp.S / 60; math.Abs(got-26.2) > 0.2 {
		t.Fatalf("the Mac's ECT %.1f min, want about 26", got)
	}
	if v := must(e.s.GetJob("raw.2160-archive")); v.Attempts[0].ECTS == nil || v.Attempts[0].RunnerUp == nil {
		t.Fatalf("the attempt does not show the scoring: %+v", v.Attempts[0])
	}
}

func TestACompressedSourceGoesToTheFasterMac(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("h264.2160-archive", "archive")
	e.sized("h264.2160-archive", 400e6)
	if l := e.claim(enc); l != nil {
		t.Fatalf("encoder took a job the Mac finishes in a third of the time: %+v", l)
	}
	if l := e.claim(mac); l == nil {
		t.Fatal("the Mac did not take it")
	}
}

func TestNoJobWaitsForeverForTheIdealWorker(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("raw.2160-stage1", "stage1")
	e.sized("raw.2160-stage1", 50e9)
	if l := e.claim(mac); l != nil {
		t.Fatal("the Mac took it at once")
	}
	// encoder does not come for it; after stage 1's two minutes the Mac takes it.
	e.clock.Add(2*time.Minute + time.Second)
	e.presence(enc, mac)
	if l := e.claim(mac); l == nil {
		t.Fatal("the job still waits for encoder")
	}
}

func TestOtherProgramsSlowAWorker(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("h264.2160-archive", "archive")
	e.sized("h264.2160-archive", 400e6)
	// Final Cut keeps 10 of the Mac's 12 threads busy: its encode takes six times as long.
	e.machineThreads(mac, 12)
	load := 10.0
	e.s.st.Workers[mac].OtherLoad = &load
	if l := e.claim(enc); l == nil {
		t.Fatal("encoder left a job to a Mac busy with other work")
	}
}

func TestTheCorrectionLearnsFromResults(t *testing.T) {
	e, enc, _ := scored(t)
	e.s.st.Workers[e.workerID("mac")].LastSeen = nil // encoder alone
	e.rung("a.2160-archive", "archive")
	e.sized("a.2160-archive", 1e6)
	l := e.claim(enc)
	est := e.s.st.Leases[l.ID].EstRunS
	if est <= 0 {
		t.Fatal("no estimate on the lease")
	}
	e.result(enc, l.ID, 3000, est*2) // took twice as long as calibrated
	if c := e.s.st.Workers[enc].Corrections[KindRung]; math.Abs(c-1.3) > 1e-9 {
		t.Fatalf("correction %v, want 0.7 + 0.3 × 2", c)
	}
}

func (e *env) machineThreads(wid string, n int) {
	e.s.st.Workers[wid].Caps.Hardware = []byte(`{"cpu_threads":` + strconv.Itoa(n) + `}`)
}

func (e *env) workerID(name string) string {
	for id, w := range e.s.st.Workers {
		if w.Name == name {
			return id
		}
	}
	return ""
}

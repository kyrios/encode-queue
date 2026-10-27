package queue

import (
	"testing"
	"time"
)

// Thorsten's live session, 2026-10-27, on v0.2.7: "Why are not multiple encodes running?" The
// archive job iphoneblog_one_144_hey_siri_podcast.720-s2-b09044aa sat queued while encoder was
// free. The Mac Studio's worker had died inside Slipstream; the queue still counted the Mac as
// connected, scored it the better worker, and encoder left the job to it for archive's hour.
// Then a stage-1 job went to encoder with ect_s and runner_up null, and nobody could say why.

func TestAFreeWorkerTakesTheJobWhenTheBetterOneAsksForNothing(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("h264.2160-archive", "archive")
	e.sized("h264.2160-archive", 400e6)
	// The Mac asks for work: it is the better worker, and encoder leaves the job to it.
	if l := e.claim(enc); l != nil {
		t.Fatalf("encoder took a job the asking Mac finishes in a third of the time: %+v", l)
	}
	// The Mac's worker dies; Slipstream keeps the Mac present. A minute later it has asked for
	// nothing for two presence intervals.
	e.clock.Add(61 * time.Second)
	e.silent(mac)
	e.presence(enc)
	l := e.claim(enc)
	if l == nil || l.Job.ID != "h264.2160-archive" {
		t.Fatalf("encoder left the job to a Mac that asks for nothing: %+v", l)
	}
	ls := e.s.st.Leases[l.ID]
	if ls.RunnerUp != nil || ls.Scoring != ScoringNotClaiming {
		t.Fatalf("scoring %q, runner-up %+v", ls.Scoring, ls.RunnerUp)
	}
	if v := must(e.s.GetJob("h264.2160-archive")); v.Attempts[0].Scoring == nil || *v.Attempts[0].Scoring != "not_claiming" {
		t.Fatalf("the attempt does not say why: %+v", v.Attempts[0])
	}
}

func TestTheMoveMenuSaysWhichWorkerAsksForNothing(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("h264.2160-archive", "archive")
	e.clock.Add(61 * time.Second)
	e.silent(mac)
	e.presence(enc)
	c := must(e.s.JobCandidates("h264.2160-archive"))
	got := map[string]bool{}
	for _, it := range c.Items {
		got[it.Worker.Name] = it.Claiming
	}
	if !got["encoder"] || got["mac"] {
		t.Fatalf("claiming: %v", got)
	}
}

func TestAWorkerAtWorkCountsWithoutAsking(t *testing.T) {
	e, enc, mac := scored(t)
	e.rung("a.2160-archive", "archive")
	e.sized("a.2160-archive", 400e6)
	if l := e.claim(mac); l == nil {
		t.Fatal("the Mac took nothing")
	}
	// At its limit it asks for nothing, renews its lease, and is still a competitor.
	e.clock.Add(61 * time.Second)
	e.silent(mac)
	e.presence(enc)
	if !e.s.claiming(e.s.st.Workers[mac], e.clock.Now()) {
		t.Fatal("a working Mac counts as not claiming")
	}
	_ = enc
}

func TestTheLeaseSaysWhyThereWasNoComparison(t *testing.T) {
	// encoder alone, its speed on the class unknown (no calibration, no result): the stage-1
	// job of the live session, go3x1fpzj30k.1080-s1-79a17e6f.
	e := newEnv(t, "encoder")
	enc := e.worker("encoder", reference)
	e.rung("go3x1fpzj30k.2160-stage1", "stage1")
	l := e.claim(enc)
	if l == nil || e.s.st.Leases[l.ID].ECT != nil || e.s.st.Leases[l.ID].Scoring != ScoringOnlyWorker {
		t.Fatalf("alone: %+v", e.s.st.Leases[l.ID])
	}

	// The Mac asks too, but neither's speed on the class is known.
	e = newEnv(t, "encoder")
	enc = e.worker("encoder", reference)
	mac := e.worker("mac", reference)
	e.rung("b.2160-stage1", "stage1")
	if l := e.claim(enc); l == nil || e.s.st.Leases[l.ID].Scoring != ScoringNoSpeed {
		t.Fatalf("no speed: %+v", l)
	}

	// Sent to the Mac on the Encoder page.
	e.rung("c.2160-stage1", "stage1")
	if _, err := e.s.AssignJob("c.2160-stage1", Assign{WorkerID: mac}); err != nil {
		t.Fatal(err)
	}
	if l := e.claim(mac); l == nil || e.s.st.Leases[l.ID].Scoring != ScoringAssigned {
		t.Fatalf("assigned: %+v", l)
	}
}

func TestTheScoringSaysScoredOrWaited(t *testing.T) {
	e, enc, _ := scored(t)
	e.rung("raw.2160-archive", "archive")
	e.sized("raw.2160-archive", 50e9)
	if l := e.claim(enc); l == nil || e.s.st.Leases[l.ID].Scoring != ScoringScored {
		t.Fatalf("scored: %+v", l)
	}
	// The Mac is the worse worker for an uncompressed stage-1 source while encoder is free and
	// asks; it takes the job after stage 1's wait.
	e, enc, mac := scored(t)
	e.rung("raw2.2160-stage1", "stage1")
	e.sized("raw2.2160-stage1", 50e9)
	e.clock.Add(maxWait["stage1"] + time.Second)
	e.presence(enc, mac)
	if l := e.claim(mac); l == nil || e.s.st.Leases[l.ID].Scoring != ScoringWaited {
		t.Fatalf("waited: %+v", l)
	}
}

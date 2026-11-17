package queue

import (
	"encoding/json"
	"testing"
)

// ADR-0008, package 3: every job carries its tenant. A job from a pipeline before package 3
// names none; it still runs to its verdict, and it shows the queue's default tenant.
func TestAnOldJobWithoutTenantStillRuns(t *testing.T) {
	e := newEnv(t, "")
	e.s.cfg.DefaultTenant = LegacyTenant
	w := e.worker("encoder", reference)
	v := e.rung("old.rung", "stage1")
	if v.Tenant != LegacyTenant {
		t.Fatalf("an old job's tenant: %q", v.Tenant)
	}
	// The state file of a queue before package 3 has no tenant either.
	e.reopen()
	e.s.cfg.DefaultTenant = LegacyTenant
	l := e.claim(w)
	if l == nil || l.Job.ID != "old.rung" {
		t.Fatalf("claim: %+v", l)
	}
	if err := e.deliver(w, l.ID, "encoded"); err != nil {
		t.Fatal(err)
	}
	got, err := e.s.PutVerdict("old.rung", 1, Verdict{Verdict: VerdictAccepted})
	if err != nil || got.State != "done" || got.Tenant != LegacyTenant {
		t.Fatalf("verdict: %s %q %v", got.State, got.Tenant, err)
	}
}

// A job that names its tenant keeps it through a restart, the list filters by it, and putting
// the same job again without a tenant is not the same job.
func TestAJobKeepsItsTenant(t *testing.T) {
	e := newEnv(t, "")
	e.s.cfg.DefaultTenant = LegacyTenant
	f := e.source("new/master.mov", "master of new")
	params := `{"rung":"2160","codec":"libx265","preset":"slower","crf":29,"width":3840,"height":2160,"vbv_maxrate_kbps":5000,"audio":{"mode":"copy"}}`
	spec := JobSubmit{Kind: KindRung, Priority: "stage1", Tenant: "other-site", Params: json.RawMessage(params),
		Source: SourceIn{FileIn: f, Width: 3840, Height: 2160}, Result: ResultSpec{Name: "2160.mp4", MaxBytes: 1 << 30}}
	if v, created, err := e.s.Submit("new.rung", spec); err != nil || !created || v.Tenant != "other-site" {
		t.Fatalf("submit: %+v %v %v", v.Tenant, created, err)
	}
	e.rung("old.rung", "stage1")
	e.reopen()
	if v := must(e.s.GetJob("new.rung")); v.Tenant != "other-site" {
		t.Fatalf("after a restart: %q", v.Tenant)
	}
	for tenant, want := range map[string]string{"other-site": "new.rung", LegacyTenant: "old.rung"} {
		list := e.s.ListJobs(JobFilter{Tenant: tenant})
		if len(list.Items) != 1 || list.Items[0].ID != want {
			t.Fatalf("filter %s: %+v", tenant, list.Items)
		}
	}
	// The same spec again is the same job.
	if _, created, err := e.s.Submit("new.rung", spec); err != nil || created {
		t.Fatalf("again: %v %v", created, err)
	}
}

func TestATenantOutsideTheSlugIsRefused(t *testing.T) {
	e := newEnv(t, "")
	f := e.source("x/master.mov", "master of x")
	params := `{"rung":"2160","codec":"libx265","preset":"slower","crf":29,"width":3840,"height":2160,"vbv_maxrate_kbps":5000,"audio":{"mode":"copy"}}`
	for _, bad := range []string{"A", "x", "iphoneblog/one", "a_b", "abcdefghijklmnopqrstuvwxy"} {
		_, _, err := e.s.Submit("x.rung", JobSubmit{Kind: KindRung, Priority: "stage1", Tenant: bad, Params: json.RawMessage(params),
			Source: SourceIn{FileIn: f, Width: 3840, Height: 2160}, Result: ResultSpec{Name: "2160.mp4", MaxBytes: 1 << 30}})
		if code(err) != "invalid" {
			t.Fatalf("tenant %q: %v", bad, err)
		}
	}
}

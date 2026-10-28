package queue

import (
	"encoding/base64"
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/kyrios/encode-queue/internal/aligner"
)

// WorkerCreate adds a worker.
type WorkerCreate struct {
	Name      string `json:"name"`
	Owner     string `json:"owner,omitempty"`
	PublicKey string `json:"public_key"`
}

// TunnelConfig is a worker's side of the tunnel.
type TunnelConfig struct {
	Address         string `json:"address"`
	ServerPublicKey string `json:"server_public_key"`
	Endpoint        string `json:"endpoint"`
	AllowedIPs      string `json:"allowed_ips"`
	Keepalive       int    `json:"persistent_keepalive_s"`
	API             string `json:"api"`
}

// Line is the configuration as the one line a worker reads (internal/tunnel parses it).
func (t TunnelConfig) Line() string {
	return fmt.Sprintf("eqtunnel1 address=%s server_public_key=%s endpoint=%s allowed_ips=%s keepalive=%d api=%s",
		t.Address, t.ServerPublicKey, t.Endpoint, t.AllowedIPs, t.Keepalive, t.API)
}

// Enrolment is the answer to adding a worker.
type Enrolment struct {
	Worker     WorkerView   `json:"worker"`
	Tunnel     TunnelConfig `json:"tunnel"`
	ConfigLine string       `json:"config_line"`
}

func (s *Store) tunnelFor(w *Worker) TunnelConfig {
	return TunnelConfig{
		Address:         w.Address + "/32",
		ServerPublicKey: s.cfg.ServerPublicKey,
		Endpoint:        s.cfg.Endpoint,
		AllowedIPs:      s.cfg.TunnelAddress.String() + "/32",
		Keepalive:       s.cfg.Keepalive,
		API:             s.cfg.API,
	}
}

func (s *Store) enrolment(w *Worker) Enrolment {
	t := s.tunnelFor(w)
	return Enrolment{Worker: s.workerView(w), Tunnel: t, ConfigLine: t.Line()}
}

// nextAddress is the first address of the subnet that is neither the network's, the
// broadcast's, the queue's nor a worker's.
func (s *Store) nextAddress() (netip.Addr, error) {
	used := map[netip.Addr]bool{s.cfg.TunnelAddress: true}
	for _, w := range s.st.Workers {
		if a, err := netip.ParseAddr(w.Address); err == nil {
			used[a] = true
		}
	}
	p := s.cfg.Subnet.Masked()
	a := p.Addr().Next()
	for p.Contains(a) {
		n := a.Next()
		if !p.Contains(n) && a.Is4() { // the last address of an IPv4 subnet is its broadcast
			break
		}
		if !used[a] {
			return a, nil
		}
		a = n
	}
	return netip.Addr{}, errf(409, "subnet_full", "no free address left in %s", p)
}

// AddWorker pairs a worker by its public key.
func (s *Store) AddWorker(c WorkerCreate) (Enrolment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" || len(c.Name) > 64 {
		return Enrolment{}, errf(422, "invalid", "name must have 1 to 64 characters")
	}
	// The key is stored in one spelling, so two spellings of the same 32 bytes cannot pair
	// two workers onto one peer.
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(c.PublicKey))
	if err != nil || len(b) != 32 {
		return Enrolment{}, errf(422, "invalid", "public_key is not a WireGuard key (32 bytes in base64)")
	}
	c.PublicKey = base64.StdEncoding.EncodeToString(b)
	for _, w := range s.st.Workers {
		if w.PublicKey == c.PublicKey {
			return Enrolment{}, errf(409, "key_exists", "this key belongs to %s already", w.Name)
		}
		if w.Name == c.Name {
			return Enrolment{}, errf(409, "name_exists", "a worker is called %s already", c.Name)
		}
	}
	addr, err := s.nextAddress()
	if err != nil {
		return Enrolment{}, err
	}
	w := &Worker{ID: randomID("wk_", 8), Name: c.Name, Owner: c.Owner, PublicKey: c.PublicKey,
		Address: addr.String(), AddedAt: s.cfg.Now(), Limits: Limits{MaxParallelJobs: 1}}
	for s.st.Workers[w.ID] != nil {
		w.ID = randomID("wk_", 8)
	}
	s.st.Workers[w.ID] = w
	s.peersChanged()
	s.touch(true)
	return s.enrolment(w), nil
}

func (s *Store) peersChanged() {
	if s.OnPeers != nil {
		s.OnPeers(s.peers())
	}
}

// WorkerTunnel is a worker's tunnel configuration again.
func (s *Store) WorkerTunnel(id string) (Enrolment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.st.Workers[id]
	if w == nil {
		return Enrolment{}, errf(404, "not_found", "no worker %s", id)
	}
	return s.enrolment(w), nil
}

// RemoveWorker removes a worker and its peer; its leases end and their jobs are queued again.
func (s *Store) RemoveWorker(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.st.Workers[id]
	if w == nil {
		return errf(404, "not_found", "no worker %s", id)
	}
	for _, l := range s.st.Leases {
		if l.WorkerID == id && l.State == LeaseActive {
			s.endLease(l, LeaseCancelled, "worker_removed")
		}
	}
	delete(s.st.Workers, id)
	s.peersChanged()
	s.touch(true)
	return nil
}

// WorkerPatch renames or pauses a worker. PausedUntil is set when the field was in the patch;
// a nil inside means resume.
type WorkerPatch struct {
	Name        *string
	PausedUntil **time.Time
	Limits      *Limits
}

// PatchWorker changes a worker.
func (s *Store) PatchWorker(id string, p WorkerPatch) (WorkerView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.st.Workers[id]
	if w == nil {
		return WorkerView{}, errf(404, "not_found", "no worker %s", id)
	}
	if p.Name != nil {
		n := strings.TrimSpace(*p.Name)
		if n == "" || len(n) > 64 {
			return WorkerView{}, errf(422, "invalid", "name must have 1 to 64 characters")
		}
		for _, o := range s.st.Workers {
			if o != w && o.Name == n {
				return WorkerView{}, errf(409, "name_exists", "a worker is called %s already", n)
			}
		}
		w.Name = n
	}
	if err := s.applyPatch(w, p); err != nil {
		return WorkerView{}, err
	}
	s.touch(true)
	return s.workerView(w), nil
}

func (s *Store) applyPatch(w *Worker, p WorkerPatch) error {
	now := s.cfg.Now()
	if p.PausedUntil != nil {
		until := *p.PausedUntil
		if until != nil && until.After(now) {
			if w.PausedUntil == nil || !w.PausedUntil.After(now) {
				w.PausedAt = ptrTime(now)
			}
			u := until.UTC()
			w.PausedUntil = &u
		} else {
			w.PausedUntil, w.PausedAt = nil, nil
		}
	}
	if p.Limits != nil {
		l := *p.Limits
		if l.MaxParallelJobs < 1 {
			l.MaxParallelJobs = 1
		}
		if l.CPUThreads != nil && *l.CPUThreads < 1 {
			return errf(422, "invalid", "cpu_threads below 1")
		}
		switch l.Efficiency {
		case "", EfficiencyAll, EfficiencyArchive:
		default:
			return errf(422, "invalid", "efficiency %q: all, archive or none", l.Efficiency)
		}
		if v := l.EfficiencySpeed; v != nil && (*v <= 0 || *v > 1) {
			return errf(422, "invalid", "efficiency_speed outside (0, 1]")
		}
		s.retime(w, w.Limits, l, now)
		if l.HighPrioritySlots != nil && *l.HighPrioritySlots < 0 {
			return errf(422, "invalid", "high_priority_slots below 0")
		}
		w.Limits = l
	}
	return nil
}

// ListWorkers is every worker, by name.
func (s *Store) ListWorkers() []WorkerView {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ws []*Worker
	for _, w := range s.st.Workers {
		ws = append(ws, w)
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i].Name < ws[j].Name })
	out := make([]WorkerView, 0, len(ws))
	peers := s.peerStatus()
	for _, w := range ws {
		out = append(out, s.workerViewWith(w, peers))
	}
	return out
}

// GetWorker is one worker.
func (s *Store) GetWorker(id string) (WorkerView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.st.Workers[id]
	if w == nil {
		return WorkerView{}, errf(404, "not_found", "no worker %s", id)
	}
	return s.workerView(w), nil
}

// ---------------------------------------------------------------- the worker's own calls

// WorkerByAddress is the worker with this tunnel address.
func (s *Store) WorkerByAddress(a netip.Addr) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	as := a.Unmap().String()
	for _, w := range s.st.Workers {
		if w.Address == as {
			return w.ID, true
		}
	}
	return "", false
}

func (s *Store) worker(id string) (*Worker, error) {
	w := s.st.Workers[id]
	if w == nil {
		return nil, errf(403, "unknown_peer", "this tunnel address belongs to no worker")
	}
	return w, nil
}

// seen notes that a worker called.
func (s *Store) seen(w *Worker) {
	w.LastSeen = ptrTime(s.cfg.Now())
}

func (s *Store) connected(w *Worker, now time.Time) bool {
	if w.LastSeen == nil {
		return false // never heard of, restart or not
	}
	last := *w.LastSeen
	if s.started.After(last) {
		last = s.started // the queue's own downtime does not count
	}
	return now.Sub(last) <= 3*s.cfg.PresenceEvery
}

// claiming says whether a worker takes work now (v0.2.10; Thorsten's live session, 2026-10-27:
// the Mac Studio's worker had died inside Slipstream, the queue still counted it as connected,
// and encoder, free, left an archive job for it for the hour of archive's max_wait). It asked
// for work within two presence intervals, or works on a lease. A worker that calibrates, waits
// for models or died asks for nothing.
func (s *Store) claiming(w *Worker, now time.Time) bool {
	if !s.connected(w, now) || s.paused(w, now) {
		return false
	}
	if len(s.activeLeases(w.ID)) > 0 {
		return true
	}
	if w.AskedAt == nil {
		return false
	}
	last := *w.AskedAt
	if s.started.After(last) {
		last = s.started // the queue's own downtime does not count
	}
	return now.Sub(last) <= 2*s.cfg.PresenceEvery
}

func (s *Store) paused(w *Worker, now time.Time) bool {
	return w.PausedUntil != nil && w.PausedUntil.After(now)
}

// Me is the calling worker.
func (s *Store) Me(id string) (MeView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(id)
	if err != nil {
		return MeView{}, err
	}
	s.seen(w)
	return s.meView(w), nil
}

// PatchMe is the worker's own pause and limits.
func (s *Store) PatchMe(id string, p WorkerPatch) (MeView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(id)
	if err != nil {
		return MeView{}, err
	}
	s.seen(w)
	p.Name = nil
	if err := s.applyPatch(w, p); err != nil {
		return MeView{}, err
	}
	s.touch(true)
	return s.meView(w), nil
}

// CapabilityReport is the verdict on what a worker runs.
type CapabilityReport struct {
	Capabilities  Capabilities `json:"capabilities"`
	Accepted      []string     `json:"accepted"`
	Refused       []Refusal    `json:"refused,omitempty"`
	MinAppVersion string       `json:"min_app_version"`
	// WantedModels: the whisper models this worker should hold (v0.2.7); it fetches the ones it
	// lacks from their pinned URL and checks their sha256.
	WantedModels []CatalogueModel `json:"wanted_models"`
}

// Refusal is a kind a worker may not take.
type Refusal struct {
	Kind      string `json:"kind"`
	Reason    string `json:"reason"`
	Reference []Tool `json:"reference,omitempty"`
}

func (s *Store) report(w *Worker) CapabilityReport {
	r := CapabilityReport{Capabilities: *w.Caps, Accepted: []string{}, MinAppVersion: s.cfg.MinAppVersion}
	acc, ref := s.kinds(w)
	r.Accepted = acc
	r.Refused = ref
	r.WantedModels = s.wantedModels(w)
	return r
}

// kinds is which job kinds a worker's tools allow against the reference.
func (s *Store) kinds(w *Worker) (accepted []string, refused []Refusal) {
	accepted = []string{}
	if w.Caps == nil {
		return accepted, nil
	}
	for _, kind := range []string{KindRung, KindWhisper, KindCut, KindRemux, KindCheck, KindAlign} {
		if kind != KindRung && kind != KindWhisper && versionLess(w.Caps.AppVersion, "0.2.8") {
			continue // a worker before 0.2.8 knows no such kind; it neither takes nor refuses it
		}
		if kind == KindAlign {
			// v0.2.16: no tools of the reference's, one of the aligner's engines (jobs.go).
			switch {
			case versionLess(w.Caps.AppVersion, "0.2.16"):
			case alignEngine(w) == "":
				refused = append(refused, Refusal{Kind: kind, Reason: "no aligner engine (" + strings.Join(aligner.Engines, " or ") + ")"})
			default:
				accepted = append(accepted, kind)
			}
			continue
		}
		tools, configured, known := s.reference(kind, false)
		reason := ""
		switch {
		case configured && !known && w.Name != s.cfg.ReferenceWorker:
			reason = "the reference worker " + s.cfg.ReferenceWorker + " has not said what it runs yet"
		default:
			for _, t := range tools {
				v, ok := w.Caps.tool(t.Name)
				if !ok {
					reason = t.Name + " missing"
				} else if v != t.Version {
					reason = fmt.Sprintf("%s differs: worker %s, reference %s", t.Name, v, t.Version)
				}
				if reason != "" {
					break
				}
			}
			if !configured {
				for _, n := range toolsByKind[kind] {
					if _, ok := w.Caps.tool(n); !ok {
						reason = n + " missing"
						break
					}
				}
			}
		}
		if reason == "" {
			accepted = append(accepted, kind)
		} else {
			refused = append(refused, Refusal{Kind: kind, Reason: reason, Reference: tools})
		}
	}
	return accepted, refused
}

// PutCapabilities stores what a worker runs.
func (s *Store) PutCapabilities(id string, c Capabilities) (CapabilityReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(id)
	if err != nil {
		return CapabilityReport{}, err
	}
	s.seen(w)
	if c.AppVersion == "" {
		return CapabilityReport{}, errf(422, "invalid", "app_version is empty")
	}
	if s.cfg.MinAppVersion != "" && versionLess(c.AppVersion, s.cfg.MinAppVersion) {
		return CapabilityReport{}, errf(426, "app_too_old", "%s is below %s", c.AppVersion, s.cfg.MinAppVersion)
	}
	if h := c.HostApp; h != nil {
		h.Name, h.Version = oneLine(h.Name, 64), oneLine(h.Version, 64)
		if h.Name == "" {
			c.HostApp = nil
		}
	}
	checkCalibration(&c)
	w.Caps = &c
	s.touch(true)
	return s.report(w), nil
}

// GetCapabilities is what a worker last said it runs.
func (s *Store) GetCapabilities(id string) (CapabilityReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(id)
	if err != nil {
		return CapabilityReport{}, err
	}
	if w.Caps == nil {
		return CapabilityReport{}, errf(404, "not_found", "no capabilities yet")
	}
	return s.report(w), nil
}

// Presence is a worker's heartbeat.
type Presence struct {
	State      string `json:"state"`
	DiskFree   *int64 `json:"disk_free_bytes,omitempty"`
	Battery    *bool  `json:"on_battery,omitempty"`
	MemoryUsed *int64 `json:"memory_used_bytes,omitempty"`
	// ThreadsBusy: how many threads the worker's own tools kept busy since its last presence
	// (v0.2.5).
	ThreadsBusy *int `json:"threads_busy,omitempty"`
	// OtherLoad: threads other programs on the machine keep busy (v0.2.7), for the scoring.
	OtherLoad *float64 `json:"other_load,omitempty"`
}

// PresenceAck answers a heartbeat.
type PresenceAck struct {
	ServerTime     time.Time  `json:"server_time"`
	PresenceEveryS int        `json:"presence_every_s"`
	PausedUntil    *time.Time `json:"paused_until"`
	Draining       bool       `json:"draining"`
	// Points: the worker's own today, this week and in all, and its place today (v0.2.15), for
	// Slipstream's menu.
	Points PointsRank `json:"points"`
}

// PutPresence notes that a worker is alive.
func (s *Store) PutPresence(id string, p Presence) (PresenceAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.worker(id)
	if err != nil {
		return PresenceAck{}, err
	}
	if p.State != "idle" && p.State != "working" && p.State != "paused" {
		return PresenceAck{}, errf(422, "invalid", "state is idle, working or paused")
	}
	s.seen(w)
	w.Presence = p.State
	w.DiskFree = p.DiskFree
	if p.MemoryUsed != nil {
		w.MemoryUsed, w.memoryAt = p.MemoryUsed, s.cfg.Now()
	}
	if p.ThreadsBusy != nil && *p.ThreadsBusy < 0 {
		return PresenceAck{}, errf(422, "invalid", "threads_busy is 0 or more")
	}
	w.ThreadsBusy = p.ThreadsBusy
	if p.OtherLoad != nil && (*p.OtherLoad < 0 || math.IsNaN(*p.OtherLoad) || math.IsInf(*p.OtherLoad, 0)) {
		return PresenceAck{}, errf(422, "invalid", "other_load is 0 or more")
	}
	w.OtherLoad = p.OtherLoad
	s.touch(false)
	now := s.cfg.Now()
	ack := PresenceAck{ServerTime: now.UTC(), PresenceEveryS: int(s.cfg.PresenceEvery / time.Second), Draining: s.draining, Points: s.pointsRank(w)}
	if s.paused(w, now) {
		ack.PausedUntil = w.PausedUntil
	}
	return ack, nil
}

// versionLess compares dotted versions number by number; text that is not a number compares
// as text.
func versionLess(a, b string) bool {
	pa, pb := strings.Split(strings.TrimPrefix(a, "v"), "."), strings.Split(strings.TrimPrefix(b, "v"), ".")
	for i := 0; i < len(pa) || i < len(pb); i++ {
		var x, y string
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		var nx, ny int
		_, ex := fmt.Sscanf(x, "%d", &nx)
		_, ey := fmt.Sscanf(y, "%d", &ny)
		if ex == nil && ey == nil {
			if nx != ny {
				return nx < ny
			}
			continue
		}
		if x != y {
			return x < y
		}
	}
	return false
}

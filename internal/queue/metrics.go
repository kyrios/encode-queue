package queue

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

func freeBytes(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// MetricHelp is every series the queue exports, with its type and help text. The README
// lists the same; a test keeps the two equal.
var MetricHelp = []struct{ Name, Type, Help string }{
	{"encode_queue_build_info", "gauge", "The queue's version, as a label; always 1."},
	{"encode_queue_revision", "gauge", "The queue's revision: every change to a job raises it."},
	{"encode_queue_jobs", "gauge", "Jobs by priority and state."},
	{"encode_queue_workers", "gauge", "Workers by state (idle, working, paused, offline)."},
	{"encode_queue_worker_connected", "gauge", "1 while the worker called within three presence intervals."},
	{"encode_queue_worker_paused", "gauge", "1 while the worker is paused."},
	{"encode_queue_worker_jobs", "gauge", "The worker's jobs now: leased (an active lease) and checking (a result waiting for the pipeline)."},
	{"encode_queue_worker_attempts_total", "counter", "Attempts the worker ended, by outcome: accepted, rejected, superseded, failed, given_back, expired, preempted, cancelled."},
	{"encode_queue_worker_leases_expired_total", "counter", "Leases of the worker that ran out or whose worker went offline."},
	{"encode_queue_worker_results_rejected_total", "counter", "Results of the worker that the pipeline rejected."},
	{"encode_queue_worker_fps", "gauge", "Frames per second the worker reported last, summed over its active leases; 0 when idle."},
	{"encode_queue_worker_speed", "gauge", "Times real time the worker reported last, summed over its active leases; 0 when idle."},
	{"encode_queue_worker_last_seen_timestamp_seconds", "gauge", "When the worker last called, in Unix seconds."},
	{"encode_queue_worker_tool_info", "gauge", "The tool versions the worker runs, as labels; always 1."},
	{"encode_queue_worker_info", "gauge", "The worker's version, OS, CPU model, architecture and the app it runs inside (host_app, host_app_version), as labels; always 1."},
	{"encode_queue_worker_cpu_threads", "gauge", "Logical CPUs the worker can use."},
	{"encode_queue_worker_cpu_threads_in_use", "gauge", "The encoder's thread limit: the worker's cpu_threads limit, or all its CPUs."},
	{"encode_queue_worker_cpu_threads_busy", "gauge", "Threads the worker's own tools kept busy since its last presence."},
	{"encode_queue_worker_memory_bytes", "gauge", "The worker's memory."},
	{"encode_queue_worker_memory_used_bytes", "gauge", "The worker's memory in use, from its last presence."},
	{"encode_queue_worker_transfer_bytes_per_second", "gauge", "Moving average of the worker's transfer rate through the tunnel, by direction (download: sources, upload: results)."},
	{"encode_queue_worker_relative_speed", "gauge", "The worker's encoding speed against the reference worker on the same rung classes, a moving average, or from the calibration clip until it has measured results; 1 is as fast."},
	{"encode_queue_worker_last_handshake_timestamp_seconds", "gauge", "The worker's last WireGuard handshake, in Unix seconds."},
	{"encode_queue_jobs_pinned", "gauge", "Queued jobs the Encoder page sent to one worker, waiting for it."},
}

func esc(v string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(v)
}

// WriteMetrics writes the Prometheus text exposition.
func (s *Store) WriteMetrics(w io.Writer, version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	lines := map[string][]string{}
	add := func(name, labels string, v float64) {
		if labels != "" {
			labels = "{" + labels + "}"
		}
		lines[name] = append(lines[name], fmt.Sprintf("%s%s %v", name, labels, v))
	}
	add("encode_queue_build_info", fmt.Sprintf(`version="%s"`, esc(version)), 1)
	add("encode_queue_revision", "", float64(s.st.Revision))
	jobs := map[[2]string]int{}
	for _, j := range s.st.Jobs {
		jobs[[2]string{j.Priority, s.jobState(j)}]++
	}
	for _, p := range Priorities {
		for _, st := range []string{StateQueued, StateLeased, StateChecking, StateDone, StateFailed, StateCancelled} {
			add("encode_queue_jobs", fmt.Sprintf(`priority="%s",state="%s"`, p, st), float64(jobs[[2]string{p, st}]))
		}
	}
	states := map[string]int{"idle": 0, "working": 0, "paused": 0, "offline": 0}
	var ws []*Worker
	for _, w := range s.st.Workers {
		ws = append(ws, w)
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i].Name < ws[j].Name })
	peers := s.peerStatus()
	pinned := 0
	for _, j := range s.st.Jobs {
		if j.Pin != nil {
			pinned++
		}
	}
	add("encode_queue_jobs_pinned", "", float64(pinned))
	for _, wk := range ws {
		states[s.workerState(wk, now)]++
		l := fmt.Sprintf(`worker_id="%s",worker="%s"`, wk.ID, esc(wk.Name))
		b := func(v bool) float64 {
			if v {
				return 1
			}
			return 0
		}
		add("encode_queue_worker_connected", l, b(s.connected(wk, now)))
		add("encode_queue_worker_paused", l, b(s.paused(wk, now)))
		leased, checking := 0, 0
		fps, speed := 0.0, 0.0
		for _, ls := range s.st.Leases {
			if ls.WorkerID != wk.ID {
				continue
			}
			if ls.State == LeaseActive {
				leased++
				if p := ls.Progress; p != nil && p.Phase == "encoding" {
					if p.FPS != nil {
						fps += *p.FPS
					}
					if p.Speed != nil {
						speed += *p.Speed
					}
				}
			}
			if ls.Result != nil && ls.Result.Verdict == VerdictChecking {
				checking++
			}
		}
		add("encode_queue_worker_jobs", l+`,state="leased"`, float64(leased))
		add("encode_queue_worker_jobs", l+`,state="checking"`, float64(checking))
		c := wk.Counts
		for _, o := range []struct {
			name string
			n    int64
		}{{"accepted", c.Accepted}, {"rejected", c.Rejected}, {"superseded", c.Superseded}, {"failed", c.Failed},
			{"given_back", c.GivenBack}, {"expired", c.Expired}, {"preempted", c.Preempted}, {"cancelled", c.Cancelled}} {
			add("encode_queue_worker_attempts_total", l+fmt.Sprintf(`,outcome="%s"`, o.name), float64(o.n))
		}
		add("encode_queue_worker_leases_expired_total", l, float64(c.Expired))
		add("encode_queue_worker_results_rejected_total", l, float64(c.Rejected))
		add("encode_queue_worker_fps", l, fps)
		add("encode_queue_worker_speed", l, speed)
		if wk.LastSeen != nil {
			add("encode_queue_worker_last_seen_timestamp_seconds", l, float64(wk.LastSeen.Unix()))
		}
		sys := s.system(wk)
		if wk.Caps != nil {
			val := func(p *string) string {
				if p == nil {
					return ""
				}
				return esc(*p)
			}
			hostName, hostVersion := "", ""
			if h := wk.Caps.HostApp; h != nil {
				hostName, hostVersion = esc(h.Name), esc(h.Version)
			}
			add("encode_queue_worker_info", l+fmt.Sprintf(`,app_version="%s",os="%s",cpu_model="%s",arch="%s",host_app="%s",host_app_version="%s"`,
				esc(wk.Caps.AppVersion), esc(wk.Caps.OS), val(sys.CPUModel), val(sys.Arch), hostName, hostVersion), 1)
		}
		if sys.CPUThreads != nil {
			add("encode_queue_worker_cpu_threads", l, float64(*sys.CPUThreads))
		}
		if sys.ThreadsInUse != nil {
			add("encode_queue_worker_cpu_threads_in_use", l, float64(*sys.ThreadsInUse))
		}
		if sys.ThreadsBusy != nil {
			add("encode_queue_worker_cpu_threads_busy", l, float64(*sys.ThreadsBusy))
		}
		if sys.MemoryBytes != nil {
			add("encode_queue_worker_memory_bytes", l, float64(*sys.MemoryBytes))
		}
		if sys.MemoryUsed != nil {
			add("encode_queue_worker_memory_used_bytes", l, float64(*sys.MemoryUsed))
		}
		for _, t := range []struct {
			dir string
			st  TransferStats
		}{{DirDownload, wk.Download}, {DirUpload, wk.Upload}} {
			if t.st.Samples > 0 {
				add("encode_queue_worker_transfer_bytes_per_second", l+fmt.Sprintf(`,direction="%s"`, t.dir), t.st.AvgBps)
			}
		}
		if r := s.relativeSpeed(wk); r != nil {
			add("encode_queue_worker_relative_speed", l, *r)
		}
		if p, ok := peers[wk.PublicKey]; ok && !p.LastHandshake.IsZero() {
			add("encode_queue_worker_last_handshake_timestamp_seconds", l, float64(p.LastHandshake.Unix()))
		}
		if wk.Caps != nil {
			for _, t := range wk.Caps.Tools {
				add("encode_queue_worker_tool_info", l+fmt.Sprintf(`,tool="%s",version="%s"`, esc(t.Name), esc(t.Version)), 1)
			}
		}
	}
	for _, st := range []string{"idle", "working", "paused", "offline"} {
		add("encode_queue_workers", fmt.Sprintf(`state="%s"`, st), float64(states[st]))
	}
	for _, m := range MetricHelp {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", m.Name, m.Help, m.Name, m.Type)
		for _, line := range lines[m.Name] {
			fmt.Fprintln(w, line)
		}
	}
}

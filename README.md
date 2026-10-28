# encode-queue

One queue for video encodes and whisper runs, and the worker that takes jobs from it, on any
Linux server or Mac. **Slipstream** is the encoder helper's name on every machine: the Mac app
([kyrios/slipstream](https://github.com/kyrios/slipstream)) and the Linux container
`ghcr.io/kyrios/slipstream`. Both run this repository's worker, whose binary is
`encode-worker`. Built for iPhoneblog's `encoder` ([ADR-0007](https://github.com/iPhoneblog-web/iphoneblog-wordpress/blob/main/docs/adr/0007-encode-helpers.md)).

- **The queue server** (`encode-queue`) keeps the jobs, their priorities (stage 1, stage 2,
  archive), leases, attempts and the workers, on disk. It brings its own WireGuard: wireguard-go
  in userspace with its own network stack, one UDP port, no host interface, no network rights.
  The peer list is the queue's state.
- **The worker** (`encode-worker`) pairs by its public key, long-polls for a job, fetches the
  source with range requests, runs ffmpeg (libx265, libx264) or whisper.cpp at `nice 19`, reports
  progress and log lines, and uploads the result over tus. Jobs are rungs, whisper runs, and
  from v0.2.8 cuts, remuxes and checks: every encode and every decode runs on a worker.
- **The worker measures, the pipeline judges** (v0.2.8). After a rung, a cut or a remux the
  same lease checks the result: a full decode, its frames and length, SSIM, PSNR-Y and XPSNR
  (and VMAF when asked) against the source, the packets of a remux. The figures go with the
  file; the pipeline, which reads headers only, accepts or rejects them against ADR-0005's
  floors, which never reach a worker. The queue never judges quality. The first accepted
  result wins. A `check` job checks a file no worker encoded, as it streams from the queue.

## The APIs

| file | who calls it | where |
|---|---|---|
| [`api/pipeline-admin.yaml`](api/pipeline-admin.yaml) | the pipeline (submit, watch, judge) and the upload center's Encoder page (queue, workers, pairing) | the queue's TCP listener, with a bearer token each |
| [`api/helper.yaml`](api/helper.yaml) | every worker | inside the WireGuard tunnel only |
| [`api/worker-control.yaml`](api/worker-control.yaml) | the app that embeds the worker (Slipstream) | the worker's Unix socket |

All three are OpenAPI 3.1.

## How it behaves

- **Priorities**: `now`, then `stage1`, then `stage2`, then `archive`, and a place within each,
  which the Encoder page can change. A worker gets the first job in that order it may run and
  would finish about as soon as any other (**Which worker gets a job**, below). `now` (the Encoder
  page's "Sofort") is for a job someone in Funkhaus waits for: it never preempts, and a worker
  takes one in its **extra slot** beside other work, on top of `max_parallel_jobs`, while its free
  RAM (memory less the last presence's use less 1 GB) holds the job's expected peak (a 4K
  `slower` encode 5.5 GB, whisper medium 2.5 GB). Without the RAM it simply waits first in line.
- **Exact tool versions**: a worker gets a job only if its tools match the job's `requires`
  exactly, as text. Without `requires` a job requires the tools of the reference worker
  (`EQ_REFERENCE_WORKER`, `encoder`'s own worker container): ffmpeg, libx265 and libx264 for a
  rung, whisper.cpp for whisper (and ffmpeg when the source is not WAV), plus the model by its
  sha256 and the captions' font. A job with burnt-in captions also requires the reference's
  libass (v0.2.7; toolchain 3 builds it on both platforms, reported from its versions.txt), and
  the worker lists fonts with the toolchain's fc-list and points `FONTCONFIG_FILE` at its
  fonts.conf (Inter only) unless something set it. Until the reference worker has said what it
  runs, only it gets jobs.
- **Which worker gets a job** (v0.2.7): when a worker asks, the queue estimates for each eligible
  worker the expected completion time, ECT = until it has a free slot + the source's bytes over
  its measured download rate + the compute + the expected result's bytes
  (`result.expected_bytes`) over its upload rate. The compute is the job's frames over its
  measured or calibrated fps on the rung class (whisper: audio seconds over its speed with the
  model it would run), scaled by the threads it may use over those free of other programs
  (`other_load` in its presence) and its own jobs, times a correction learned from its results.
  The asking worker takes the job when its ECT is at most 1.25 times the best, or when the job
  waited longer than 30 s (`now`), 2 min (`stage1`), 15 min (`stage2`) or 1 h (`archive`); else
  the queue looks at the next job for it. A job sent by hand, or one nobody's speed is known
  for, goes to whoever asks. Each attempt keeps its ECT and the runner-up's.
  **Only a worker that takes work counts as the better one** (v0.2.10; Thorsten's live session,
  2026-10-27: the Mac Studio's worker had died inside Slipstream, the Mac stayed connected, and
  encoder, free, left an archive job to it for the hour of archive's wait). A worker is
  `claiming` when it asked for work within two presence intervals or works on a lease; one that
  died, calibrates or waits for models asks for nothing. A worker shows it, and so does each move
  menu candidate. Each attempt says why it had the comparison it had, as `scoring`: `scored`,
  `waited` (a better worker existed; the job's wait ran out), `assigned`, `only_worker` (no other
  worker could take it), `not_claiming` (others could, but asked for nothing), `speed_unknown`
  (others asked, but no worker's speed on the job's class is known: encoder, the reference,
  needs a result or a calibration of the class first, and the others' speed is relative to it).
- **Whisper models** (v0.2.7): a job may list `models` best first. A worker holding one may take
  it unless a connected worker holds a better one; the lease names the one chosen. The queue
  tells each worker which models to hold (`wanted_models`): small for caption timing and the
  best one it runs at least `EQ_WHISPER_MIN_SPEED_PCT` per cent of real time by its calibration.
  A model it has not timed is guessed from small's speed, but not wanted while a model below it
  measured too slow; when none clears the threshold, the best model it holds stays wanted as its
  fallback (encoder's medium at 0.96×). The speed never keeps a held model from a job.
  The worker fetches those from ggerganov/whisper.cpp at a pinned revision only, checks their
  sha256 and times them on the calibration's speech clip.
- **Leases** last 120 s and are renewed every 30 s with the progress; the long poll waits 25 s;
  presence every 30 s, offline after three missed. A lease that runs out, a worker that goes
  offline, a removed worker: the job goes back to `queued`, the next attempt with the next number.
- **Preemption**: a stage 1 or stage 2 job that no free worker may run ends the active lease
  with the lowest priority (then the youngest) whose worker may run it; that worker's next
  renewal answers `410 preempted`.
- **Moving a job to a worker** (`POST /v1/jobs/{id}/assign`): a queued or leased job goes to
  one worker that is connected, not paused and runs the reference's tools. Its attempt on
  another worker is preempted; the job is pinned to the target until the target leases it, or
  until the target goes offline, is paused or removed.
- **What the Encoder page sees of a worker**: its WireGuard endpoint and last handshake, CPU,
  cores, threads in use, memory, OS, the transfer rates the queue measures (source downloads
  and result uploads, a moving average), and `relative_speed`: its fps against the reference
  worker's (the median of its last 5 results per codec, size and preset, against the reference's).
  Until a worker has measured results, the **calibration** gives it one: at pairing and when
  ffmpeg, libx265 or libx264 change, every worker (the reference too) makes the same 10 s 4K clip
  with its own ffmpeg, bit-exact, and encodes it once per rung class of the ladder with the
  rung's real flags, about a minute; `relative_speed_source` says `calibrated` or `measured`.
  The worker claims while it calibrates (v0.2.11): a calibration beside a job measures low, and
  the first measured result overrides it, but a worker that waited for its calibration stood
  "bereit" claiming nothing, a minute or more after every toolchain change.
  `EQ_CALIBRATE=false` turns it off. Without either, `speed_estimate` gives the live fps of the
  rung job it encodes against the reference's median on that class. `threads_busy` is how many
  threads the worker's own tools kept busy since its last presence.
  Each job has `eta_s` and `finish_at`; `GET /v1/jobs/{id}/candidates` estimates the job on
  every worker for the move menu, and `DELETE /v1/jobs/{id}/assign` ends a pin.
- **One worker cannot hold the queue** (v0.2.9; the security review, 2026-10-27, M2): a worker
  sets its own `max_parallel_jobs` and reports its own calibration, so the queue caps both.
  Regular slots are at most `EQ_MAX_PARALLEL_JOBS` (2; the Sofort slot comes on top). A
  calibrated speed counts as at most four times the reference's until measured results show
  more, and a measured sample is never faster than the frames over the time the queue saw the
  lease held. A lease may be renewed up to `hold_until`: `EQ_HOLD_FACTOR` (3) times its
  expected completion time, at least `EQ_HOLD_MIN_S` (30 min), and `EQ_HOLD_MAX_S` (12 h)
  when the queue expects nothing. After it the renewal answers `410 overdue`, the job goes back
  to the queue, and that worker gets it no more, as after a rejection; the job counts no failure.
- **Cut, remux and check** (v0.2.8; ADR-0007: every encode and decode on a worker): the queue
  takes `cut`, `remux` and `check` jobs with strict params, matches `cut` on the reference's
  ffmpeg and libx264 and the others on its ffmpeg, and gives them, a rung's `check` step and
  `audio.mono: measure` only to workers of 0.2.8 or later. A worker's check figures arrive with
  its file (`result.check`), or alone from a `check` job (`outcome: figures`, no file); they wait
  as `checking` for the pipeline's verdict like a file, and the job's attempts show them. A
  worker may report the phase `checking` while it checks.
- **A clip's rung** (v0.2.13; video-pipeline v0.2.10's Short, one 9:16 file for Instagram
  Reels, YouTube Shorts, Mastodon, Threads and Bluesky): a rung may set `fps` (`fps=` after the
  crop, before the scale; a number or `"30000/1001"`), `audio.sample_rate` and `audio.channels`
  (`-ar`, `-ac`, with AAC only) and `edit_list: false` (`-use_editlist 0`; Instagram's and Threads'
  specs want no edit lists). A worker before 0.2.13 reads params loosely and would make the file
  without them, so such a rung goes only to workers of 0.2.13 or later; until one is paired it
  waits, and the Encoder page's reason names the version and the older workers. At another rate a
  file reports its own frames and no `source_frames`, and a comparison takes the source at that
  rate too. Without edit lists ffmpeg keeps picture and
  sound together by the timestamps; AVFoundation plays the sound early by the picture's B-frame
  delay less the AAC priming (58.7 ms at 25 fps, 22.7 ms at 60), measured in helper.yaml's
  `edit_list`.
- **Late results** are taken and judged while the job is not done; the first accepted wins,
  later ones are `superseded`. A rejected or failed result excludes its worker from that job;
  after `max_failures` (3) the job is `failed`.
- **Pause**: the worker stops its tool (SIGSTOP), keeps renewing with `phase: paused`, and gives
  the job back after 60 s. The queue takes it back itself 30 s later if the worker does not.
- **Disk**: the queue leases only while its result directory has room for every active lease's
  `max_bytes` plus `EQ_RESULT_RESERVE_BYTES`.

## The queue server

Every address, port, subnet and path comes from the environment; the image has none.

| variable | what it is |
|---|---|
| `EQ_STATE_DIR` | the state (`state.json`), the WireGuard key; required |
| `EQ_SOURCE_DIR` | where job sources are; a job's `source.path` is relative to it; required |
| `EQ_RESULT_DIR` | where results land; default `$EQ_STATE_DIR/results` |
| `EQ_API_LISTEN` | the pipeline and admin API and `/healthz`, e.g. `:8090`; also `/metrics` unless `EQ_METRICS_LISTEN` is set; required. Never reachable through the tunnel |
| `EQ_METRICS_LISTEN` | `/metrics` and `/healthz` on a listener of their own, e.g. `:8091`, so a scraper never reaches the API |
| `EQ_PIPELINE_TOKEN_FILE`, `EQ_ADMIN_TOKEN_FILE` | files holding the two bearer tokens (16 characters at least, different), sent as `Authorization: Bearer <token>`; required |
| `EQ_WG_LISTEN_PORT` | the UDP port WireGuard listens on; required |
| `EQ_WG_ADDRESS` | the queue's address in the tunnel; required |
| `EQ_WG_SUBNET` | the tunnel's subnet, from which workers get their addresses; required |
| `EQ_WG_API_PORT` | the helper API's port inside the tunnel; required |
| `EQ_WG_ENDPOINT` | `host:port` the workers send their UDP to (what `iron` forwards); required |
| `EQ_WG_PRIVATE_KEY_FILE` | the queue's WireGuard private key (base64, as `wg genkey` prints it); default `$EQ_STATE_DIR/wireguard.key`, made on first start when missing |
| `EQ_WG_KEEPALIVE_S` | the keepalive workers get; default 25 |
| `EQ_REFERENCE_WORKER` | the name of the worker whose tools jobs require by default |
| `EQ_WHISPER_MIN_SPEED_PCT` | how fast a worker must transcribe with a model, in per cent of real time, for the queue to want it there; default 100 |
| `EQ_MIN_APP_VERSION` | workers below it get `426` |
| `EQ_MAX_FAILURES` | default 3 |
| `EQ_MAX_PARALLEL_JOBS` | the most regular slots any worker gets, whatever it asks for; default 2 (v0.2.9) |
| `EQ_HOLD_FACTOR`, `EQ_HOLD_MIN_S`, `EQ_HOLD_MAX_S` | how long a lease may be held: the factor times its expected completion time, between the two; defaults 3, 1800 and 43200 (v0.2.9) |
| `EQ_EXCLUDE_BACKOFF_S` | default 600: how long a job that every able worker is excluded from waits before the reference worker gets it again |
| `EQ_RESULT_RESERVE_BYTES` | default 2 GiB |
| `EQ_LEASE_S`, `EQ_RENEW_EVERY_S`, `EQ_LONG_POLL_S`, `EQ_PRESENCE_EVERY_S`, `EQ_PAUSE_GIVE_BACK_S` | the timings, defaults 120, 30, 25, 30 and 60 |
| `EQ_JOB_RETENTION` | how long done, cancelled and failed jobs stay, with their result files: seconds, `7d` or `36h`; default 7 days. `EQ_DONE_KEEP_S`, its older name, still works |
| `EQ_LOG_LEVEL` | `debug`, `info` (default), `warn`, `error` |

`encode-queue pubkey` prints the queue's public key (making the key if there is none).
`encode-queue healthcheck` asks `/healthz` on `EQ_API_LISTEN` and exits 0 when it answers (the
image's `HEALTHCHECK`; distroless has no curl). The image runs as uid 65532 (distroless
`nonroot`). Adding and removing a worker changes only `state.json` in `EQ_STATE_DIR`: the peers
live in the queue's own WireGuard, nothing on the host changes. SIGTERM drains: no new leases
(`503 draining`), the long polls end, the state is written.

```bash
docker run --rm -p 8090:8090 -p 51821:51821/udp -v /srv/eq:/data \
  -e EQ_STATE_DIR=/data/state -e EQ_SOURCE_DIR=/data/sources -e EQ_API_LISTEN=:8090 \
  -e EQ_PIPELINE_TOKEN_FILE=/data/pipeline.token -e EQ_ADMIN_TOKEN_FILE=/data/admin.token \
  -e EQ_WG_LISTEN_PORT=51821 -e EQ_WG_ADDRESS=10.10.20.1 -e EQ_WG_SUBNET=10.10.20.0/24 \
  -e EQ_WG_API_PORT=8090 -e EQ_WG_ENDPOINT=vpn.example.org:51821 \
  ghcr.io/kyrios/encode-queue:v0.1.0
```

(The addresses above are `encoder`'s role defaults, shown as an example only.)

## The worker

1. `encode-worker pubkey` prints the worker's public key, made on first use (in
   `$EQ_WORKER_STATE_DIR/wireguard.key`, mode 0600; the private key never leaves the machine).
2. Paste it into the Encoder page (or `POST /v1/workers`): the answer is one line,
   `eqtunnel1 address=… server_public_key=… endpoint=… allowed_ips=… keepalive=25 api=…`.
3. Give the worker that line in `EQ_TUNNEL` or the file `EQ_TUNNEL_FILE` (default
   `$EQ_WORKER_STATE_DIR/tunnel.conf`) and start `encode-worker run`.

| variable | what it is |
|---|---|
| `EQ_WORKER_STATE_DIR` | its key, tunnel line, model cache; default the user's config directory |
| `EQ_TUNNEL`, `EQ_TUNNEL_FILE` | the tunnel line |
| `EQ_TUNNEL_ENDPOINT` | another `host:port` for the queue's UDP (a worker beside the queue) |
| `EQ_WORK_DIR` | where sources and results are while a job runs; default `$EQ_WORKER_STATE_DIR/work` |
| `EQ_FFMPEG`, `EQ_WHISPER_CLI` | the tools; default `ffmpeg`, `whisper-cli` from `PATH` |
| `EQ_WHISPER_VERSION` | the whisper.cpp release, when there is no `versions.txt` beside `whisper-cli` (the toolchain has one) |
| `EQ_TOOLCHAIN_VERSIONS` | the toolchain's `versions.txt` when it is not beside ffmpeg and whisper-cli (Slipstream's signed bundle keeps it in `Contents/Resources`): libass's and whisper.cpp's versions come from it; `EQ_WHISPER_VERSION` still wins (v0.2.8) |
| `EQ_WHISPER_MODELS_DIR` | the `*.bin` models, found by sha256 (`/models` in the image); the models the queue wants are fetched into it, or into `STATE/models` without it. A read-only folder (encoder's, filled by infra) fetches nothing: a wanted model it lacks is `unavailable`, provided by the host, never a failure |
| `EQ_CALIBRATE` | `false` skips the calibration at pairing (`true` by default) |
| `EQ_FONTS` | font families to announce besides fontconfig's, comma-separated |
| `EQ_HOST_APP_NAME`, `EQ_HOST_APP_VERSION` | the app the worker runs inside (Slipstream and its version), shown on the Encoder page; never compared with the queue's minimum |
| `EQ_CPU_THREADS` | x265's pool (`pools=N`) and whisper.cpp's threads; default the tools' own |
| `EQ_GPU` | whisper.cpp may use the GPU; default false |
| `EQ_MAX_PARALLEL_JOBS` | default 1 |
| `EQ_CONTROL_SOCKET` | a Unix socket for [worker-control.yaml](api/worker-control.yaml) |
| `EQ_WORKER_KEY_FILE` | the private key; default `$EQ_WORKER_STATE_DIR/wireguard.key`, made only when missing, so a key Ansible puts there keeps the worker's identity |
| `EQ_LOG_LEVEL` | as the queue's |

Every variable is also a flag (`encode-worker -h`); the flag wins. `encode-worker healthcheck`
exits 0 when the queue answered within two minutes (the image's `HEALTHCHECK`). The image runs
as uid 10001 (`worker`) with its scratch space in `/var/lib/encode-worker/work`.

### Embedding (Slipstream)

```
nice -n 19 encode-worker --key-stdin --tunnel-file … --state-dir … --name … --app-version x.y.z \
  --threads N --gpu true|false --jobs N --ffmpeg … --ffprobe … --whisper-cli … \
  --whisper-models … --control stdio
```

- **Standard input**: the first line is the WireGuard private key in base64 (the app keeps it
  in the keychain). Then one JSON object per line: `{"cmd":"pause","until":"<RFC 3339>"}`,
  `{"cmd":"resume"}`, `{"cmd":"limits","threads":N,"gpu":bool,"jobs":N}`, `{"cmd":"quit"}`
  (give every lease back with `reason=quit`, exit 0). The end of input quits too.
- **Standard output**: one JSON object per line, `event` naming it: `connection` (`up`,
  `last_answer_at`, `refused: [{kind, reason}]`), `job` (`lease_id`, `title`, `kind`, `rung`,
  `phase`, `fraction`, `fps`, `speed`, `eta_s`; at every phase change and once a second),
  `idle`, `result` (`lease_id`, `title`, `verdict`: `accepted`, `rejected` or `given_back`,
  `check`), `paused` (`until` or null), `update` (`latest`, when the queue wants a newer app),
  `tools` (`tools: [{name, version}]`), `log` (`lease_id`, `line`), `calibrating` (`classes`,
  when the calibration clip starts) and `calibrated` (`classes`, when it is done), and `model`
  for each whisper model the queue wants (`name`, `sha256`, `state`: `wanted`, `downloading`,
  `checking`, `calibrating`, `ready`, `failed`, or `unavailable` when the models folder is read-only and the host provides the models; `bytes`, `bytes_done` while downloading,
  `speed` in times real time once timed, `reason` when it failed).
- **Standard error** is the log for people.

Without `--control stdio`, `--control <path>` serves [worker-control.yaml](api/worker-control.yaml)
on a Unix socket instead.
`encode-worker selftest` encodes, cuts and remuxes a three-second clip with the machine's own
ffmpeg, checks each result and prints the figures; it exits 1 with the problems. A lease works
in its own folder below `EQ_WORK_DIR`, deleted however the lease ends; at start the worker
deletes any lease folder a crash left.

`encode-worker versions` prints what the worker runs, exactly as the queue will compare it. The
worker builds ffmpeg's arguments itself from the job's structured settings, in the order
infra's `video-encode` did (a test compares them with `video-encode`'s own), so a worker's file
is the file `encoder` made before the split. The cut and the check run the commands
video-pipeline ran itself before v0.2.8 (testdata/pipeline-v028-args.json).

### The images

- `ghcr.io/kyrios/encode-queue`: the queue, on distroless.
- `ghcr.io/kyrios/encode-toolchain:<N>`: the one toolchain of [`toolchain/versions`](toolchain/versions),
  built from source: ffmpeg with libx265 (8, 10 and 12 bits), libx264, libvmaf, libmp3lame and
  libass, fc-list, and whisper-cli, in `/opt/encode-toolchain/bin` (on `PATH`) with
  `versions.txt`, on Debian 13. The worker image and video-pipeline's image build FROM it, so
  every worker and the pipeline run the same build. Tag `toolchain-<N>` releases it, with
  `encode-toolchain-darwin-arm64.tar.gz` (whisper.cpp with Metal) for Macs and the source
  tarballs it was built from. Each component's licence and `SOURCES.md` are in
  `/opt/encode-toolchain/licenses` (and `licenses/` in the tarball): see "Licence".
- **Captions are the same on Linux and macOS** from toolchain 3 on: libass and its stack
  (HarfBuzz, FriBidi, FreeType, fontconfig, Expat) are built from source on both, and
  `fonts/` holds Inter with a `fonts.conf` that sees only those fonts. The image sets
  `FONTCONFIG_FILE=/opt/encode-toolchain/fonts/fonts.conf`; an app that embeds the macOS
  tarball sets `FONTCONFIG_FILE` to its `fonts/fonts.conf` and puts its `fc-list` first on the
  worker's `PATH`. The toolchain workflow burns the same ASS file on both and compares the
  pictures.
- `ghcr.io/kyrios/slipstream`: Slipstream for Linux, the worker (`encode-worker`) on that
  toolchain. Models are not in the image; mount them at `/models` or point
  `EQ_WHISPER_MODELS_DIR` at a read-only mount. Up to v0.2.2 this image was
  `ghcr.io/kyrios/encode-worker`; releases from v0.2.3 on publish only `slipstream`.
- Each release `vX.Y.Z` carries `encode-worker-darwin-arm64` and the toolchain's
  `encode-toolchain-darwin-arm64.tar.gz`, with `SHA256SUMS`, for Slipstream to embed and sign.

Images are tagged with the release (`v0.1.0`) and pinned by Ansible. From v0.2.7 on, the
worker image and the release's toolchain tarball are toolchain 3 (libass and Inter on both
platforms), with every licence in `licenses/`.

## Metrics

`GET /metrics`, without a token, on `EQ_METRICS_LISTEN` when it is set (on the API listener
otherwise). Per-worker series carry `worker_id` and
`worker` (its name); `worker_id` stays when a worker is renamed.

<!-- metrics: generated from internal/queue/metrics.go; a test keeps them equal -->
| series | type | what it is |
|---|---|---|
| `encode_queue_build_info` | gauge | The queue's version, as a label; always 1. |
| `encode_queue_revision` | gauge | The queue's revision: every change to a job raises it. |
| `encode_queue_jobs` | gauge | Jobs by priority and state. |
| `encode_queue_workers` | gauge | Workers by state (idle, working, paused, offline). |
| `encode_queue_worker_connected` | gauge | 1 while the worker called within three presence intervals. |
| `encode_queue_worker_paused` | gauge | 1 while the worker is paused. |
| `encode_queue_worker_jobs` | gauge | The worker's jobs now: leased (an active lease) and checking (a result waiting for the pipeline). |
| `encode_queue_worker_attempts_total` | counter | Attempts the worker ended, by outcome: accepted, rejected, superseded, failed, given_back, expired, preempted, cancelled. |
| `encode_queue_worker_leases_expired_total` | counter | Leases of the worker that ran out or whose worker went offline. |
| `encode_queue_worker_results_rejected_total` | counter | Results of the worker that the pipeline rejected. |
| `encode_queue_worker_fps` | gauge | Frames per second the worker reported last, summed over its active leases; 0 when idle. |
| `encode_queue_worker_speed` | gauge | Times real time the worker reported last, summed over its active leases; 0 when idle. |
| `encode_queue_worker_last_seen_timestamp_seconds` | gauge | When the worker last called, in Unix seconds. |
| `encode_queue_worker_tool_info` | gauge | The tool versions the worker runs, as labels; always 1. |
| `encode_queue_worker_info` | gauge | The worker's version, OS, CPU model, architecture and the app it runs inside (host_app, host_app_version), as labels; always 1. |
| `encode_queue_worker_cpu_threads` | gauge | Logical CPUs the worker can use. |
| `encode_queue_worker_cpu_threads_in_use` | gauge | The encoder's thread limit: the worker's cpu_threads limit, or all its CPUs. |
| `encode_queue_worker_cpu_threads_busy` | gauge | Threads the worker's own tools kept busy since its last presence. |
| `encode_queue_worker_memory_bytes` | gauge | The worker's memory. |
| `encode_queue_worker_memory_used_bytes` | gauge | The worker's memory in use, from its last presence. |
| `encode_queue_worker_transfer_bytes_per_second` | gauge | Moving average of the worker's transfer rate through the tunnel, by direction (download: sources, upload: results). |
| `encode_queue_worker_relative_speed` | gauge | The worker's encoding speed against the reference worker on the same rung classes, a moving average, or from the calibration clip until it has measured results; 1 is as fast. |
| `encode_queue_worker_last_handshake_timestamp_seconds` | gauge | The worker's last WireGuard handshake, in Unix seconds. |
| `encode_queue_jobs_pinned` | gauge | Queued jobs the Encoder page sent to one worker, waiting for it. |
<!-- /metrics -->

The counters come from the queue's state, so they survive a restart; a removed worker's series
go with it.

## Development

```bash
go test -race ./...
```

The end-to-end tests (`internal/e2e`) run a fake pipeline against the pipeline API and real
workers through a real userspace tunnel on localhost, with the test binary standing in for
ffmpeg and whisper-cli: jobs end to end, preemption, a crashed worker's lease running out, the
pause, rejected and failed results, cancelling.

## Licence

encode-queue's own code (the queue and the worker) is MIT: [LICENSE](LICENSE).

The toolchain is not: ffmpeg and ffprobe are built with `--enable-gpl --enable-version3`, so
they are **GPLv3**, linked with x264 and x265 (GPLv2 or later), LAME and FriBidi (LGPL), libvmaf
(BSD-2-Clause-Patent), libass (ISC), FreeType (FTL or GPLv2 or later), and HarfBuzz, fontconfig
and Expat (MIT-style); whisper-cli is MIT, and the Inter fonts are OFL-1.1. From toolchain 2 on, every image and tarball carries
each licence and `SOURCES.md` in `licenses/`, and the `toolchain-<N>` release attaches the exact
source tarballs (the Corresponding Source). The worker calls ffmpeg as a separate program; it
links none of it.

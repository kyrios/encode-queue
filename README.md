# encode-queue

One queue for video encodes and whisper runs, and the worker that takes jobs from it, on any
Linux server or Mac. Built for iPhoneblog's `encoder` ([ADR-0007](https://github.com/iPhoneblog-web/iphoneblog-wordpress/blob/main/docs/adr/0007-encode-helpers.md)).

- **The queue server** (`encode-queue`) keeps the jobs, their priorities (stage 1, stage 2,
  archive), leases, attempts and the workers, on disk. It brings its own WireGuard: wireguard-go
  in userspace with its own network stack, one UDP port, no host interface, no network rights.
  The peer list is the queue's state.
- **The worker** (`encode-worker`) pairs by its public key, long-polls for a job, fetches the
  source with range requests, runs ffmpeg (libx265, libx264) or whisper.cpp at `nice 19`, reports
  progress and log lines, and uploads the result over tus.
- **The pipeline judges.** The queue never checks quality: the pipeline accepts or rejects every
  result against ADR-0005's floors. The first accepted result wins.

## The APIs

| file | who calls it | where |
|---|---|---|
| [`api/pipeline-admin.yaml`](api/pipeline-admin.yaml) | the pipeline (submit, watch, judge) and the upload center's Encoder page (queue, workers, pairing) | the queue's TCP listener, with a bearer token each |
| [`api/helper.yaml`](api/helper.yaml) | every worker | inside the WireGuard tunnel only |
| [`api/worker-control.yaml`](api/worker-control.yaml) | the app that embeds the worker (Slipstream) | the worker's Unix socket |

All three are OpenAPI 3.1.

## How it behaves

- **Priorities**: `stage1`, then `stage2`, then `archive`, and a place within each, which the
  Encoder page can change. A worker gets the first job it may run.
- **Exact tool versions**: a worker gets a job only if its tools match the job's `requires`
  exactly, as text. Without `requires` a job requires the tools of the reference worker
  (`EQ_REFERENCE_WORKER`, `encoder`'s own worker container): ffmpeg, libx265 and libx264 for a
  rung, whisper.cpp for whisper (and ffmpeg when the source is not WAV), plus the model by its
  sha256 and the captions' font. Until the reference worker has said what it runs, only it gets
  jobs.
- **Leases** last 120 s and are renewed every 30 s with the progress; the long poll waits 25 s;
  presence every 30 s, offline after three missed. A lease that runs out, a worker that goes
  offline, a removed worker: the job goes back to `queued`, the next attempt with the next number.
- **Preemption**: a stage 1 or stage 2 job that no free worker may run ends the active lease
  with the lowest priority (then the youngest) whose worker may run it; that worker's next
  renewal answers `410 preempted`.
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
| `EQ_API_LISTEN` | the pipeline and admin API, `/metrics`, `/healthz`, e.g. `:8090`; required |
| `EQ_PIPELINE_TOKEN_FILE`, `EQ_ADMIN_TOKEN_FILE` | files holding the two bearer tokens (16 characters at least, different); required |
| `EQ_WG_LISTEN_PORT` | the UDP port WireGuard listens on; required |
| `EQ_WG_ADDRESS` | the queue's address in the tunnel; required |
| `EQ_WG_SUBNET` | the tunnel's subnet, from which workers get their addresses; required |
| `EQ_WG_API_PORT` | the helper API's port inside the tunnel; required |
| `EQ_WG_ENDPOINT` | `host:port` the workers send their UDP to (what `iron` forwards); required |
| `EQ_WG_PRIVATE_KEY_FILE` | default `$EQ_STATE_DIR/wireguard.key`, made on first start |
| `EQ_WG_KEEPALIVE_S` | the keepalive workers get; default 25 |
| `EQ_REFERENCE_WORKER` | the name of the worker whose tools jobs require by default |
| `EQ_MIN_APP_VERSION` | workers below it get `426` |
| `EQ_MAX_FAILURES` | default 3 |
| `EQ_RESULT_RESERVE_BYTES` | default 2 GiB |
| `EQ_LEASE_S`, `EQ_RENEW_EVERY_S`, `EQ_LONG_POLL_S`, `EQ_PRESENCE_EVERY_S`, `EQ_PAUSE_GIVE_BACK_S`, `EQ_DONE_KEEP_S` | the timings, defaults 120, 30, 25, 30, 60 and 7 days |
| `EQ_LOG_LEVEL` | `debug`, `info` (default), `warn`, `error` |

`encode-queue pubkey` prints the queue's public key (making the key if there is none).
SIGTERM drains: no new leases (`503 draining`), the long polls end, the state is written.

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
| `EQ_WHISPER_VERSION` | the whisper.cpp release the binary is (`v1.9.5` in the image) |
| `EQ_WHISPER_MODELS_DIR` | the `*.bin` models, found by sha256 (`/models` in the image) |
| `EQ_FONTS` | font families to announce besides fontconfig's, comma-separated |
| `EQ_CPU_THREADS` | x265's pool (`pools=N`) and whisper.cpp's threads; default the tools' own |
| `EQ_GPU` | whisper.cpp may use the GPU; default false |
| `EQ_MAX_PARALLEL_JOBS` | default 1 |
| `EQ_CONTROL_SOCKET` | a Unix socket for [worker-control.yaml](api/worker-control.yaml) |
| `EQ_LOG_LEVEL` | as the queue's |

`encode-worker versions` prints what the worker runs, exactly as the queue will compare it. The
worker builds ffmpeg's arguments itself from the job's structured settings, in the order
infra's `video-encode` did (a test compares them with `video-encode`'s own), so a worker's file
is the file `encoder` made before the split.

### The images

- `ghcr.io/kyrios/encode-queue`: the queue, on distroless.
- `ghcr.io/kyrios/encode-worker`: the worker on Debian 13 with Debian's ffmpeg, libx265 and
  libx264, `fonts-inter`, and whisper.cpp built from `v1.9.5` (commit `d1be6fd`, as infra's
  `encoder_whisper_version` and `encoder_whisper_commit`) for x86-64-v3. Models are not in the
  image; mount them at `/models`.
- The macOS arm64 worker is a release asset (`encode-worker-darwin-arm64`), for Slipstream to
  embed and sign.

Images are tagged with the release (`v0.1.0`) and pinned by Ansible.

## Metrics

`GET /metrics` on the API listener, without a token. Per-worker series carry `worker_id` and
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

MIT.

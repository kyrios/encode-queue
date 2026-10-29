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
  source with range requests, runs ffmpeg (libx265, libx264) or whisper.cpp, a normal job's at
  `nice 19` and a realtime job's at the worker's own (below, "Realtime before normal"), reports
  progress and log lines, and uploads the result over tus. Jobs are rungs, whisper runs, from
  v0.2.8 cuts, remuxes and checks (every encode and every decode runs on a worker), from
  v0.2.16 align jobs, which time a text's words in its sound with Qwen3-ForcedAligner, and from
  v0.2.22 the sound's peaks and a video's filmstrip for Funkhaus's timeline, and from v0.2.24
  its loudness in LUFS.
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
  page's "Sofort") is for a job someone in Funkhaus waits for, and runs, as `stage1` does, in a
  worker's **high-priority slots** beside other work (below).
- **Normal and high-priority slots** (v0.2.14; Thorsten, 2026-10-28, after a `now` whisper job
  waited over 40 minutes while every worker's one slot held a 2160 archive encode). A worker's
  normal slots (`max_parallel_jobs`, `EQ_MAX_PARALLEL_JOBS`) take every job; its high-priority
  slots (`high_priority_slots`, `EQ_HIGH_PRIORITY_SLOTS`, on top of them) take only `now` and
  `stage1`, so no archive or stage 2 job ever holds one. Both are 1 by default; Slipstream's
  settings set them. A worker before 0.2.14 has one high-priority slot, its v0.2.5 extra slot. A
  claim beside a job in the other kind of slot, and a high-priority one beside any, needs the RAM:
  memory (in a container, its cgroup's limit where that is lower, v0.2.15: encoder's worker runs
  with `mem_limit` 10g on 14 GiB) less the last presence's use, less the expected peak of each lease claimed since (one
  taken back since included, as its tool runs until the next renewal), less 1 GB, must hold the
  job's expected peak (a 4K `slower` encode 5.5 GB, whisper turbo or medium 2 GB, measured on
  encoder; an align job 3.5 GiB on a Mac's MLX, 3 GiB on the CPU, v0.2.16).
  Two normal slots side by side need none. Where only the RAM keeps a `now` job out of a free
  high-priority slot and no worker has a slot for it, it takes that worker's lowest lease below
  `stage1` back, one at a time, and the worker's claims wait for its next measured presence:
  on `encoder` (8 GB) a 4K `slower` archive encode and whisper medium do not fit together. A
  `stage1` job does not wait for a worker busy with a lower job, and the expected completion time
  splits a worker's threads evenly among the jobs that run on it. A worker's `slots` and each
  job's `slot` are in `GET /v1/workers` for the Encoder page, and in `encode_queue_worker_slots`
  and `_slots_busy` for the console.
- **Exact tool versions**: a worker gets a job only if its tools match the job's `requires`
  exactly, as text. Without `requires` a job requires the tools of the reference worker
  (`EQ_REFERENCE_WORKER`, `encoder`'s own worker container): ffmpeg, libx265 and libx264 for a
  rung, whisper.cpp for whisper (and ffmpeg when the source is not WAV), plus the model by its
  sha256 and the captions' font. A job with burnt-in captions also requires the reference's
  libass (v0.2.7; toolchain 3 builds it on both platforms, reported from its versions.txt), and
  the worker lists fonts with the toolchain's fc-list and points `FONTCONFIG_FILE` at its
  fonts.conf (Inter only) unless something set it. libass gets the captions by their name in the
  lease's folder, where ffmpeg runs, so any work dir works (v0.2.14: a Mac's sits under
  "Application Support"). Until the reference worker has said what it
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
  tells each worker which models to hold (`wanted_models`): small for caption timing and
  large-v3-turbo, the one caption model on every worker (v0.2.14; Thorsten, 2026-10-28), wherever
  its RAM holds turbo's 2 GB peak with 1 GB to spare. The speed decides only which worker gets a
  job, never what it holds.
  The worker fetches those from ggerganov/whisper.cpp at a pinned revision only, checks their
  sha256 and times them on the calibration's speech clip.
- **The align job** (v0.2.16; video-pipeline's `docs/aligner-timing.md`, approved by Thorsten on
  2026-10-28): `Qwen/Qwen3-ForcedAligner-0.6B` at revision `c7cbfc20` gives every word of a text
  that already exists its time in the sound: the Socials version's burnt-in captions and the
  proofread captions, which whisper `small` with the pipeline's matching and `proofread.align`
  timed before (93 to 97 % of speech onsets met within 0.2 s against 74 to 90 %). Audio and the
  words in, whisper-cli's `-oj` JSON out, one segment per word (helper.yaml's `AlignParams`).
  **A Mac first, encoder's CPU as the fallback** ("The macs are not always there. We need a
  fallback that works", Thorsten): a Mac runs it through MLX and `mlx-qwen3-asr`, which the
  worker sets up itself (python-build-standalone's CPython 3.12.12 and the pinned wheels, each
  by its sha256, in `STATE/aligner`, 357 MB, 18 s); the Linux image brings PyTorch for the CPU and
  `qwen-asr`. A worker with the CPU engine leaves a job to a connected Mac that may run it, as a
  worse whisper model does. Pieces of at most 30 s, cut between words by the times the text
  already has; MLX's buffer cache is freed after each (without it a 21-minute episode grew to 24
  GiB). Measured: a Mac Studio 1 % of the audio's length plus 1.4 s to load, 3.0 to 3.4 GiB;
  encoder's six cores in float32 16 % plus 6 s, 4.4 to 4.9 GB, and where 6 GiB are not free
  beside what runs (a 4K archive encode) `lean`, bfloat16 weights with float32 products, 58 % and
  2.9 GB with the weights' pages, the same start as float32 for 1635 of 1641 words. The model is a folder of
  eight files, `qwen3-forced-aligner-0.6b-c7cbfc20`, in the models folder, each file by its
  sha256; its sha256 is their manifest's (`32f47d21…`). A worker fetches it where the folder is
  writable; encoder's is filled by infra.
- **The peaks and strip jobs** (v0.2.22; Thorsten, 2026-10-28: "0 encoding/calculating/handling
  of media is happening outside of Slipstream instances. Funkhaus is 'just' the job manager,
  event manager, file manager and UI"): the two things video-pipeline still decoded itself are
  worker jobs. `peaks` reads an upload's first sound with ffmpeg, mono at 8 kHz, and writes
  upload-center's peaks file for its waveform, byte for byte as its `makePeaks` does (the golden
  file `internal/worker/testdata/peaks-lcg-3s.peaks`); a source without sound fails at once with
  `decode_failed`. `strip` makes a video into the timeline's filmstrip from its keyframes alone
  (video-pipeline#29's spec); since v0.2.23 the upload itself, so the frames are there while Alex
  trims: an iPhone's 10-bit HEVC too, its pictures 8-bit before their colours are measured, and
  decoded with slice threads only (a 4K keyframe in 21 ms at 272 MB, where ffmpeg's frame
  threads took 737 MB), and since v0.2.27 over every frame when an HEVC with an open GOP and
  keyframes far apart (a screen recording that hardly moves) decodes them out of order: a
  `scenes` level of at most 32 pictures at the cuts,
  a `fine` level every 4 s, JPEG sheets named by their content and thumbs.json with 128
  placeholder colours, delivered as one tar. Someone in Funkhaus waits for both, so they are
  realtime: priority `now` when the pipeline names none, first in line and in a worker's
  high-priority slot beside a long encode, which they never preempt. Each takes seconds for an
  hour (a two-hour file: peaks 10 s, strip 13 s, 71 and 101 MB at the peak, counted as 512 MiB),
  at most 64 MiB of result, and earns no points. Only workers of 0.2.22 with the reference's
  ffmpeg get them.
- **The loudness job** (v0.2.24; Thorsten, 2026-10-28, idea 6 of the NLE research): how loud an
  upload is by EBU R 128, for Funkhaus's Checks ("zu leise / passt / zu laut" against -16 LUFS
  for podcasts and Folgen, -14 for clips) and a lane on its waveform. ffmpeg's
  `ebur128=peak=true` reads the first sound; its log goes to an FFREPORT file and gives the
  integrated loudness, the loudness range, the true peak and the short-term loudness every
  second, as a small JSON (helper.yaml's `LoudnessParams`; the golden file
  `internal/worker/testdata/loudness-tone.json`). EBU Tech 3341's tone reads -23.0 LUFS. As the
  peaks: realtime by default, `decode_failed` at once without sound, a worker of 0.2.24 with the
  reference's ffmpeg, no points; type "Lautheit".
- **Leases** last 120 s and are renewed every 30 s with the progress; the long poll waits 25 s;
  presence every 30 s, offline after three missed. A lease that runs out, a worker that goes
  offline, a removed worker: the job goes back to `queued`, the next attempt with the next number.
- **Preemption**: a stage 1 or stage 2 job that no free worker may run ends an active lease of a
  lower priority whose worker may run it; that worker's next renewal answers `410 preempted`.
  Since v0.2.15 a lease with under 20 minutes to go by its progress, or over 80 % of its frames
  done, is never taken back, and the new job waits for it (Thorsten, 2026-10-28: queue v0.2.12 threw
  away a 2160 archive encode 95 % done after five hours for the smoke test's stage 2 job). Of the
  others the one that ran the shortest goes, whatever its priority. The log's `preempt` line says
  the minutes and frames lost, and `encode_queue_worker_preempted_work_seconds_total` and
  `_preempted_frames_total` count them.
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
  Normal slots are at most `EQ_MAX_PARALLEL_JOBS` (2), high-priority ones on top at most
  `EQ_MAX_HIGH_PRIORITY_SLOTS` (2, v0.2.14). A
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
- **Points** (v0.2.15; Thorsten, 2026-10-28): every job the pipeline accepts earns its worker
  points from a frozen table, **Punkte-Tabelle v1**. One point is one minute of the Mac Studio's
  work by its calibration of 2026-10-28, and a job earns the same on every machine, linear in its
  frames or seconds of audio:

  | class | the Mac Studio's speed | points |
  |---|---|---|
  | libx265 3840x2160 ultrafast | 48.42 fps | 1 / (fps × 60) a frame |
  | libx265 1920x1080 veryfast | 99.63 fps | ″ |
  | libx265 1280x720 veryfast | 131.05 fps | ″ |
  | libx264 1280x720 veryfast | 271.00 fps | ″ |
  | libx265 3840x2160 slower | 2.284 fps | ″ (a 2-hour episode at 25 fps: 1314) |
  | libx265 1920x1080 slow | 29.17 fps | ″ |
  | libx265 1280x720 slow | 57.47 fps | ″ |
  | libx264 1280x720 slow | 188.31 fps | ″ |
  | whisper large-v3-turbo | 12.76 × real time | 1 / (speed × 60) a second of audio (an hour: 4.7) |
  | whisper small | 32.34 × | ″ |
  | whisper medium | 12.93 × (2.5 times small's time, not timed there) | ″ |
  | align, Qwen3-ForcedAligner on MLX (v0.2.16) | 90.9 × (0.011 of the audio's length, #24) | ″ (an hour: 0.66) |
  | remux job of a 3840x2160 master (the copy and its check) | 39.0 fps | 1 / (fps × 60) a source frame (2 hours: 77) |
  | score check job against a 3840x2160 master | 65.8 fps | ″ (2 hours: 46) |

  Another size of a codec and preset in the table takes the nearest size's speed scaled by the
  pixels (the Clip's 1080x1920 at x264 `slow`: 188.31 / 2.25); a codec and preset outside it earns
  nothing. A cut is priced as an encode of its frames in its class (libx264 at the source's size
  and its preset, its length at the source's rate). A remux or check job pays its source's frames
  at the measured speed, scaled by the source's pixels, both measured on the Mac Studio with the
  worker's own commands (`TestMeasureRemuxAndCheck`, three runs on 2026-10-28). A job that names
  no frames counts its length at 25 fps; an audio check names no size and earns nothing. The
  queue prices a job from the pipeline's own figures,
  the source's `frames` or `duration_s` and the job's settings, never from the worker's (a whisper
  job without `duration_s` earns nothing). Only a result its worker delivered while it held the
  lease earns: nothing for a rejected, failed, given back, expired, preempted or moved attempt,
  and nothing for moving bytes. Points are counted and shown and never steer the scoring. Every
  worker started at zero. `GET /v1/workers` gives each worker `points` (`today` and `week` from
  Monday, both Europe/Berlin, and `total`) and `house` for encoder's own worker, each attempt its
  `points`; a presence's answer gives the worker its own and its place today among every paired
  worker (`rank` of `of`, equal points sharing a place); the metric is
  `encode_queue_worker_points_total`.
- **Late results** are taken and judged while the job is not done; the first accepted wins,
  later ones are `superseded`. A rejected or failed result excludes its worker from that job;
  after `max_failures` (3) the job is `failed`.
- **Pause** (v0.2.18; Thorsten, 2026-10-28: "Pause also wasn't working"): a paused worker claims
  nothing from that moment (a claim's long poll ends with the pause, and a lease whose answer
  crosses it goes back unstarted), and the queue hears it at once. "Sofort pausieren" (`now`)
  stops the running tools (SIGSTOP) and renews with `phase: paused`. A job of priority archive or
  stage 2, which nobody waits for, stays (`pause_hold`): x265 cannot resume elsewhere, and a 2160
  archive encode given back after three of its five hours on the Mac Studio loses three hours.
  Its hold time stops meanwhile, up to 24 h of pause, and the tool goes on where it stood. A now
  or stage 1 job goes back after 60 s as before, for another worker; the queue takes it back
  itself 30 s later if the worker does not. "Nach diesem Auftrag" (`after_job`) lets the running
  jobs finish; the queue never takes back a lease whose tool runs. A pause that runs out ends as
  "Fortsetzen" does.
- **Realtime before normal, by the system's scheduler** (v0.2.20; Thorsten, 2026-10-28: "Instead
  of throttling manually can we simply use the OS task scheduler and give the realtime jobs a
  slightly higher CPU priority?"). A job of priority `now` is realtime; a job of stage 1, stage 2
  or archive is normal, keeps its lease and its progress, and every idle cycle. On Linux a normal
  job's tools start at nice 19 and stay there, a realtime job's at the worker's own nice, 0: CFS
  weighs them 15 to 1024, so the realtime tool gets 98.5 % of a CPU both want. On macOS a normal
  job's tools also start under the utility QoS clamp (`taskpolicy -c utility`), and go into the
  background QoS (the efficiency cores) while a realtime job runs on the worker, out again when the
  last one ends; a realtime job's start unclamped. **The worker no longer renices itself**, since
  an unprivileged process cannot lower a nice value again: start it at nice 0. Slipstream starts it
  at nice 19, so there a realtime tool runs at 19 too, and the QoS makes the difference.
- **The efficiency cores** (v0.2.18; Thorsten, 2026-10-28: "Run archive encodes only on
  efficiency cores"). A Mac's worker may run every job (`efficiency: all`, Slipstream's "Nur
  Effizienzkerne") or only archive jobs (`archive`) in macOS's background QoS (PRIO_DARWIN_BG, as
  `taskpolicy -b`), which keeps all their threads on the efficiency cores, so the performance
  cores stay with the person at the Mac. It reaches a running tool at once and goes the same way;
  the tool keeps the limit's threads, so it has every core again once taken out. The worker sends
  the mode and its estimate of the speed there, `efficiency_speed` = e·r / (p + e·r) for p
  performance and e efficiency cores, r the ratio measured below. The ECT divides such a job's run
  time by it, and the lease's hold grows with it: a job on the efficiency cores is neither left to
  the Mac by mistake nor taken back as overdue. A lowered `cpu_threads` lengthens the holds of the
  worker's running leases too (by the ratio of the threads), since the throttle slows them. The
  points do not change: they price the job, not the time. Measured on the Mac Studio (M2 Max,
  8 P and 4 E cores) with nothing else heavy running, Slipstream's ffmpeg 9.0.2 at nice 19, x265
  slower CRF 29 with `pools=12`, 180 frames of the calibration graph at 1920x1080, by turns:

  | run | normal | `taskpolicy -b` |
  |---|---|---|
  | 1 | 26.2 s | 296.7 s |
  | 2 | 30.2 s | 282.3 s |
  | 3 | 28.2 s | 314.5 s |
  | mean | 28.2 s, 6.4 fps | 297.8 s, 0.60 fps |

  10.6 times as long, so r = 0.21 and the Mac Studio's `efficiency_speed` is 0.095. That is the
  idle Mac's figure. The efficiency cores are macOS's own too: its background daemons ran beside
  the encode, which used between 0 and 310 % of a core from second to second. And macOS gives a
  background job only what the Mac leaves over: beside other work (load averages 80 to 140,
  **The CPU limit** above) a 4K slower encode in the background got 0.07 cores over a minute and
  no frame. Such a job runs almost only while the Mac does nothing else, which the estimate
  cannot know; Slipstream says so beside the setting. Every run's file has its 180 frames.
- **The CPU limit** (v0.2.17; Thorsten, 2026-10-28: Slipstream said 3 of 12 cores while a 2160
  archive encode, started with `pools=12`, ran at 978 % for hours). The worker's `cpu_threads`
  (`EQ_CPU_THREADS`, Slipstream's `limits` line) is x265's pool and whisper.cpp's threads when a
  tool starts, and also the cores a running tool may use. A tool that uses more than the limit
  plus half a core runs in a duty cycle, as cpulimit does: in each 100 ms it runs for a share and
  stands still for the rest (SIGSTOP and SIGCONT to its process group). The share is the limit
  over the cores it uses while it runs, from its CPU time; each stop lasts in proportion to the
  run as it was, so a late wake-up at nice 19 costs no extra share. It then averages the limit
  and loses no work. A higher limit, or none, frees it within a period; a pause's SIGSTOP wins.
  A tool started at the limit runs free, and x264, which keeps its own threads, is held the same
  way. The job's log says when a tool is held and freed, Slipstream's `limits` event says it, and
  the worker tells the queue a new limit at once, not with its next presence. Measured on the Mac
  Studio (M2 Max, 8 P and 4 E cores, at nice 19 beside other work at load averages 80 to 140),
  the 2160 archive rung on the calibration graph as a 3840x2160 master, a minute each
  (`TestMeasureThrottle`):

  | the encode | ps %cpu each second: median (range) | cores by CPU time | fps |
  |---|---|---|---|
  | `pools=3`, free | 296 (276 to 316) | 2.95 | 1.13 |
  | `pools=12`, held to 3 | 306 (195 to 405) | 2.99 | 0.83 |
  | `pools=12`, `taskpolicy -b` | 0 (0 to 32) | 0.07 | 0.00 |
  | `pools=12`, free | 917 (568 to 1099) | 9.11 | 3.01 |

  macOS's own means hold no number of cores: the background policy (`taskpolicy -b`,
  PRIO_DARWIN_BG) keeps a tool on the efficiency cores and, on a busy Mac, all but stops it. A
  held encode does less per core than one started at the limit, since x265's twelve frame
  threads wait on each other; the next job starts with the limit. A 4 s clip encoded whole while
  held to 3 and freed every 5 s has its 102 frames, decodes without errors and has the free
  encode's SSIM against the source, 0.98567; its bytes differ, as two free encodes' bytes differ
  (x265's frame threads and VBV). whisper-cli with 12 threads on the CPU, held to 3, used 2.95
  cores and wrote the free run's transcript byte for byte; with the GPU it uses under one core and
  runs free.
- **A result does not wait for its verdict forever** (v0.2.26; 2026-10-29: three strip jobs sat in
  `checking` from 23:16 on, after the pipeline restarted and lost the threads that waited for
  them). A job whose last result has waited `EQ_CHECK_TIMEOUT` (2 hours) for its verdict is
  cancelled by the queue, `by: queue` with `code: check_timeout` ("verfallen" on Funkhaus, under
  Abgebrochen): its result files go and free their room, a verdict that comes later answers
  `409 not_checking`, and the submitter may send the job again under its id.
  `encode_queue_check_timeouts_total` counts them.
- **Disk**: the queue leases only while its result directory has room for every active lease's
  `max_bytes` plus `EQ_RESULT_RESERVE_BYTES`. **A claim counts the leases of its own priority and
  higher; a lower one gets what is left** (v0.2.25; v0.2.19 did it for now and stage1 alone, after a
  `now` whisper job of 64 MB waited for hours beside an idle encoder, and on 2026-10-28 a stage 2 job
  then waited behind an archive lease's 8 GB with 10 MB written): free space less the reserve, the
  `max_bytes` of the running leases of its priority and higher, and what the lower ones' uploads
  still have to write. A 2160 archive rung's `max_bytes` is 2 × its source or 12.5 MB/s of its
  length, so archive never blocks stage 2, stage 2 never stage 1 or `now`, and three stage-1 rungs
  of one upload still count each other. A job the room keeps out says so: `waiting.code`
  `no_room`, and `encode_queue_result_room_bytes{priority}`.
  **A result then meets the disk when its upload starts** (v0.2.21): it must fit the free space
  less the reserve, what other uploads still have to write and the `max_bytes` of the active
  leases of a higher priority than its own that have not begun theirs (v0.2.25: every higher one,
  not only now and stage1), or the upload answers `503 no_room` and the
  worker asks again every 30 s, its lease held. A worker before 0.2.21 fails that attempt, as it
  would on a full disk.

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
| `EQ_MIN_APP_VERSION` | workers below it get `426` |
| `EQ_MAX_FAILURES` | default 3 |
| `EQ_MAX_PARALLEL_JOBS` | the most normal slots any worker gets, whatever it asks for; default 2 (v0.2.9) |
| `EQ_MAX_HIGH_PRIORITY_SLOTS` | the most high-priority slots any worker gets on top; default 2 (v0.2.14) |
| `EQ_HOLD_FACTOR`, `EQ_HOLD_MIN_S`, `EQ_HOLD_MAX_S` | how long a lease may be held: the factor times its expected completion time, between the two; defaults 3, 1800 and 43200 (v0.2.9) |
| `EQ_EXCLUDE_BACKOFF_S` | default 600: how long a job that every able worker is excluded from waits before the reference worker gets it again |
| `EQ_RESULT_RESERVE_BYTES` | default 2 GiB |
| `EQ_LEASE_S`, `EQ_RENEW_EVERY_S`, `EQ_LONG_POLL_S`, `EQ_PRESENCE_EVERY_S`, `EQ_PAUSE_GIVE_BACK_S` | the timings, defaults 120, 30, 25, 30 and 60 |
| `EQ_CHECK_TIMEOUT` | how long a result waits for its verdict before the queue cancels its job (`check_timeout`): seconds or `2h`; default 2 hours |
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
| `EQ_WHISPER_MODELS_DIR` | the `*.bin` models, found by sha256 (`/models` in the image), and the aligner's folder `qwen3-forced-aligner-0.6b-c7cbfc20` (v0.2.16); the models the queue wants and, where the worker has an aligner engine, the aligner are fetched into it, or into `STATE/models` without it. A read-only folder (encoder's, filled by infra) fetches nothing: a wanted model it lacks is `unavailable`, provided by the host, never a failure |
| `EQ_ALIGNER_PYTHON` | the Python of the aligner's engine (v0.2.16; the image's `/opt/aligner/python/bin/python3`). Unset on an Apple-silicon Mac, the worker sets up MLX in `STATE/aligner` itself; `off`: no align jobs |
| `EQ_CALIBRATE` | `false` skips the calibration at pairing (`true` by default) |
| `EQ_FONTS` | font families to announce besides fontconfig's, comma-separated |
| `EQ_HOST_APP_NAME`, `EQ_HOST_APP_VERSION` | the app the worker runs inside (Slipstream and its version), shown on the Encoder page; never compared with the queue's minimum |
| `EQ_CPU_THREADS` | x265's pool (`pools=N`) and whisper.cpp's threads, and the cores a running tool may use (v0.2.17, **The CPU limit**); default the tools' own |
| `EQ_GPU` | whisper.cpp may use the GPU; default false |
| `EQ_MAX_PARALLEL_JOBS` | normal slots, which take every job; default 1 |
| `EQ_EFFICIENCY` | `all` or `archive`: those jobs run on the efficiency cores only (macOS's background QoS, v0.2.18); default `off` |
| `EQ_HIGH_PRIORITY_SLOTS` | high-priority slots on top, which take only `now` and `stage1` jobs; default 1, 0 none (v0.2.14) |
| `EQ_CONTROL_SOCKET` | a Unix socket for [worker-control.yaml](api/worker-control.yaml) |
| `EQ_WORKER_KEY_FILE` | the private key; default `$EQ_WORKER_STATE_DIR/wireguard.key`, made only when missing, so a key Ansible puts there keeps the worker's identity |
| `EQ_LOG_LEVEL` | as the queue's |

Every variable is also a flag (`encode-worker -h`); the flag wins. `encode-worker healthcheck`
exits 0 when the queue answered within two minutes (the image's `HEALTHCHECK`). The image runs
as uid 10001 (`worker`) with its scratch space in `/var/lib/encode-worker/work`.

### Embedding (Slipstream)

```
encode-worker --key-stdin --tunnel-file … --state-dir … --name … --app-version x.y.z \
  --threads N --gpu true|false --jobs N [--high-slots N] [--efficiency off|all|archive] --ffmpeg … --ffprobe … --whisper-cli … \
  --whisper-models … --control stdio
```

- **Standard input**: the first line is the WireGuard private key in base64 (the app keeps it
  in the keychain). Then one JSON object per line: `{"cmd":"pause","until":"<RFC 3339>","mode":"now"}`
  (`mode` `now` or `after_job`, v0.2.18; `now` without one), `{"cmd":"resume"}`,
  `{"cmd":"limits","threads":N,"gpu":bool,"jobs":N,"high_slots":N,"efficiency":"off"}`
  (`efficiency` `off`, `all` or `archive`, v0.2.18),
  `{"cmd":"quit"}` (give every lease back with `reason=quit`, exit 0). The end of input quits too.
  A `limits` line without `high_slots` or `efficiency` keeps them; a worker before 0.2.14 ignores the field (and
  `EQ_HIGH_PRIORITY_SLOTS`), so an app may send both to any worker. A `limits` line counts at
  once: the queue hears it, the next tool starts with it, and a running tool is held to its
  `threads` (v0.2.17, **The CPU limit** above).
- **Standard output**: one JSON object per line, `event` naming it: `connection` (`up`,
  `last_answer_at`, `refused: [{kind, reason}]`), `job` (`lease_id`, `title`, `kind`, `rung`,
  `slot`: `normal` or `high`, `phase`, `fraction`, `fps`, `speed`, `eta_s`; at every phase change and once a second),
  `idle`, `result` (`lease_id`, `title`, `verdict`: `accepted`, `rejected` or `given_back`,
  `check`), `paused` (`until` or null, `mode`; v0.2.18), `update` (`latest`, when the queue wants a newer app),
  `tools` (`tools: [{name, version}]`), `points` (`table`, `today`, `week`, `total`, `rank`, `of`,
  when they or the place change; v0.2.15), `limits` (`threads` in effect, the machine's
  `cores`, `throttled`: how many running jobs are held to it, `efficiency` (v0.2.18); at the
  start, after each `limits` line and when a job is held or freed; v0.2.17), `log` (`lease_id`, `line`, and
  from v0.2.21 `level`: `DEBUG`, `INFO`, `WARN` or `ERROR`, slog's words), `calibrating` (`classes`,
  when the calibration clip starts) and `calibrated` (`classes`, when it is done), and `model`
  for each whisper model the queue wants (`name`, `sha256`, `state`: `wanted`, `downloading`,
  `checking`, `calibrating`, `ready`, `failed`, or `unavailable` when the models folder is read-only and the host provides the models; `bytes`, `bytes_done` while downloading,
  `speed` in times real time once timed, `reason` when it failed); the aligner's folder is
  `Qwen3-ForcedAligner-0.6B` among them (v0.2.16). `aligner` (v0.2.16): the aligner's engine,
  `state` `downloading`, `installing`, `ready` or `failed`, with `engine`, `bytes`, `bytes_done`
  and `reason`.
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
  toolchain, and from v0.2.16 the aligner's engine in `/opt/aligner`: python-build-standalone's
  CPython 3.12.12 with PyTorch 2.15.0 for the CPU and qwen-asr 0.0.6, every package by its hash
  (`internal/aligner/requirements-torch*.txt`): 1.8 GB more in the image (223 MB before),
  0.52 GB more to download. Models are not in
  the image; mount them at `/models` or point `EQ_WHISPER_MODELS_DIR` at a read-only mount. Up to v0.2.2 this image was
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
| `encode_queue_check_timeouts_total` | counter | Jobs the queue cancelled because their result waited longer than EQ_CHECK_TIMEOUT for a verdict (check_timeout). |
| `encode_queue_result_room_bytes` | gauge | Bytes of results a job of the priority can still claim: beside the max_bytes of the running leases of its priority and higher, the lower ones' uploads in flight and the reserve. |
| `encode_queue_jobs` | gauge | Jobs by priority and state. |
| `encode_queue_workers` | gauge | Workers by state (idle, working, paused, offline). |
| `encode_queue_worker_connected` | gauge | 1 while the worker called within three presence intervals. |
| `encode_queue_worker_paused` | gauge | 1 while the worker is paused. |
| `encode_queue_worker_jobs` | gauge | The worker's jobs now: leased (an active lease) and checking (a result waiting for the pipeline). |
| `encode_queue_worker_slots` | gauge | The worker's slots by kind: normal (every job) and high (priority now and stage1 only), as the queue gives them. |
| `encode_queue_worker_slots_busy` | gauge | The worker's slots that hold an active lease, by kind. |
| `encode_queue_worker_preempted_work_seconds_total` | counter | How long the worker's leases that preemption took back had run: work thrown away. |
| `encode_queue_worker_preempted_frames_total` | counter | Frames those leases had done. |
| `encode_queue_worker_points_total` | counter | Points the worker's accepted results earned, by Punkte-Tabelle v1: one point is a minute of the Mac Studio's work. |
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

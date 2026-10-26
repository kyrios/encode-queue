# encode-queue

One queue for video encodes and whisper runs, and the worker that takes jobs from it, on any
Linux server or Mac. Built for iPhoneblog's `encoder` ([ADR-0007](https://github.com/iPhoneblog-web/iphoneblog-wordpress/blob/main/docs/adr/0007-encode-helpers.md)).

- **The queue server** keeps the jobs, their priorities (stage 1, stage 2, archive), leases,
  attempts and the workers. It brings its own WireGuard (wireguard-go in userspace with its own
  network stack): one UDP port, no host interface, no network rights.
- **The worker** pairs by its public key, long-polls for a job, fetches the source, runs
  ffmpeg (libx265, libx264) or whisper.cpp at `nice 19`, and uploads the result over tus.
- **The pipeline judges.** The queue never checks quality: the pipeline accepts or rejects
  every result.

## The APIs

| file | who calls it | where |
|---|---|---|
| [`api/pipeline-admin.yaml`](api/pipeline-admin.yaml) | the pipeline (submit, watch, judge) and the upload center's Encoder page (queue, workers, pairing) | the queue's TCP listener, with a bearer token |
| [`api/helper.yaml`](api/helper.yaml) | every worker | inside the WireGuard tunnel only |

Both are OpenAPI 3.1.

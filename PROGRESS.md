# Progress Tracker

Single source of truth for build state. **Update this file in the same change
that lands the work** — see [AGENTS.md](AGENTS.md).

Legend: `[ ]` not started · `[~]` in progress · `[x]` done (gate green) · `[!]` blocked

Every phase closes on a **gate**: a command you can actually run. A phase is not
done because the code exists; it is done when the gate passes.

**Each checkbox below is roughly one commit** — see *Commit granularity* in
[AGENTS.md](AGENTS.md). A phase is never one commit; a checkbox that outgrows one
commit splits into two.

| Phase | Title | State | Gate |
|---|---|---|---|
| 0 | Config baseline & docs | `[x]` | `ls -l CLAUDE.md` resolves to `AGENTS.md` |
| 1 | Infra skeleton | `[~]` | `make up && make smoke` ✅ passing |
| 2 | Shared contracts library | `[x]` | `make unit` ✅ 72 tests |
| 3 | Upload path | `[ ]` | `make integration ARGS="-k upload"` |
| 4 | Probe stage | `[ ]` | `make integration ARGS="-k probe"` |
| 5 | Transcode workers | `[ ]` | `make integration ARGS="-k transcode"` |
| 6 | Read model & projector | `[ ]` | `make integration ARGS="-k projector"` |
| 7 | SSE gateway | `[ ]` | `make integration ARGS="-k sse"` |
| 8 | Frontend | `[ ]` | `make e2e ARGS="-k upload_flow"` |
| 9 | Thumbnails, HLS & completion join | `[ ]` | `make e2e ARGS="-k hls"` |
| 10 | Observability | `[ ]` | `make obs-verify` |
| 11 | Notify & failure UX | `[ ]` | `make e2e ARGS="-k failure"` |
| 12 | Production hardening | `[ ]` | `make ci` from a clean clone + `make security-verify` |
| 13 | Deployment | `[ ]` | `make deploy-staging && make e2e BASE_URL=<staging>` |

---

## Phase 0 — Config baseline & docs `[x]`

- [x] `PLAN.md` — architecture, topology, scope reasoning
- [x] `PROGRESS.md` — this file
- [x] `AGENTS.md` + `CLAUDE.md` symlink
- [x] ADRs 0001–0015 + index

**Gate:** `ls -l CLAUDE.md` shows `CLAUDE.md -> AGENTS.md`. ✅

## Phase 1 — Infra skeleton `[~]`
Refs: ADR-0002, ADR-0006, ADR-0007, ADR-0014, ADR-0015

- [x] `docker-compose.yml`: Kafka in **KRaft mode** (no ZooKeeper), Postgres 16, MinIO
- [x] `docker-compose.obs.yml`: Prometheus, Grafana, Tempo, OTel Collector, kafka-exporter
- [x] Topic bootstrap job — partitions/RF per the ADR-0002 table, idempotent re-run
- [x] MinIO bucket + lifecycle policy bootstrap
- [x] `Makefile`: `up down logs topics smoke unit integration e2e lint ci`
- [x] Alembic baseline migration (empty)
- [x] `pyproject.toml` + `uv.lock`, ruff/mypy/pre-commit config (ADR-0014)
- [x] `.env.example` documenting every variable
- [x] `pydantic-settings` module — **landed in Phase 2** as one shared
      `libs/pipeline/settings.py` rather than one module per service.
- [x] `/healthz` (no dependency checks) and `/readyz` (checks deps) — **landed in
      Phase 2** as `libs/pipeline/health.py`, so every service gets probes by
      construction rather than by copy-paste.
- [ ] Multi-stage images: non-root user, pinned ffmpeg in the worker image only —
      deferred again, now to **Phase 3**: still nothing to containerise.

**Gate:** `make up && make smoke` — **PASSING**, verified from a clean slate
(`make down` wiping volumes, then `up`, then `smoke`) on 2026-08-26:

```
kafka
  PASS  external listener reachable on localhost:29092
  PASS  topic list readable
  PASS  7 declared topics exist with the right partition counts
  PASS  retry and DLQ topics exist
  PASS  auto topic creation is disabled
postgres
  PASS  accepting connections on localhost:5432
  PASS  query executes
minio
  PASS  health endpoint live
  PASS  bucket 'videos' exists
  PASS  CORS preflight allows browser origin http://localhost:5173

SMOKE PASSED — Kafka, Postgres and MinIO are usable
```

27 topics created from `infra/topics.json` (7 declared + retry tiers + DLQs).
Re-running `make topics` reports `0 created, 27 already present` — the idempotent
path is the one that runs every day, so it is verified rather than assumed.

`make obs-up` also verified (not part of the gate, but an unrun compose file is a
liability): all 8 containers healthy, Grafana provisioned with both datasources,
OTel collector accepting OTLP on 4317/4318, and **kafka-exporter scraping for
real** — Prometheus reports `kafka_brokers=1` and 27 topics, so the consumer-lag
panel of ADR-0010 will have data the moment a consumer group exists. The `api`
and `workers` scrape targets read DOWN, as intended until Phase 3.

The phase stays `[~]` rather than `[x]`: the gate passes, but three checkboxes
presuppose services that do not exist. They move to Phase 2 rather than being
ticked on a technicality.

### What actually went wrong here (worth remembering)

- **Kafka bound to `kafka:9092` instead of `0.0.0.0`.** The container was
  healthy-looking but nothing inside it could reach the broker over localhost —
  the healthcheck and every CLI tool failed. Fixed in `0bda6ca`.
- **MinIO rejects `AbortIncompleteMultipartUpload` lifecycle rules** and has no
  `mc` flag for it; it expires stale uploads server-side instead. Real S3 does
  want that rule, so Phase 13 must add it or abandoned parts bill forever. Noted
  in ADR-0006.
- **`make down` referenced `docker-compose.obs.yml` before it existed**, so the
  whole gate chain aborted silently. Ordering matters in a Makefile that spans
  two compose files.

## Phase 2 — Shared contracts library `[x]`
Refs: ADR-0003, ADR-0004, ADR-0005, ADR-0009, ADR-0010, ADR-0015

- [x] `libs/pipeline/events.py` — Pydantic envelope: `event_id`, `video_id`,
      `occurred_at`, `schema_version`, typed payload per event
- [x] `libs/pipeline/topics.py` — topic names, keys, partition counts in one place
- [x] `libs/pipeline/producer.py` — idempotent producer (`enable.idempotence=true`,
      `acks=all`), OTel `traceparent` injected into headers
- [x] `libs/pipeline/consumer.py` — the long-poll/pause loop of ADR-0004, reusable
- [x] `libs/pipeline/retry.py` — retry topic + backoff + DLQ routing
- [x] `libs/pipeline/storage.py` — S3/MinIO client, key builders, presign helpers
- [x] `libs/pipeline/obs.py` — metrics registry, OTel tracer bootstrap
- [x] `libs/pipeline/settings.py` — carried from Phase 1. One shared module rather
      than one per service: every service reads the same variables, and a second
      copy is how two services end up disagreeing about a default.
- [x] `libs/pipeline/health.py` — carried from Phase 1. `/healthz`, `/readyz` and
      `/metrics` on a side port, so a worker gets probes without a web framework.
- [ ] Multi-stage images (non-root, ffmpeg only in the worker image) — **still
      deferred**, now to Phase 3. There is still nothing to containerise.

**Gate:** `make unit` — **PASSING**, 72 tests, plus `make lint` (ruff + mypy
strict on `libs/pipeline`) clean.

Includes the two the gate names explicitly: the envelope round-trips, and a
payload carrying an unknown field still parses — the forward compatibility that
lets a new producer deploy before its consumers (ADR-0003).

### The tests worth knowing about

The ADR-0004 eviction loop is unit-tested, not just described. `ConsumerProtocol`
is narrow enough for a fake, so `make unit` proves: partitions are paused before
the handler runs, `poll()` keeps being called *during* the handler, the
assignment captured at pause is the one resumed, a message delivered while
paused is stashed rather than dropped, and failures are produced and flushed
**before** the offset is committed.

`make unit` runs through `uv` — from the host when installed, otherwise in a
container, so a machine with only Docker can still run everything.

## Phase 3 — Upload path `[ ]`
Refs: ADR-0001, ADR-0006

- [ ] `POST /videos` → row in `videos` (status `awaiting_upload`) + presigned PUT
- [ ] `POST /videos/{id}/complete` → verify object exists, emit `video.uploaded`
- [ ] Size/content-type limits enforced in the presign policy, not in the app
- [ ] `GET /videos`, `GET /videos/{id}`
- [ ] OIDC bearer verification (JWKS), owner-prefixed object keys, per-user quota
      and rate limit — ADR-0015. Dev identity provider in compose.

**Gate:** `make integration ARGS="-k upload"` — PUT the fixture to MinIO via the
presigned URL, call `/complete`, assert exactly one `video.uploaded` message keyed
by `video_id`, and that a second `/complete` does **not** produce a second message.

## Phase 4 — Probe stage `[ ]`
Refs: ADR-0012 (conditional fan-out)

- [ ] `worker_probe`: ffprobe → duration, resolution, codecs, audio streams
- [ ] Ladder selection: never upscale — a 720p source yields 360p/480p/720p only
- [ ] Emit `video.probed` + one `rendition.requested` per selected rendition
- [ ] Emit `video.status` (`probed`, with the planned rendition list so the UI can
      render placeholders immediately)

**Gate:** `make integration ARGS="-k probe"` — a 640×360 fixture emits exactly the
sub-360p ladder and no 1080p request. Ladder selection also unit-tested with
synthetic probe JSON (no ffmpeg needed).

## Phase 5 — Transcode workers `[ ]`
Refs: ADR-0004, ADR-0005 — **the highest-risk phase**

- [ ] ffmpeg invocation per rendition, streamed to a temp file then uploaded
- [ ] `max.poll.records=1`, work on a thread, `pause()`/`poll()` heartbeat,
      `resume()` + manual commit on success
- [ ] Idempotency: skip if the output object already exists **and** the
      `(video_id, rendition)` row is `completed`
- [ ] Failure classification: retryable (transient S3/network) vs terminal
      (corrupt input) → retry topic vs DLQ
- [ ] Size-check before `storage.promote()`: single-part `copy_object` caps at
      5 GB on real S3 but not on MinIO, so large renditions pass every local
      test and fail in production (ADR-0006). Use multipart copy above the cap.
- [ ] Emit `rendition.completed` + `video.status`; emit `pipeline.failed` on DLQ

**Gate:** `make integration ARGS="-k transcode"`, covering:
1. a transcode longer than `max.poll.interval.ms` completes **without** a
   rebalance (assert no duplicate output, no repeated consumption);
2. a deliberately redelivered message produces one object and one DB row;
3. a corrupt input lands in the DLQ with the failure reason in its headers.

## Phase 6 — Read model & projector `[ ]`
Refs: ADR-0007

- [ ] Schema: `videos`, `renditions` (unique on `(video_id, rendition)`),
      `events` (append-only, for `Last-Event-ID` replay)
- [ ] `projector` consumes `video.status` (shared group) and upserts
- [ ] Offsets committed **after** the DB write; upsert makes replay safe

**Gate:** `make integration ARGS="-k projector"` — replay the same event batch twice,
assert identical final rows and no duplicate `renditions`.

## Phase 7 — SSE gateway `[ ]`
Refs: ADR-0008

- [ ] `GET /videos/{id}/events` — snapshot from Postgres first, then live deltas
- [ ] Each API instance consumes `video.status` with a **unique `group.id`**
- [ ] `Last-Event-ID` resume from the `events` table
- [ ] `:heartbeat` every 15 s; `X-Accel-Buffering: no`; clean disconnect teardown

**Gate:** `make integration ARGS="-k sse"` — (a) a client connecting *after* two
renditions finished still receives both, then the third live; (b) with two API
replicas, a client on replica A receives an event produced via replica B;
(c) reconnect with `Last-Event-ID` replays no duplicates.

## Phase 8 — Frontend `[ ]`

- [ ] Upload page: direct-to-MinIO PUT with progress, then `/complete`
- [ ] Video detail: rendition grid, placeholders from the probed ladder,
      each tile flipping to ready as its SSE event lands
- [ ] Reconnect/backoff on SSE drop; empty and error states

**Gate:** `make e2e ARGS="-k upload_flow"` — Playwright uploads the fixture and asserts
each rendition tile turns ready **without a page reload**.

## Phase 9 — Thumbnails, HLS & completion join `[ ]`
Refs: ADR-0013

- [ ] `worker_thumbnail`: poster + sprite sheet + WebVTT, off `video.probed`
- [ ] `worker_transcode` also emits HLS segments + per-rendition playlist
- [ ] `worker_package`: completion join — write `master.m3u8` only when every
      *expected* rendition is done, using the DB claim (`UPDATE … WHERE NOT
      packaged RETURNING`) so concurrent finishers elect exactly one packager
- [ ] Emit `video.completed`; player in the UI

**Gate:** `make e2e ARGS="-k hls"` — `master.m3u8` lists every rendition exactly once,
and a forced concurrent double-finish produces exactly one packaging run.

## Phase 10 — Observability `[ ]`
Refs: ADR-0010

- [ ] `/metrics` on api + every worker; Prometheus scrape config
- [ ] `kafka-exporter` wired; consumer-lag panel per group
- [ ] OTel spans in every stage, `traceparent` propagated via Kafka headers
- [ ] Provisioned Grafana dashboards in `ops/grafana/`: pipeline throughput,
      stage latency histograms, consumer lag, failure/DLQ rate
- [ ] Alert rules: lag over threshold, DLQ non-empty, stage p99 regression

**Gate:** `make obs-verify` — dashboards load from provisioning with no manual
setup; one uploaded video yields a **single trace** spanning API → probe →
transcode → package; the lag panel returns non-empty series.

## Phase 11 — Notify & failure UX `[ ]`

- [ ] `worker_notify`: webhook on `video.completed`, own consumer group, own retries
- [ ] Failure surfacing: DLQ'd renditions show as failed in the UI with a reason
- [ ] Manual replay endpoint/CLI: DLQ → source topic

**Gate:** `make e2e ARGS="-k failure"` — a corrupt upload shows a failed state in the UI
with a reason, the webhook fires for a successful one, and a DLQ replay drives the
video to completion.

## Phase 12 — Production hardening `[ ]`
Refs: ADR-0015

- [ ] Graceful SIGTERM shutdown everywhere: stop consuming, finish or cleanly
      abort in-flight work, commit offsets, flush producer, close
- [ ] **Media-worker containment** — non-root, read-only rootfs + tmpfs scratch,
      `cap-drop ALL`, `no-new-privileges`, seccomp, no network egress beyond
      Kafka/S3/Postgres, CPU/memory/wall-clock limits, ffmpeg `-threads` cap
- [ ] Input allow-list from ffprobe before transcoding; no `shell=True` anywhere;
      filenames derived from `video_id`, never from the uploaded name
- [ ] Backpressure / concurrency caps per worker; resource limits in compose
- [ ] Kafka prod profile: RF=3, `min.insync.replicas=2`, `acks=all`, no unclean
      leader election, auto-create off, TLS+SASL, per-topic retention
- [ ] Prometheus multiprocess mode wired in the API entrypoint (ADR-0014 gotcha)
- [ ] Backups: Postgres PITR with a **rehearsed restore**; object versioning +
      lifecycle rules for `tmp/` and abandoned uploads
- [ ] CI workflow: lint → unit → integration → build+Trivy scan+SBOM → e2e
- [ ] Migrations backward compatible for one release; run as a pre-deploy job
- [ ] SLOs defined; alerts fire on symptoms (lag, DLQ depth, SLO burn)
- [ ] README quickstart; ADR index current; PLAN.md diagram matches reality

**Gate:** `make ci` green from a clean clone on a machine with only Docker, plus
`make security-verify` — images scan clean at the agreed severity, containers run
as non-root with a read-only rootfs, and an egress attempt from a media worker to
an arbitrary host fails.

## Phase 13 — Deployment `[ ]`
Refs: ADR-0015

**Assumption (change if you say otherwise):** a **hardened compose production
profile on a single host** is the end state, with Kubernetes documented as the
upgrade path. This keeps the phase buildable now; the KEDA and NetworkPolicy
items below are written as K8s-when-adopted and degrade to compose equivalents
(`deploy.replicas`, an internal-only network) on a single host.

- [ ] `docker-compose.prod.yml` (hardened profile) — or Helm chart if K8s is chosen
- [ ] Secrets via the platform's secret store (or SOPS); nothing in the image
- [ ] Lag-based worker scaling — KEDA on K8s; a documented manual
      `docker compose up --scale worker-transcode=N` runbook on compose.
      Replica ceiling = partition count either way
- [ ] Shutdown grace period above p99 rendition time (`preStop` hook on K8s,
      `stop_grace_period` on compose)
- [ ] Media-worker egress restriction (NetworkPolicy on K8s, internal-only
      compose network + no default bridge on a single host)
- [ ] Separate node pool for transcode if on K8s (spot-safe: work is idempotent)

**Gate:** `make deploy-staging && make e2e BASE_URL=<staging-url>` — the e2e suite
passes against the deployed environment, not localhost.

---

## Open questions

- [ ] Auth: is this single-user/local, or does it need per-user isolation?
      (Affects object key prefixes and the SSE authorization check.)
- [ ] Rendition ladder: confirm the target set (360/480/720/1080 assumed).
- [ ] Retention: how long do source files and renditions live?
- [ ] Deploy target — **assumed: hardened compose on a single host**, K8s as the
      documented upgrade path (Phase 13). Confirm or redirect; it changes the
      scaling and network-policy work, not the application code.

## Decision log

| Date | Change | Why |
|---|---|---|
| 2026-08-25 | Plan, tracker and ADRs 0001–0013 written | Project kickoff |
| 2026-08-26 | Phase 2 contracts library landed; `make unit` green with 71 tests, `make lint` clean | The ADR-0004 eviction loop is now covered by unit tests rather than only by prose |
| 2026-08-26 | Phase 1 infra landed; gate green from a clean slate. Three service-dependent checkboxes moved to Phase 2 | Nothing to configure, health-check or containerise until services exist |
| 2026-08-26 | Frontend baseline corrected from React 18 to React 19 in PLAN.md and ADR-0014 | 18 was a stale default, not a decision; 19 is stable and unblocked by our stack |
| 2026-08-25 | ADR-0014 (library stack) and ADR-0015 (production readiness) added; Phase 12 expanded, Phase 13 added | Target is a production-grade app, so dependency and hardening choices are decided up front |

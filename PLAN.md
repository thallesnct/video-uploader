# Kafka Video Processing Pipeline — Overall Plan

A Python system that ingests an uploaded video, fans it out across Kafka-driven
stages (probe → transcode → thumbnails → package → notify), and streams progress
to a browser over SSE, with Prometheus/Grafana metrics and OpenTelemetry traces.

Status: **planning complete, implementation not started.** Track work in
[PROGRESS.md](PROGRESS.md). Every decision below is recorded as an ADR in
[docs/adr/](docs/adr/README.md) — read those for the rejected alternatives.

**Scope note (confirmed with the user 2026-08-28):** this system is built to
a genuine production-readiness bar (ADR-0015) — hardening, observability,
idempotency, the works — but it is **never actually deployed to a live or
production environment.** "Production ready" means the code and
architecture demonstrate that standard by the end of development, not that
anything runs beyond local Docker Compose. Weigh design trade-offs
accordingly: prefer whatever most clearly demonstrates the production-grade
decision over whatever would be cheapest for an actual live deployment.
Phase 13 ("Deployment") means standing up the hardened profile locally long
enough to prove its own gate passes, then tearing it down — not a
persistent target.

---

## 1. What we are building

```
                    ┌──────────┐   presigned PUT    ┌─────────┐
   browser ────────►│  MinIO   │◄───────────────────│ browser │
      │             └──────────┘                    └─────────┘
      │ POST /videos            (bytes never touch Kafka — ADR-0001)
      │ POST /videos/{id}/complete
      ▼
 ┌──────────┐  video.uploaded   ┌────────────┐  rendition.requested  ┌──────────────┐
 │   API    ├──────────────────►│   probe    ├──────────────────────►│  transcode   │
 │ FastAPI  │                   │  (ffprobe) │  (1 msg per rendition)│  (ffmpeg) xN │
 └────┬─────┘                   └─────┬──────┘                       └──────┬───────┘
      │                               │ video.probed                        │ rendition.completed
      │                               ▼                                     ▼
      │                        ┌────────────┐                        ┌──────────────┐
      │                        │ thumbnail  │                        │  packager    │
      │                        │  (sprite)  │                        │ (HLS join)   │
      │                        └─────┬──────┘                        └──────┬───────┘
      │                              │                                      │ video.completed
      │                              ▼            video.status              ▼
      │                        ╔═══════════════════════════════════════════════╗
      │                        ║      video.status  (every stage emits)        ║
      │                        ╚════════════╤══════════════════════╤═══════════╝
      │  SSE /videos/{id}/events            │ shared group         │ unique group per replica
      ▼                                     ▼                      ▼
 ┌──────────┐                        ┌────────────┐         ┌──────────────┐
 │ browser  │◄───────────────────────│  Postgres  │         │ API replicas │
 └──────────┘   snapshot + deltas    │ (read model)│        │ (SSE fan-out)│
                                     └────────────┘         └──────────────┘
                                          ▲ projector consumer (ADR-0007)
```

Sidecar consumers on `video.completed`: **notify** (webhooks) — a second consumer
group on the same topic, which is what makes the consumer-lag dashboards
interesting (ADR-0012).

## 2. Component inventory

| Service | Runtime | Responsibility |
|---|---|---|
| `web` | React 19 + TS + Vite, nginx in prod | Upload form, live rendition grid, HLS player with manual quality selection |
| `api` | FastAPI + `aiokafka` (asyncio) | Presign, `/complete`, read-model queries, SSE gateway |
| `worker-probe` | `confluent-kafka` + ffprobe | Read source metadata, decide the rendition ladder |
| `worker-transcode` | `confluent-kafka` + ffmpeg | One rendition per message; the parallel hot path |
| `worker-thumbnail` | `confluent-kafka` + ffmpeg | Poster frame + sprite sheet + WebVTT |
| `worker-package` | `confluent-kafka` | Completion join → HLS master manifest (ADR-0013) |
| `worker-notify` | `confluent-kafka` | Outbound webhooks; demo of a second consumer group |
| `projector` | `confluent-kafka` | Sole writer of the Postgres read model (ADR-0007) |
| `kafka` | Confluent/Redpanda image, **KRaft mode** | No ZooKeeper |
| `postgres` / `minio` | 16 / latest | Read model / blob store (ADR-0006, ADR-0007) |
| `prometheus`, `grafana`, `tempo`, `kafka-exporter`, `otel-collector` | | Observability stack (ADR-0010) |

Two Kafka client libraries on purpose: workers need `pause()/resume()` and manual
commit for long transcodes; the SSE gateway needs a native-asyncio consumer.
That is the whole discriminator (ADR-0009).

## 3. Kafka topology (summary — ADR-0002)

| Topic | Key | Partitions (dev/prod) | Producers | Consumer groups |
|---|---|---|---|---|
| `video.uploaded` | `video_id` | 3 / 6 | api | probe |
| `video.probed` | `video_id` | 3 / 6 | probe | thumbnail |
| `rendition.requested` | `video_id` | 12 / 24 | probe | transcode |
| `rendition.completed` | `video_id` | 6 / 12 | transcode | packager |
| `video.completed` | `video_id` | 3 / 6 | packager | notify |
| `video.status` | `video_id` | 6 / 12 | **all stages** | projector (shared), api (unique group per replica) |
| `*.retry` / `*.dlq` | `video_id` | 3 / 3 | any | retry pump / human |

Keying by `video_id` gives per-video ordering. Fan-out is one message per
`(video_id, rendition)` so 360p and 1080p run on different workers concurrently
and the ladder scales by adding partitions and pods, not threads.

`video.status` is a deliberate seam: internal stage topics can be reshaped
without changing the browser's event contract.

## 4. The three constraints that decide the design

These are the ones that break naive implementations. Each has an ADR.

1. **Video bytes never enter Kafka** (ADR-0001). Broker default
   `max.message.bytes` is ~1 MiB; raising it to gigabytes destroys replication
   latency and consumer memory. Messages carry `{video_id, object_key, rendition}`
   — the claim-check pattern.

2. **A transcode outliving `max.poll.interval.ms` (default 5 min) evicts the
   consumer** (ADR-0004). The group rebalances, the job is redelivered, the next
   worker is evicted the same way — an infinite reprocessing loop that looks like
   "Kafka is slow". Fix: `max.poll.records=1`, run ffmpeg on a worker thread,
   `pause()` the assigned partitions and keep calling `poll()` as a heartbeat,
   `resume()` and commit manually on success.

3. **SSE with more than one API replica silently drops events** (ADR-0008). An
   in-process subscriber registry only reaches clients attached to *that* replica.
   Fix: every API instance consumes `video.status` with a **unique `group.id`**
   (broadcast, not load-balanced). And a client connecting after a rendition
   finished must still see it: **snapshot from Postgres on connect, then stream
   deltas**, with `Last-Event-ID` resume, `:heartbeat` comments every 15 s, and
   `X-Accel-Buffering: no`.

## 5. Storage decisions

- **Blob store: MinIO in dev, S3-compatible in prod** (ADR-0006). Weighed against
  Postgres large objects (kills the DB, no presigning), local volumes (no
  horizontal scale), and GCS/Azure (fine, but S3's API is the portable one).
  Presigned `PUT` direct from the browser — multi-GB uploads must not stream
  through FastAPI. Layout:
  `videos/{video_id}/source.{ext}`, `videos/{video_id}/renditions/{h}p.mp4`,
  `videos/{video_id}/hls/{h}p/…`, `videos/{video_id}/thumbs/…`.
- **Database: PostgreSQL, written only by the projector** (ADR-0007). Kafka is
  the source of truth for the flow; Postgres is a projection serving SSE
  snapshots and list/detail queries. Rejected: each worker writing its own row
  (dual-write inconsistency), Redis-only (no durable history), Mongo (no
  relational win here).

## 6. Which extra use cases to build — and which to skip (ADR-0012)

You gave the transcode base case. The ranking axis is: **does it add a new
pipeline topology, or is it just another ffmpeg flag?** Only the first kind
teaches the system anything.

**In scope — each introduces a distinct distributed pattern:**

| Use case | New pattern it introduces |
|---|---|
| **ffprobe metadata stage** | *Conditional fan-out* — don't emit 1080p for a 720p source. Makes the fan-out data-dependent instead of fixed. |
| **Thumbnail / sprite sheet** | *Parallel independent branch* off the same upstream event; finishes fast while transcode is still running, so the UI proves incremental updates. |
| **HLS/DASH packaging** | *Multi-stage chaining* and forces a real **DAG join**: the master manifest needs *every* rendition done. Genuine distributed problem — see ADR-0013. |
| **Webhook / notify consumer** | *Second consumer group on one topic* — same events, independent offsets and lag. The dashboard case. |

**Stretch, only after Phase 11:** audio extract → transcription → subtitle track.
A slow independent branch with a heavy dependency (faster-whisper); good for
showing a stage whose latency is 10× the others, bad as an early dependency.

**Rejected, with reasons:** watermarking (an ffmpeg filter argument, zero new
topology), perceptual-hash dedupe (interesting but it is a similarity-search
project wearing a Kafka hat), retention/GC sweeper (a cron job, not a pipeline).

## 7. Observability — one correction to your ask (ADR-0010)

You said "tracing", but Prometheus + Grafana are **metrics**. We want both, and
they answer different questions:

- **Metrics (Prometheus → Grafana):** `transcode_duration_seconds` histogram by
  rendition, `stage_failures_total` by reason, in-flight gauge, queue depth. Plus
  **`kafka-exporter`** for per-group **consumer lag**, which is the single number
  that tells you which stage is the bottleneck.
- **Traces (OTel → Collector → Tempo):** the W3C `traceparent` is propagated
  **through Kafka message headers**, so one video's journey across
  API → probe → transcode → package is a *single trace*. This is what actually
  delivers "see what's going on with the different parts" — a metric tells you
  transcode is slow, the trace tells you it was slow *for this video, at this
  stage, after waiting 40 s in the queue*.
- Grafana dashboards live in-repo as **provisioned JSON** (`ops/grafana/`), not
  clicked together by hand.

## 8. Testing (ADR-0011)

`ffmpeg` is **not installed on this machine**. Therefore: ffmpeg/ffprobe live
only in the worker image; any test shelling out to them runs in Docker or is
marked `@pytest.mark.ffmpeg` and skipped locally. This is a hard constraint on
how tests are written, not a preference.

- **Unit** — pure logic, no I/O: envelope round-trip, ladder selection from probe
  output, retry/backoff math, key derivation. Fast, run on every save.
- **Integration** — `testcontainers`: Kafka (KRaft), Postgres, MinIO. Real broker,
  real bucket. Covers redelivery/idempotency, DLQ routing, projector writes,
  SSE snapshot+delta.
- **E2E** — full compose, Playwright: upload a fixture, assert every rendition
  object lands *and* that SSE emitted an event per rendition without a reload.
- **Fixture:** a 2-second generated clip
  (`ffmpeg -f lavfi -i testsrc=size=640x360:rate=15 -t 2`), committed or built
  once in the image. Keeps integration/e2e hermetic and under a minute.

## 9. Library stack (ADR-0014)

Chosen for a production system, each against its credible alternative. Full
reasoning and the rejected options are in ADR-0014; the discriminators in brief:

| Layer | Choice | Beat out | Because |
|---|---|---|---|
| API | FastAPI + uvicorn (gunicorn-managed) | Litestar, Django REST, Flask | async-native for long-lived SSE; Pydantic-native; largest async ecosystem |
| Kafka | `confluent-kafka` (workers) + `aiokafka` (API) | `kafka-python`, one-client-everywhere | pause/resume + manual commit vs. native asyncio — two real needs (ADR-0009) |
| DB | SQLAlchemy 2.0 + Alembic (`asyncpg` / `psycopg`) | SQLModel, Tortoise, raw asyncpg | production track record, and drops to raw SQL for the `UPDATE … RETURNING` claims |
| Storage | boto3 / aioboto3 | minio-py, s3fs | reference S3 implementation; keeps us portable off MinIO |
| Media | `subprocess` + ffmpeg CLI argv | PyAV, ffmpeg-python, MoviePy | **out-of-process** — hostile input crashes a subprocess, not the worker |
| Config | pydantic-settings | dynaconf, raw env | validated at startup, fails fast |
| Logs / metrics / traces | structlog · prometheus-client · opentelemetry-sdk | loguru, statsd, vendor SDKs | context binding, the reference exporter, vendor-neutral propagation |
| Retry (non-Kafka) | tenacity | backoff, hand-rolled | declarative and testable |
| SSE | sse-starlette | hand-rolled StreamingResponse | heartbeats, framing and disconnect detection done right |
| Deps / QA | uv (locked) · ruff · mypy · pre-commit | Poetry, black+isort+flake8 | one fast toolchain, reinstallable in every CI layer |
| Tests | pytest · testcontainers · hypothesis · Playwright | mocks, shared compose, Cypress | real broker, per-session isolation (ADR-0011) |
| Frontend | React 19 + TS + Vite · TanStack Query · native `EventSource` · hls.js | SvelteKit, Next.js, socket.io, video.js | plain SPA; `EventSource` gives reconnect and `Last-Event-ID` for free |

Two stack-level alternatives worth naming because a reviewer will ask:
**Celery+Redis** (mainstream Python job queue — rejected: second broker, loses
replay/lag/multi-group semantics) and **Go/Rust workers** (defensible, rejected:
one language, one contracts library; ffmpeg dominates the runtime anyway).

## 10. Production readiness (ADR-0015)

Not a final phase — several of these constrain how each service is written, so
they land as the code lands.

- **ffmpeg parses attacker-controlled bytes.** Media demuxers are a standing CVE
  source, so media workers run non-root, read-only rootfs with a tmpfs scratch,
  all capabilities dropped, **no network egress** beyond Kafka/S3/Postgres, and
  hard CPU/memory/wall-clock limits. Never `shell=True`; filenames derive from
  `video_id`, never from the uploaded name.
- **Auth:** OIDC bearer tokens (no home-grown auth), owner-prefixed object keys,
  per-video authorization on the SSE stream with token expiry enforced on the
  long-lived connection, upload quotas and rate limits at the edge.
- **Health probe semantics:** `/healthz` (liveness) checks **nothing external** —
  a liveness probe that pings Kafka turns a broker blip into a fleet-wide restart.
  `/readyz` checks dependencies and pulls the instance from the load balancer.
- **Graceful shutdown** on SIGTERM everywhere: stop consuming, finish or cleanly
  abort the in-flight rendition, commit, flush, close.
- **Kafka in prod:** RF=3, `min.insync.replicas=2`, `acks=all`, no unclean leader
  election, auto-create off, TLS+SASL, per-topic retention.
- **Scale workers on consumer lag (KEDA)**, not CPU — lag is the signal that
  reflects backlog. Replica ceiling = partition count, which is why partitions are
  over-provisioned in §3. Transcode is spot-instance-safe because it is idempotent.
- **Durability:** Postgres PITR with a rehearsed restore; object versioning and
  lifecycle rules. The read model is rebuildable from Kafka; the blobs are not.
- **Supply chain:** pinned lockfile, Renovate, Trivy scan + SBOM in CI,
  backward-compatible migrations run as a pre-deploy job so rollback is safe.
- **Alert on symptoms** — lag, DLQ depth, SLO burn — not on causes.

## 11. Build order

Phases with their verifiable gates are in [PROGRESS.md](PROGRESS.md). The shape:
infra skeleton → shared contracts → upload path → probe → transcode (with the
rebalance and idempotency work) → read model → SSE → frontend → thumbnails and
HLS join → observability → notify and failure UX → hardening.

Rule of thumb: nothing moves to the next phase until its gate command is green.

## 12. Repository layout (target)

```
.
├── AGENTS.md              # agent/dev workflow  (CLAUDE.md is a symlink to this)
├── PLAN.md  PROGRESS.md
├── Makefile               # up down topics unit integration e2e lint ci smoke
├── docker-compose.yml     # + docker-compose.obs.yml
├── docs/adr/              # 0001..0015 + README index
├── libs/pipeline/         # envelope, topics, kafka wrappers, storage, otel, idempotency
├── services/
│   ├── api/               # FastAPI: presign, complete, queries, SSE
│   ├── worker_probe/  worker_transcode/  worker_thumbnail/
│   ├── worker_package/  worker_notify/  projector/
├── web/                   # React + Vite
├── ops/{prometheus,grafana,tempo,otel}/
└── tests/{unit,integration,e2e}/  fixtures/
```

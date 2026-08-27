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
| 3 | Upload path | `[x]` | `make integration` ✅ 20 tests |
| 4 | Probe stage | `[x]` | `make integration ARGS="-k probe"` ✅ 6 + `make ffmpeg-tests` ✅ 4 |
| 5 | Transcode workers | `[x]` | `make integration ARGS="-k transcode"` ✅ 5 + full suite ✅ 30 |
| 6 | Read model & projector | `[ ]` | `make integration ARGS="-k projector"` |
| 7 | SSE gateway | `[ ]` | `make integration ARGS="-k sse"` |
| 8 | Frontend | `[ ]` | `make e2e ARGS="-k upload_flow"` |
| 9 | Thumbnails, HLS & completion join | `[ ]` | `make e2e ARGS="-k hls"` |
| 10 | Observability | `[ ]` | `make obs-verify` |
| 11 | Notify & failure UX | `[ ]` | `make e2e ARGS="-k failure"` |
| 12 | Production hardening | `[ ]` | `make ci` from a clean clone + `make security-verify` |
| 13 | Deployment | `[ ]` | `make deploy-staging && make e2e BASE_URL=<staging>` |
| 14 | Load testing | `[ ]` | Four scenarios run, results and bottlenecks recorded |

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

## Phase 3 — Upload path `[x]`
Refs: ADR-0001, ADR-0006, ADR-0016

- [x] Local OIDC issuer in compose + JWKS verification in the API (ADR-0016)
- [x] `owner_id` from the `sub` claim; repository functions take it as a
      **required positional argument**
- [x] Migration `0002_videos`: `owner_id` not null, indexed for listing and for
      quota counting
- [x] `POST /videos` → row (`awaiting_upload`) + presigned PUT under the
      caller's own prefix only
- [x] `POST /videos/{id}/complete` → verify object exists, claim, emit
      `video.uploaded` + `video.status`
- [x] Content-type allow-list at the door; **size limit verified after upload**,
      not in the presign — see the note below
- [x] Per-user quotas: declared upload size, videos in flight
- [x] `GET /videos`, `GET /videos/{id}` — owner-filtered
- [x] Multi-stage image for the API: non-root, read-only rootfs, caps dropped

### Where the presigned URL cannot enforce what ADR-0006 implied

A presigned **PUT** signs a key and content type, but it cannot bound the body
size — only a presigned **POST policy** carries `content-length-range`. So the
size ceiling is enforced in two places instead of one: the *declared* size is
refused at `POST /videos` (413), and the *actual* stored size is checked at
`/complete`, which deletes the object and returns 413 if it is over. A caller
can therefore still burn bandwidth uploading something too large before being
told. Moving to a presigned POST policy is the fix if that becomes a problem;
recorded here rather than claiming enforcement we do not have.

**Gate:** `make integration` — **PASSING**, 20 tests, on 2026-08-26. Also
`make unit` (77) and `make lint` clean.

Covers what the gate named: the fixture is PUT to MinIO through the presigned
URL, `/complete` publishes exactly one `video.uploaded` keyed by `video_id`, and
a second `/complete` publishes nothing while still returning 200. Plus the
isolation cases — user B cannot read, complete, or list user A's video, and a
presigned URL rewritten to point at another tenant's key is rejected by the
signature.

### Three production bugs the integration tests caught

None of these could fail a unit test, and all three are fatal in the built image:

1. **`aiokafka` needs the `[lz4]` extra.** librdkafka compiles lz4 in, so the
   workers were fine while the API died at startup — a split that looks like a
   broker problem.
2. **`sqlalchemy` needs the `[asyncio]` extra** for greenlet, or the API raises
   on its first database query.
3. **Instrumenting inside the lifespan silently does nothing.** Starlette builds
   its middleware stack before lifespan runs, so the tracing middleware never
   joined it: the app worked, and every message quietly lost its `traceparent`.
   ADR-0010's single trace per video would have been broken at the first hop,
   discoverable only months later.

### Also learned: never let pip resolve on this host

`pip install -e ".[dev]"` backtracked to urllib3 1.25 and burned minutes at 86%
CPU. `uv.lock` is the source of truth (ADR-0014), so `make` now exports it to a
pinned `requirements-dev.txt` and installs with `--no-deps`. The project is not
installed at all — `pytest`'s `pythonpath` imports it from the source tree,
removing the build-backend step that hung the same way.

## Phase 4 — Probe stage `[x]`
Refs: ADR-0011, ADR-0012, ADR-0013, ADR-0016

- [x] `worker_probe`: ffprobe → duration, resolution, codecs, audio streams
- [x] Ladder selection: never upscale — keyed on the **short side**, so portrait
      video is classed correctly and rotation is irrelevant
- [x] Emit `video.probed` + one `rendition.requested` per selected rendition
- [x] Emit `video.status` (`probed`, with the planned rendition list so the UI
      can render placeholders immediately)
- [x] `owner_id` added to the event envelope — a worker cannot build an
      owner-scoped key without it (ADR-0016 §6)
- [x] Worker image with ffmpeg, sharing one `ffmpeg-base` stage between the
      shipped image and the test image

**Gate — split, because the original wording was unreachable on this host.**
The gate said "a 640×360 fixture emits exactly the sub-360p ladder", which needs
ffprobe. ffmpeg lives only in the worker image (ADR-0011), so rather than
silently reinterpreting the gate as "injected metadata", it is now two checks:

1. `make integration ARGS="-k probe"` — **PASSING**, 6 tests. Injects the prober
   and covers the Kafka round trip, the fan-out, and the plan/fan-out invariant.
2. `make ffmpeg-tests` — **PASSING**, 4 tests, run inside the worker image
   against real ffprobe and the generated clip. This is where the original
   wording is actually verified: a real 640×360 file yields `["360p"]` and no
   1080p.

Plus `make unit` (106) and `make lint` clean, and the **full** integration suite
green together: 26 tests (20 upload + 6 probe) in one session.

Running them together mattered. It caught a regression from the `owner_id`
envelope change that the phase gates alone missed: `/complete` published
`video.uploaded` and then raised a `ValidationError` on `video.status`, leaving
the claim taken and no status event emitted. In production that is every upload
failing halfway through.

### The assertion this phase exists for

`test_the_plan_and_the_fan_out_cannot_disagree` asserts that the set of emitted
`rendition.requested` messages **equals** `video.probed.expected_renditions` —
not a subset in either direction. If those ever diverge, the packaging join
(ADR-0013) waits forever for a rendition nobody was asked to produce, and the
video sits in `transcoding` with nothing to alert on. The handler computes the
ladder once and derives both from that single list, which is what makes the
assertion hold by construction rather than by care.

## Phase 5 — Transcode workers `[x]`
Refs: ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0009 — **the highest-risk phase**

- [x] ffmpeg invocation per rendition, streamed to a temp file then uploaded
- [x] `pause()`/`poll()` heartbeat, `resume()` + manual commit on success —
      `StageWorker`, exercised for the first time against a real long-running
      process rather than a synthetic sleep in a unit test
- [x] Idempotency: skip (re-announce, don't re-encode) if the output object
      already exists; a DB claim additionally guards against genuine
      *concurrent* double-processing (a manual DLQ replay racing a live retry)
- [x] Failure classification: retryable (transient S3/network) vs terminal
      (corrupt input) → retry topic vs DLQ
- [x] `storage.promote()` uses boto3's transfer-managed `copy()`, not the
      low-level `copy_object` — chosen over hand-rolled multipart after
      verifying the method and its behavior exist (ADR-0006)
- [x] Emit `rendition.completed` + `video.status`; emit `pipeline.failed` on
      DLQ (landed in `StageWorker` itself, so every future stage gets it free)
- [x] `renditions` table + `RenditionRepository` — the column-ownership rule
      from ADR-0007 made concrete: the worker's one write path touches only
      `attempt`/`claimed_at`, never `status`/`object_key`
- [x] Multi-stage image with ffmpeg, sharing one `ffmpeg-base` stage between
      the shipped image and the test image (mirrors `worker_probe`)

**Gate:** `make integration ARGS="-k transcode"` — **PASSING**, 5 tests. Plus
`make ffmpeg-tests` (9 — both probe's and transcode's), `make unit` (123), and
the **full combined integration suite** (upload + probe + transcode, 30 tests,
one session — no cross-test interference this time).

1. ✅ a transcode longer than `max.poll.interval.ms` completes without eviction.
   The literal gate wording ("no duplicate output, no repeated consumption")
   turned out to be too weak to trust — see below.
2. ✅ a redelivered message produces one object and one DB row (`attempt == 1`,
   confirming the second delivery never touched the claim).
3. ✅ a corrupt input lands in the DLQ with `failure_reason` in its headers,
   **and** a `pipeline.failed` event is published — a gate case the original
   wording didn't ask for but Phase 5's own checklist did.

### Why "no duplicate output" was rewritten before it was trusted

A broken implementation (no heartbeat during the handler) still ends up with
exactly **one** file on disk: eviction causes redelivery, but the *second*
invocation's own object-existence idempotency check would see the first
invocation's output already sitting at the target key and skip re-encoding.
"No duplicate output" is therefore satisfied by both the correct and the broken
implementation — it does not discriminate.

The assertion that actually catches the bug: **the injected transcode function
was invoked exactly once**, plus a follow-up `consumer.poll()` on the same
consumer proving nothing is left to redeliver. Verified against a real KRaft
broker with `max.poll.interval.ms` and `session.timeout.ms` both reduced to 6s
(confirmed empirically first that librdkafka requires the former ≥ the latter)
and a handler that sleeps 8s — longer than the poll interval, so a broken
implementation would provably be evicted mid-handler.

### Two more places a scripted edit silently missed, caught before they shipped

- `duration_s` added to `RenditionRequested` (needed for the realtime-ratio
  metric) — the probe worker's fan-out call is where a naive text match would
  miss it again after `ruff format` reflows a multi-line call. Verified this
  time against pydantic's own required-field list rather than a regex, since
  the sweep script used for the `owner_id` addition had a truncation bug on
  long multi-line calls (silently produced both false positives and, had the
  file been different, could have missed a true one).
- The `insert(...).on_conflict_do_update(...)` claim statement was drafted with
  an extra `OR status == 'completed'` branch in its WHERE guard that would have
  let a *completed* rendition be re-claimed — backwards from the claim's actual
  purpose (mutual exclusion, not a completion check). Caught on review before
  it was ever run, not by a test.

## Phase 6 — Read model & projector `[x]`
Refs: ADR-0007

- [x] Schema: `videos`, `renditions` (unique on `(video_id, rendition)` —
      both already existed from Phase 5) plus `events` (append-only,
      `event_id` unique, for `Last-Event-ID` replay) — migration 0004_events
- [x] `projector` consumes `video.status` **and** `pipeline.failed` (shared
      group) and upserts — the registry's `consumed_by` list already said so;
      ADR-0007's prose undersold it (follow-on added)
- [x] Offsets committed **after** the DB write; upsert makes replay safe

**Gate:** `make integration ARGS="-k projector"` — 6 passed. Replaying the same
two-event batch twice produced exactly one `videos` row, one `renditions` row,
and two `events` rows (not four) — no duplicates anywhere.

**Discovered mid-phase, resolved before this phase closed (not deferred):**

1. `VideoStatusChanged` didn't carry the data the read model needs
   (`duration_s`/`width`/`height`/`expected_renditions`/rendition completion).
   ADR-0007's decision text said the projector "consumes video.status" but
   that topic's only payload was a human-readable `detail` string. Fixed by
   enriching the event (additive, optional fields) rather than having the
   projector or the future SSE gateway read internal stage topics — see
   ADR-0007's follow-on.
2. `video.status`/`pipeline.failed` are `retries: false`, and `TopicRegistry`
   skipped creating a DLQ for them too — the projector is the first consumer
   to ever hit that path via `StageWorker`, and a transient failure would have
   tried to produce to a retry-tier topic that was never created. First fix
   attempt (leave the offset uncommitted, keep polling) was itself wrong:
   Kafka offsets are monotonic, so a later successful commit would silently
   skip the failed message forever. Corrected to: DLQ topics always exist now
   (independent of `retries`), and a TRANSIENT failure with no route crashes
   the worker so a restart resumes from the last committed offset. See
   ADR-0005's follow-on for the full reasoning and the rejected alternative
   (giving `video.status` a normal retry ladder breaks its per-video
   ordering guarantee).
3. Crash-to-retry only works if a misclassified *permanent* failure doesn't
   loop forever. The concrete case: the projector's `UPDATE videos SET ...
   WHERE id=<missing>` is a silent no-op, and the following `events` insert's
   FK then raises `IntegrityError` — unclassified, that defaults to TRANSIENT
   and crash-loops on the same message indefinitely. The handler now
   reclassifies `IntegrityError` as `TerminalError`, routing it to the
   (now always-present) `video.status.dlq` instead.
4. `docker compose run migrate` had never actually worked — the api image
   (which the `migrate` service builds from) never shipped `alembic.ini` or
   `migrations/`. Every prior migration was applied via the host venv, which
   masked it. Fixed and verified by rebuilding and running the service
   directly (a no-op, since the host venv had already applied 0004_events).
5. Not fixed, documented instead: the projector is deliberately not wired
   into `docker-compose.yml` — no worker is yet (Phase 13's job) — so "crash
   and let the orchestrator restart it" currently means a manual restart.
   Acceptable pre-deployment; flagged in ADR-0005's follow-on to revisit
   before Phase 13 is called done.
6. No monotonicity guard on `videos.status` — a manual DLQ replay could in
   principle regress state out of order. Deferred as a Phase 12 item
   alongside the Phase 5 claim-window gap (see ADR-0007's follow-on).

## Phase 7 — SSE gateway `[x]`
Refs: ADR-0008

- [x] `GET /videos/{id}/events` — snapshot from Postgres first, then live deltas
- [x] Each API instance consumes `video.status` **and `pipeline.failed`** with
      a unique, ephemeral `group_id` (`StatusBroadcaster`, never committed)
- [x] `Last-Event-ID` resume — the same code path as a fresh connect, just a
      different starting watermark, both reading from the `events` table
- [x] 15s keep-alive ping (sse-starlette's default, matches the ADR exactly);
      `X-Accel-Buffering: no` and `Cache-Control: no-store` (sse-starlette
      defaults — stricter than the ADR's original `no-cache`, amended);
      `finally: unsubscribe(...)` proven to run on disconnect

**Gate:** `make integration ARGS="-k sse"` — 7 passed. (a) a client connecting
after two renditions finished sees both in the snapshot, then the third live
through the real projector and broadcaster; (b) two broadcasters with unique
group ids both wake for one event, and the negative case (shared group id,
asserted over a 20-event batch rather than one message — see below) shows a
clean, non-overlapping split; (c) reconnect with `Last-Event-ID` replays no
duplicates. Full combined integration suite: 43 passed, one session.

**Discovered mid-phase, resolved before this phase closed:**

1. The `events.id` an SSE client needs is assigned by the **projector**, a
   separate, unsynchronized consumer of the same Kafka topic — the gateway
   parsing the live message and inventing an id would not agree with it.
   Resolved: the gateway's Kafka consumer only reads the message **key**
   (`video_id`) as a wake-up signal; content and ids always come from
   re-querying Postgres, unifying fresh-connect and `Last-Event-ID` resume
   into one code path. See ADR-0008's follow-on.
2. `AIOKafkaConsumer.assignment()` being non-empty is not the same milestone
   as `auto_offset_reset="latest"` actually resolving to a concrete fetch
   position — that resolution is otherwise lazy. Deterministically reproduced
   (5/5) a message published right after `start()` returns being missed by a
   freshly-started consumer; fixed with `seek_to_end()` (5/5 clean after).
   This is the same *class* of bug `StageWorker.wait_for_assignment()` exists
   to prevent for confluent-kafka, in aiokafka's own shape.
3. FastAPI's `TestClient` was verified (not assumed) to buffer an entire SSE
   response before returning any of it — useless for asserting live timing.
   `sse_stream` was pulled out as a standalone async generator, parametrized
   on `sessions`/`broadcaster` rather than `app.state.*`, specifically so
   tests can drive it by direct async iteration against real Kafka/Postgres.
   One HTTP-level test still covers the route wiring itself, using a stream
   that terminates on its own (an already-failed video).
4. Gate (b)'s negative case (shared `group_id` delivers to only one replica)
   was flaky on a single message even with a settle delay and re-seek — a
   two-way rebalance's timing isn't deterministic enough at that grain.
   Rewritten to assert the aggregate property over 20 events (every event
   delivered, none delivered to both) instead, which is what the test is
   actually there to prove and doesn't depend on that timing.

**Not addressed, documented instead:** `video.completed` as a terminal event
— nothing emits `VideoState.COMPLETED` before Phase 9, so its shape isn't
settled; only `failed` ends the stream today, re-checked from the video row's
`status` column every poll rather than inferred from event payloads, so
Phase 9 extends this by widening one comparison. Also: a browser's
`EventSource` auto-reconnects even after a clean server close, so Phase 8's
frontend must call `eventSource.close()` itself on a terminal event — recorded
as a cross-phase contract in ADR-0008's follow-on, not something Phase 7 can
enforce from the server.

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
- [ ] **Known gap from Phase 5** — `RenditionRepository.claim()`'s stale window
      (`STALE_AFTER = 2h`) is longer than the retry ladder's total span
      (10s/1m/10m). If a worker crashes mid-claim, siblings retrying the same
      rendition hit `TransientError` (claim denied) on every attempt and the
      message DLQs before the 2h stale window ever frees the claim — even
      though nothing is actually wrong with the rendition. Fix by shortening
      `STALE_AFTER` to comfortably less than the retry ladder's span, or by
      widening the ladder, so a crashed sibling's claim frees before the
      message dead-letters.

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

## Phase 14 — Load testing `[ ]`
Refs: ADR-0016, ADR-0002, ADR-0008, ADR-0010

The reason isolation and quotas are built rather than assumed. What is worth
measuring here is not "requests per second" — it is where the pipeline bends.

- [ ] Token minter driving N synthetic users against the local issuer
- [ ] Scenario: upload throughput — concurrent presigned PUTs, measuring the API
      only (bytes never cross it, so this should stay flat as size grows)
- [ ] Scenario: transcode saturation — verify the parallelism ceiling really is
      the partition count of `rendition.requested` (ADR-0002), by scaling workers
      past it and watching lag stop improving
- [ ] Scenario: SSE fan-out — concurrent open streams, since connection count is
      the API's real capacity metric, not RPS (ADR-0008)
- [ ] Scenario: noisy neighbour — one tenant floods; assert quotas keep the
      others' end-to-end latency within budget
- [ ] Record the numbers as SLO baselines in ADR-0015, replacing its placeholders

**Gate:** a documented run of all four scenarios with results committed, and the
bottleneck of each named.

---

## Open questions

- [x] ~~Auth: single-user or per-user isolation?~~ **Answered 2026-08-26:**
      per-user isolation, because this is not a study project and will be load
      tested. Recorded as ADR-0016; object keys now carry the owner prefix.
- [ ] Rendition ladder: confirm the target set (360/480/720/1080 assumed).
- [ ] Retention: how long do source files and renditions live?
- [ ] Deploy target — **assumed: hardened compose on a single host**, K8s as the
      documented upgrade path (Phase 13). Confirm or redirect; it changes the
      scaling and network-policy work, not the application code.

## Decision log

| Date | Change | Why |
|---|---|---|
| 2026-08-25 | Plan, tracker and ADRs 0001–0013 written | Project kickoff |
| 2026-08-26 | Phase 5 transcode workers landed; the rebalance test survives a real KRaft broker with a reduced poll interval | "No duplicate output" proved to be a non-discriminating assertion — rewritten to handler-invocation-count before being trusted |
| 2026-08-26 | Phase 4 probe stage landed; ladder is data-dependent and the plan/fan-out invariant is asserted | Gate split into an ffmpeg-free integration check and an in-image ffprobe check, rather than reinterpreting the original wording |
| 2026-08-26 | Phase 3 upload path landed; integration gate green with 20 tests | Caught three production-fatal bugs (lz4, greenlet, lifespan instrumentation) that no unit test could reach |
| 2026-08-26 | Per-user isolation confirmed (ADR-0016); object keys gain an owner prefix; Phase 3 gains auth/quota tasks and a Phase 14 for load testing | The user confirmed real-world intent and load testing, making tenancy a design input rather than a retrofit |
| 2026-08-26 | Phase 2 contracts library landed; `make unit` green with 71 tests, `make lint` clean | The ADR-0004 eviction loop is now covered by unit tests rather than only by prose |
| 2026-08-26 | Phase 1 infra landed; gate green from a clean slate. Three service-dependent checkboxes moved to Phase 2 | Nothing to configure, health-check or containerise until services exist |
| 2026-08-26 | Frontend baseline corrected from React 18 to React 19 in PLAN.md and ADR-0014 | 18 was a stale default, not a decision; 19 is stable and unblocked by our stack |
| 2026-08-25 | ADR-0014 (library stack) and ADR-0015 (production readiness) added; Phase 12 expanded, Phase 13 added | Target is a production-grade app, so dependency and hardening choices are decided up front |

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
| 8 | Frontend | `[ ]` | `make e2e ARGS="--grep upload_flow"` |
| 9 | Thumbnails, HLS & completion join | `[x]` | `make e2e ARGS="--grep hls"` ✅ 1 test |
| 10 | Observability | `[x]` | `make obs-verify` ✅ passing |
| 11 | Notify & failure UX | `[x]` | `make e2e ARGS="--grep failure"` ✅ 2 tests + `make replay-verify` ✅ passing |
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

### Follow-on: a stalled rebalance that never recovers (2026-08-27, Phase 8)

The eviction loop above handles a *long handler*; it had no answer for a
*broker that stops answering group-coordination requests*, which is what
actually happened running the real stack (see ADR-0004's follow-on and Phase
8 finding #6, now closed rather than deferred to Phase 14). `StageWorker`
now wires `on_assign` and crashes with `ConsumerGroupStalled` once it has
held no assignment for longer than any ordinary rebalance takes;
`docker-compose.yml`'s app-tier services gained `restart: unless-stopped` to
make that crash self-heal. `make unit` (134) and `make integration` (50)
both green with the change in place.

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

### Follow-on: expiring and cancelling a stuck `awaiting_upload` (2026-08-27)

Hit directly using the real Phase 8 frontend: a presigned URL signed for a
host the browser couldn't resolve left a `video` row wedged at
`awaiting_upload` forever, silently occupying one of `max_videos_in_flight`'s
10 slots with no way to see or clear it. This ADR's own Consequences section
had already named the risk ("an upload that never calls `/complete` leaves an
orphan") but only ever specified the object-store half of the fix, never the
Postgres half. Added both, scoped to `awaiting_upload` only — see ADR-0006's
follow-on for the full reasoning: `expire_stale_awaiting_uploads` (keyed off
the presign's own expiry, not an invented second TTL) runs opportunistically
on `POST /videos` and `GET /videos`; `DELETE /videos/{id}` cancels one
manually, as a claim rather than read-then-delete. Cancelling anything past
`awaiting_upload` is deliberately out of scope — a worker may already be
mid-job by then, and stopping that needs real cooperation from the workers,
not a DB flag.

**Gate:** `make integration -k test_upload` — 26 passed (20 original + 6 new:
one expiry test backdating `created_at` past the presign window, five
cancellation cases covering the happy path, quota release, the past-
`awaiting_upload` 409, and both isolation cases). Full suite: `make
integration` — 50 passed.

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

## Phase 8 — Frontend `[x]`

- [x] Upload page: direct-to-MinIO PUT with progress, then `/complete`
- [x] Video detail: rendition grid, placeholders from the probed ladder,
      each tile flipping to ready as its SSE event lands
- [x] Reconnect/backoff on SSE drop; empty and error states

**Gate:** `make e2e` — Playwright uploads the fixture and asserts each rendition
tile turns ready **without a page reload**. `ARGS` passes through to
`playwright test` itself (e.g. `ARGS="--grep upload-flow"`), not pytest's `-k`.

```
Running 1 test using 1 worker
  ✓  1 upload-flow.spec.ts:8:1 › upload flow: each rendition tile turns ready without a page reload (8.1s)
  1 passed (12.3s)
```

React 19 + TypeScript + Vite (ADR-0014); TanStack Query for server state
*and* mutations (`useMutation`, not a hand-rolled async function + `useState`
for every API write); TanStack Router for the two routes (`/`,
`/videos/$videoId`) — added as an ADR-0014 follow-on, same family as
TanStack Query already justified there. CSS Modules per component,
driven by CSS custom-property tokens, never a shared global stylesheet.
`backend/` and `frontend/` are now separate top-level trees (own manifests,
own lockfiles); `tests/e2e/` stays at the true root since it drives a browser
against both sides together.

**Discovered mid-phase, resolved before this phase closed:**

1. **The repo needed a real backend/frontend split before frontend/ could
   land cleanly.** `libs/`, `services/`, `migrations/`, `tests/{unit,
   integration,ffmpeg}/`, `pyproject.toml`, `uv.lock`, `alembic.ini` moved
   under `backend/`; `docker-compose.yml`'s six build blocks point at
   `context: ./backend`; the Makefile's Python targets use
   `uv run --directory backend` so every relative path already in
   `pyproject.toml` (testpaths, per-file-ignores, pythonpath) keeps
   resolving unchanged. `infra/` stays at the true root (runs before the
   backend's venv exists) but lints against the same rules via a new
   `infra/ruff.toml` that extends `backend/pyproject.toml`. Caught in the
   process: `.gitignore`'s bare `*.ts` rule (meant for HLS segments) would
   have silently swallowed every TypeScript source file — scoped to `tmp/`.
2. **`api`/`worker-probe`/`worker-transcode`/`projector` had no profile
   tag**, so a bare `make up` started them immediately, racing `bootstrap`
   (topic creation) — `UnknownTopicOrPartitionError` every time, a silent
   trap for anyone running the real stack rather than the test suites.
   Tagged `profiles: ["app"]`; `make up` now only brings up the infra tier,
   matching what its own docstring always claimed. `make e2e` starts the
   app tier explicitly, after `bootstrap` and a new `make migrate` target.
3. **Presigned URLs and MinIO's CORS allow-list both assumed a host-run
   browser** (`S3_PUBLIC_ENDPOINT=http://localhost:9000`,
   `MINIO_API_CORS_ALLOW_ORIGIN` listing only `localhost` origins). Broke
   immediately for `make e2e`, whose browser runs *inside* the compose
   network (Playwright's own container) — `localhost` there means that
   container, not the host. `S3_PUBLIC_ENDPOINT` is now overridable
   (`make e2e` sets it to `http://minio:9000`); the CORS list gained
   `http://frontend:5173`.
4. **`vite preview` rejects any `Host` header not on an allowlist**
   (DNS-rebinding protection) — same root cause as (3), the e2e browser
   reaches the frontend container by compose service name. Added
   `preview.allowedHosts`.
5. **No Chromium build exists for every host OS/arch combination** — hit
   directly ("Playwright does not support chromium on mac13-arm64").
   Playwright now runs inside Microsoft's own container image, joined to
   the compose network, rather than on the host — which is also what CI
   would do, so the local and CI paths are identical rather than diverging
   the first time someone's laptop doesn't match Playwright's support matrix.
6. **A Kafka consumer that session-times-out during a resource-contended
   cold start did not resume on its own.** Found running the real e2e gate
   repeatedly on a 4-CPU dev machine: the projector logged
   "revoking assignment and rejoining group" once, then produced zero
   further log lines while two videos sat stuck at `status=uploaded`
   indefinitely — confirmed via direct DB inspection, not inferred. A plain
   `docker compose restart projector` (no rebuild) caught both videos up
   instantly, which points at broker-side unavailability under sustained
   load rather than a `StageWorker` defect: `poll()` is what surfaces
   librdkafka's own rejoin, and it recovered cleanly the moment it got a
   stable window. **Not root-caused further here** — flagged for a closer
   look before Phase 14 load testing, which will produce exactly this kind
   of sustained pressure for real, not just as an artifact of rebuilding
   five images back to back on a laptop.

   **Update:** recurred twice more using the real frontend, hitting `probe`,
   `transcode`, and `projector` simultaneously — clearly this session's own
   `docker compose --build` activity as the trigger, not an e2e-specific
   fluke. Mitigated in Phase 2's follow-on / ADR-0004's follow-on: a stall
   watchdog crashes a worker that never recovers an assignment, paired with
   `restart: unless-stopped`. Root cause (why rejoin itself keeps failing
   for minutes under contention) is still open, tracked in Phase 14.

## Phase 9 — Thumbnails, HLS & completion join `[x]`
Refs: ADR-0013

- [x] `worker_thumbnail`: poster + sprite sheet + WebVTT, off `video.probed`
- [x] `worker_transcode` also emits HLS segments + per-rendition playlist
- [x] `worker_package`: completion join — write `master.m3u8` only when every
      *expected* rendition is done, using the DB claim (`UPDATE … WHERE NOT
      packaged RETURNING`) so concurrent finishers elect exactly one packager.
      Also emits `video.completed` and `video.status`(`completed`).
- [x] Player in the UI (hls.js)

**Gate:** `make e2e ARGS="--grep hls"` — `master.m3u8` lists every rendition exactly
once, asserted against the real playlist an authenticated browser fetches.
The concurrent-double-finish half of the original gate wording is proven at
the integration level instead (`test_package.py`) — a race between two
workers isn't something a single Playwright run can force; explained in the
spec file's own header comment, not repeated here. (Corrected from the
original `make e2e --grep hls`: `ARGS` passes through to `playwright test`,
which has no `-k` — established in Phase 8.)

```
Running 1 test using 1 worker
  ✓  1 hls-playback.spec.ts:32:1 › hls playback: master.m3u8 lists every rendition once and hls.js fetches the whole tree authenticated (11.4s)
  1 passed (16.1s)
```

**`worker_thumbnail`, discovered mid-implementation:**

1. **The race ADR-0013 assumes for `worker_package` also applies to
   `video.status`'s `state` field, one stage earlier.** `_apply_status` sets
   `status` unconditionally from whatever `state` a `video.status` event
   carries, with no ordering guard — two producers (thumbnail, transcode) both
   publish to it for the same video, unordered relative to each other. Publishing
   `state=PROBED` from the thumbnail worker (which seemed natural — the whole
   ladder hasn't started yet) risks regressing a video already advanced to
   `TRANSCODING` by a faster-finishing rendition. Fixed by having
   `worker_thumbnail` always publish `state=TRANSCODING`, same as
   `worker_transcode` — both are sibling work within that one stage, so the
   value never actually varies. Root cause (`_apply_status` has no monotonic
   ordering guard at all) is unfixed and out of scope here; flagged for
   whoever touches the projector next.
2. **`VideoProbed` had nowhere to carry the source key.** `worker_thumbnail`
   needs to download the original upload, but `VideoProbed` — unlike
   `RenditionRequested` — never carried it (only `worker_probe` itself had
   `event.object_key` from the `VideoUploaded` it consumed). Added
   `source_key: str | None = None` to `VideoProbed` (optional per ADR-0003;
   the real producer always sets it, but the field itself must not be
   required for a message from an older producer to still parse), populated
   by `worker_probe` from the same value `RenditionRequested.source_key`
   already carries. A `None` is treated as terminal, not transient — there is
   no retry that fixes a message an old producer already published.
3. **ffmpeg's `fps` filter silently drops the sprite sheet on most real
   durations, not just edge cases.** `tile_count`'s `ceil()` means the nominal
   sample cadence's last period almost always extends past the source's
   actual end (exact-multiple durations are the rare exception). Verified
   empirically inside the worker image: `fps=1/N` over a period ffmpeg never
   sees the end of emits *zero* frames — not the last one at EOF — so
   `-frames:v 1` gets nothing to write and the whole sheet comes out empty
   with exit code 0, no error. `plan_sprite` now spaces `count` tiles evenly
   across the real `duration_s` with one slot of margin
   (`duration_s / (count + 1)`) instead of using the nominal interval
   directly, which also gives full-video scrubbing coverage for a source
   capped at `MAX_TILES` rather than only covering its first chunk.
4. **The mjpeg encoder rejects the `tile` filter's default color range.**
   `-pix_fmt yuvj420p` is required on the sprite argv, or every sheet fails
   with "Non full-range YUV is non-standard" — caught by the real-ffmpeg test
   (`tests/ffmpeg/`), not the injected-fake unit path, which is exactly why
   that split exists (ADR-0011).

**`worker_transcode` + HLS, discovered mid-implementation:**

1. **The MP4 and its HLS playlist are two separate promotes, so the existing
   single-object idempotency check (Phase 5) was no longer sufficient.** An
   attempt that dies between them leaves the rendition genuinely done but the
   playlist missing, and nothing would ever revisit it — the message that
   would trigger a retry already committed successfully the first time.
   Widened the skip condition to require both the MP4 **and** the playlist
   present; when only the MP4 exists, the handler remuxes from the existing
   object (no re-claim, no re-download, no re-encode) rather than either
   silently skipping or redoing the whole job. Covered by a dedicated
   integration test, not just inferred from the redelivery test, since the
   two failure windows are genuinely different states.
2. **Remux, don't re-encode.** HLS segments are generated with `-c copy`
   from the MP4 `worker_transcode` just produced, not from the original
   source — it is already at the target rendition's exact
   dimensions/bitrate, so a second encode pass would be pure waste.
3. **`RenditionCompleted` had no playlist field either**, same shape as
   `VideoProbed.source_key` from the thumbnail work above — added
   `playlist_key: str | None = None` (ADR-0003) so `worker_package` (next)
   can build `master.m3u8` from what it already consumes, without a DB
   round trip or re-deriving the key itself.
4. **Segments upload straight to their final keys; only the playlist goes
   through scratch+promote.** Nothing references a segment until the
   playlist that lists it is promoted, so only the playlist — the thing that
   must never be readable half-written — needs the atomic-rename treatment
   the MP4 rendition already gets.

**`worker_package`, discovered mid-implementation (see the ADR-0013
follow-on for the design decision itself):**

1. **A claim committed but never acted on needed its own recovery path.**
   `claim()` and the `master.m3u8` write are not atomic together — a crash
   between them leaves `packaging_claimed_at` set with no object ever
   written, and nothing left to naturally retry it (the join's inputs are
   exhausted; nothing re-triggers a video once every rendition has reported
   in). Two layers, cheapest first: the write is wrapped in
   `try/except Exception: release_claim(); raise`, so any failure both frees
   the claim *and* leaves the triggering message's offset uncommitted —
   Kafka redelivers it, same discipline as `worker_transcode`'s claim
   (ADR-0004: commit only after success). `PackagerRepository.claim()`
   additionally accepts a claim older than `STALE_AFTER` (10 minutes, short
   relative to `RenditionRepository`'s 2h — packaging a text file is seconds
   of work) as defense-in-depth for a hard kill that never reaches the
   `except` block. Safe to make stale-reclaimable unconditionally because
   `claim()` is only ever called after confirming `master.m3u8` doesn't
   exist yet — a genuinely finished video never reaches it, regardless of
   how old its claim is.
2. **Object existence, not the claim, is the idempotency check on the read
   path.** A redelivery of the message that already completed packaging
   finds `master.m3u8` present and re-announces without re-claiming —
   mirroring `worker_transcode`/`worker_thumbnail`'s existing pattern rather
   than inventing a new one. (Like those two, redelivery still re-publishes
   `video.completed`/`video.status` with fresh `event_id`s — an existing,
   accepted gap this phase didn't introduce; a downstream idempotent
   consumer, per ADR-0005, is the intended fix, not producer-side
   suppression.)
3. **Verified, not assumed: two ORM `session.add()`s in one transaction did
   not respect the FK dependency order.** Writing a `VideoRow` and a
   `RenditionRow` referencing it via `session.add()` in the same
   `sync_session_scope` block raised a foreign-key violation — SQLAlchemy's
   unit-of-work didn't sequence the insert before the two flushes this repo's
   existing tests never exercised (every prior test inserts a `VideoRow`
   alone, then creates `RenditionRow`s later via `RenditionRepository`'s raw
   `insert()`, never both via ORM `add()` in one flush). Test fixtures now
   commit the video and its renditions in two separate transactions; not
   investigated further since it only affects test setup, not any shipped
   code path.

**Player (hls.js), discovered mid-implementation:**

1. **A presigned MinIO URL can't serve a playlist tree.** hls.js resolves
   every relative reference inside a playlist (a rendition playlist from the
   master, a segment from a rendition playlist) against the URL it fetched
   that playlist from; a presigned URL's signature only covers the exact key
   it was issued for, so the relative fetch that resolves to arrives at MinIO
   unsigned and gets rejected. Added `GET /videos/{video_id}/media/{path:path}`
   (`video_media`) as one stable, authenticated origin the whole tree is
   served from instead, mirroring the claim-check pattern rather than
   bypassing it.
2. **A second browser-native-request auth gap, same shape as ADR-0008's
   EventSource one.** `<video poster>` fetches its image the way
   `EventSource` opens its stream — no code of ours runs, so no header can be
   set. `sse_principal` is renamed `query_or_header_principal` and reused as
   `MediaCaller`: hls.js itself attaches the bearer token via `xhrSetup` on
   every playlist/segment request (the header path, same as every other
   endpoint), so only the single flat poster request needs the
   `access_token` query-param fallback — a query string doesn't survive the
   relative-URL resolution the playlist tree in note 1 depends on.
3. **Verified by hand first, then by `tests/e2e/hls-playback.spec.ts`.**
   Uploaded a video through the real dev stack to `completed` and confirmed
   playback in an actual (non-headless) browser: network tab showed
   poster/master.m3u8/playlist/segment all 200, console clean, picture
   actually moving. The committed spec automates the same upload→completed
   flow and asserts every rendition tile turns ready, `master.m3u8` lists
   each rendition exactly once, the poster loads via the query-param path,
   and hls.js's own fetch of the first segment succeeds — all through
   `video_media`'s auth. It does not assert decoded pixel playback
   (`video.currentTime` advancing); see note 4.
4. **The e2e gate can prove the fetch chain but not decode, and that's a
   CI-browser limitation, not a product gap.** `mcr.microsoft.com/playwright:*-noble`'s
   bundled Chromium has no H.264 license —
   `MediaSource.isTypeSupported("video/mp4; codecs=\"avc1...\"")` is `false`,
   verified empirically inside that exact image, and it ships no
   `google-chrome` to fall back to. hls.js still fetches and parses every
   playlist/segment correctly (proven by note 3's spec); only the final MSE
   `addSourceBuffer`/decode step is unavailable in that environment. Recorded
   as a repo-level environment constraint in AGENTS.md rather than worked
   around — installing Google Chrome or switching the transcode codec are
   both a new dependency (non-negotiable #9) that a test-environment gap
   doesn't justify.

## Phase 10 — Observability `[x]`
Refs: ADR-0010

- [x] `/metrics` on api + every worker; Prometheus scrape config
- [x] `kafka-exporter` wired; consumer-lag panel per group
- [x] OTel spans in every stage, `traceparent` propagated via Kafka headers
- [x] Provisioned Grafana dashboards in `ops/grafana/`: pipeline throughput,
      stage latency histograms, consumer lag, failure/DLQ rate
- [x] Alert rules: lag over threshold, DLQ non-empty, stage p99 regression

**Gate:** `make obs-verify` — dashboards load from provisioning with no manual
setup; one uploaded video yields a **single trace** spanning API → probe →
transcode → package; the lag panel returns non-empty series.

```
driving one real upload through the stack
  PASS  devauth issues a token
  PASS  POST /videos
  PASS  PUT fixture bytes to the presigned URL
  PASS  POST /videos/{id}/complete
  PASS  video reaches completed
tracing (ADR-0010): one video, one trace, every stage
  PASS  Tempo has at least one trace tagged video_id=b1fdf310-a1f5-44b2-a7ad-f986ee59fdbd
  PASS  trace 11d0bca1d256d9e6b8edfebdd3ef4981 spans every required stage
metrics: the lag panel has real data
  PASS  kafka_consumergroup_lag_sum returns a non-empty series
dashboards: provisioned with no manual setup
  PASS  all four dashboards provisioned with no manual setup

OBS-VERIFY PASSED — dashboards provisioned, one trace per video, lag panel is live
```

**Discovered mid-implementation — the checklist looked closer to done than it
was.** `libs/pipeline/obs.py`/`health.py` already implemented most of
ADR-0010 before this phase's first commit; reading the actual wiring (not
just the checklist) found several silent gaps, not a blank slate:

1. **`worker_transcode` never called `setup_tracing()`** — every other
   service did. Its spans were created against the OTel SDK's default
   no-op tracer provider, since nothing ever installed a real
   `TracerProvider` in that process. The longest, most important stage was
   invisible in every trace, silently — this is exactly what the phase's
   gate is supposed to catch, and did, once the gate existed to run.
2. **No span anywhere ever got `video_id` set as an attribute.** Trace
   propagation itself already worked (Kafka header injection/extraction,
   per-stage child spans) — there was just no way to ask Tempo for "the
   trace for *this* video", the actual stated purpose of tracing in
   ADR-0010. Fixed with one line per place a video_id becomes known
   (`consumer.py`'s `_invoke_handler`, covering every `StageWorker`-based
   service for free, plus each `api/main.py` route with a video_id).
3. **`stage_in_flight_seconds` and `sse_connections_active` were both dead
   metrics** — defined, registered, scraped, and always reporting `0`. The
   first because `observe_stage`'s `finally` block unconditionally reset it
   instead of ever reflecting real elapsed time; the second because nothing
   ever called `.inc()`/`.dec()` on it. Neither would have been caught by
   "does `/metrics` return 200" — both needed a real workload and someone
   to check the actual values, which `obs_verify.py`'s real-upload-then-query
   approach now does.
4. **`sse_connections_active` is an unlabeled `Gauge` in a module shared by
   every service** (`obs.py`), so every worker process registers it too,
   always stuck at `0` since only the API route ever touches it. Caught
   while verifying the Pipeline Overview dashboard's SSE panel against the
   real running stack — it returned 6 series instead of 1. Fixed with
   `job="api"` + `sum()` in the panel query; the metric itself is unchanged
   (still process-global by construction).
5. **`infra/obs_verify.py`'s first version picked the wrong trace** when
   more than one request happened to tag the same `video_id` (every status
   poll does). It guessed by reported trace duration, which doesn't
   reliably reflect when the last async, Kafka-consumer-side span
   finished. Fixed by fetching every candidate trace and picking whichever
   one actually covers every required service, not the "longest" one.

**Deliberately out of scope, documented rather than fixed:**
`stage_duration_seconds`'s `rendition` label is always `"none"` in
practice. `_invoke_handler` enters the metrics/span context *before*
parsing the event (so it can time and record the parse itself as part of
the stage's work), and the rendition isn't known until after parsing —
threading it through would mean restructuring parse-error semantics (a
parse failure currently still counts as a timed stage failure; moving
parsing outside the context would change that). Not worth the behavioral
risk for a label.

The **Infrastructure** dashboard is honestly scoped: cAdvisor (container
CPU/memory) isn't part of this dev compose stack, and adding it is a new
dependency (non-negotiable #9) that a documentation phase doesn't justify
on its own — the dashboard says so in its own text panel rather than
shipping panels with permanently no data.

## Phase 11 — Notify & failure UX `[x]`

- [x] `worker_notify`: webhook on `video.completed`, own consumer group, own retries
- [x] Failure surfacing: DLQ'd renditions show as failed in the UI with a reason
- [x] Manual replay endpoint/CLI: DLQ → source topic
- [x] **Retry-tier pump** — a service that drains each `<topic>.retry.<tier>`
      once its delay elapses and republishes to `<topic>`. ADR-0002 and
      ADR-0009 named this component from the start ("retry pump" appears in
      both), but no phase ever scheduled building it, and none of the
      retry-routing tests (Phase 4 onward) exercise redelivery *from* a retry
      topic — only that a failed message is correctly *produced to* one.
      Discovered running Phase 8's real compose stack for the first time: a
      transient failure has been equivalent to a **silent, permanent drop**
      since Phase 5, contradicting the DoD in AGENTS.md ("failures route to
      retry or DLQ ... never a silent drop"). Not fixed here — it's a new
      service, out of scope for a frontend phase — but it must land before
      this system is called reliable, and ideally before Phase 14 load
      testing, which will generate transient failures for real. Concretely
      demonstrated in this same session: wiring worker-probe into
      docker-compose.yml hit a tmpfs-permission bug (see that commit) that
      made the first message fail transiently. With no pump to redeliver it,
      that message would have been stranded on `video.probe.retry.10s`
      forever rather than self-healing once the compose fix landed.

**Gate:** `make e2e ARGS="--grep failure"` — a corrupt upload shows a failed state in
the UI with a reason, the webhook fires for a successful one, and a DLQ replay
drives the video to completion. The retry-pump item additionally needs its own
redelivery test: produce a TRANSIENT failure, assert the message reaches `<topic>`
again after (not before) its tier's delay, with no earlier delivery.

```
$ make e2e ARGS="--grep failure"
  ✓  1 failure-and-notify.spec.ts:17:1 › failure UX: a corrupt upload reaches
     failed with a reason, not stuck pending (13.8s)
  ✓  2 failure-and-notify.spec.ts:49:1 › failure UX: webhook-sink receives a
     notification for a completed video (13.1s)
  2 passed (31.5s)

$ make replay-verify
seeding a real, valid upload straight onto video.uploaded.dlq
  PASS  devauth issues a token
  PASS  POST /videos
  PASS  PUT the real fixture to the presigned URL
  PASS  seed video.uploaded.dlq with a valid-file message
  PASS  publish video.status=uploaded (what /complete would have)
confirming it's genuinely stuck, not progressing on its own
  PASS  video is stuck at uploaded before replay (DLQ isn't auto-drained)
running the real `make replay` CLI
  PASS  make replay's own CLI (infra/replay.py) exits 0 — replayed 1 message(s)
        from video.uploaded.dlq to video.uploaded
polling for completion
  PASS  replayed video reaches completed (not failed, not stuck) — got 'completed'
REPLAY-VERIFY PASSED — a DLQ'd valid-payload message reached completed via replay

$ make integration ARGS="-k retry_pump"
tests/integration/test_retry_pump.py .                                   [100%]
1 passed, 75 deselected in 44.00s

$ make integration
tests/integration/test_package.py ........                               [ 10%]
tests/integration/test_probe.py ......                                   [ 18%]
tests/integration/test_projector.py ........                             [ 28%]
tests/integration/test_retry_pump.py .                                   [ 30%]
tests/integration/test_sse.py ...........                                [ 44%]
tests/integration/test_thumbnail.py ....                                 [ 50%]
tests/integration/test_transcode.py .....                                [ 56%]
tests/integration/test_upload.py ..........................              [ 90%]
tests/integration/test_video_media.py .......                            [100%]
76 passed, 5 warnings in 359.06s (0:05:59)
```

**Discovered mid-implementation:**

1. **The gate text itself had the stale `-k` bug** — `ARGS="-k failure"` is
   pytest's flag; Playwright has no `-k`, only `--grep`, so this line would
   have silently run every e2e spec unfiltered. Same class of bug already
   caught and fixed for Phase 8's and Phase 9's gate text earlier this
   session; fixed here too, plus in the summary table above.
2. **The plan's assumed gap in `_apply_failure` didn't exist.** It already
   wrote the rendition-level `status`/`failure_reason` row on a
   rendition-scoped `PipelineFailed` (an `on_conflict_do_update` block from
   Phase 9, unused on this path until now) — the real gap was one layer up,
   in `services/api/sse.py`'s `sse_event_name()`, which mapped every
   `pipeline.failed` row to the bare `"failed"` event name regardless of
   payload shape. Caught by reading the actual code before implementing,
   not by trusting the plan's description of it.
3. **A genuinely corrupt file can never prove the DLQ-replay-to-completion
   leg of this gate.** `worker_probe`'s ffprobe failure on unparseable
   input is `TerminalError` — it fails *identically* on every attempt, so
   replaying the same bad bytes just re-dead-letters them. The only
   scenario where a replay is meaningful is the real-world one it exists
   for: a message dead-lettered for a reason since resolved, whose
   underlying data was fine all along. `infra/replay_verify.py` builds
   exactly that — a real, valid upload whose `video.uploaded` is seeded
   straight onto `video.uploaded.dlq` instead of the live topic (as if some
   now-fixed condition had dead-lettered it), then replayed via the actual
   shipped `infra/replay.py` CLI as a subprocess, not a reimplementation of
   its logic. New `make replay-verify` target, separate from `e2e` for the
   same reason `obs-verify` already is: it needs `api` in its normal
   (non-e2e-profile) `S3_PUBLIC_ENDPOINT`, which `make e2e` only restores on
   its way out — the replay-verify run's first attempt failed at the
   presigned-PUT step for exactly this reason before that was caught.
4. **This also means the third gate leg isn't inside
   `tests/e2e/failure-and-notify.spec.ts`.** `make replay` is a host CLI
   action by ADR-0005's own stated design (`make replay TOPIC=… VIDEO=…`),
   not an HTTP endpoint the Playwright container — which has no Docker
   socket and isn't on the host — could ever invoke. The spec covers the
   two legs that are genuinely browser-observable (failed-with-a-reason,
   webhook delivery); `replay_verify.py` covers the third for real, on its
   own re-runnable gate.
5. **None of the three new service images (`worker-notify`,
   `worker-retry-pump`, `webhook-sink`) had ever been built via
   `docker compose --profile app up --build`** before this phase's closing
   verification — every prior check used `make integration ARGS="-k ..."`,
   which never touches the compose stack. Built and confirmed all three
   reach `healthy` (including `worker-notify`, which would have
   crash-looped on start if `NOTIFY_WEBHOOK_URL` resolved wrong) before
   writing a single line of the e2e spec, on the advisor's flag that this
   was cheaper to catch now than inside a Playwright timeout.

**Deliberately out of scope, carried forward from an earlier commit's own
note:** the rendition-level failed tile (`.tileFailed`, the ✗ mark,
`rendition.failed` SSE event) is covered by an integration test
(`test_sse.py`'s `test_a_rendition_failure_streams_live_as_rendition_failed`,
driven at the projector/Kafka/broadcaster layer) but not a browser e2e
test. Engineering a real "one rendition fails, others succeed" pipeline
run needs fault injection this codebase doesn't have — every failure this
phase's e2e coverage can trigger for real (a corrupt upload) fails at
`worker_probe`, before any rendition-specific work starts, so it can only
ever exercise the whole-video failure path, which is what the gate's own
wording asks for ("a corrupt upload shows a failed state in the UI").

**Also learned, closing this phase's gate:** running `make integration`
concurrently with the real `docker compose --profile app` stack (left
running from the e2e verification just before it) produced 76 spurious
setup errors, not 76 real failures — testcontainers' own dynamic-port
containers never collided on ports, but a laptop running ten-plus
app-profile containers alongside a fresh Kafka/Postgres/MinIO trio per
test module starved something (never root-caused further, not worth the
time). Stopping the app profile first (`docker compose --profile app
--profile e2e stop`) made the exact same suite pass clean. Not a code bug;
recorded here since it cost real time to notice and would again.

**Two more caught by a second advisor pass after the first version of this
commit:** `ci: lint unit integration e2e` never ran `replay-verify` — the
leg just built to close this phase's own gate was invisible to Phase 12's
gate ("`make ci` green"). `e2e`'s own recipe already restores `api` to its
normal (non-e2e-profile) `S3_PUBLIC_ENDPOINT` unconditionally on the way
out, so `replay-verify` can safely run right after it in the same `ci`
chain — verified for real (not assumed): ran the exact `e2e` → restore →
`replay-verify` sequence by hand and it passed. `ci` now reads `lint unit
integration e2e replay-verify`. Also, `check_stuck_before_replay`'s first
version read the video's status exactly once right after seeding — a real
flake against a freshly-restarted stack, since the seeded
`video.status=uploaded` event can take a couple of seconds to reach the
projector's consumer; a single-shot check can catch the row before it
catches up and fail on a status that was correct, just not yet applied.
Fixed with the same poll-with-timeout idiom `wait_for_completion` already
uses.

## Phase 12 — Production hardening `[ ]`
Refs: ADR-0015

- [x] Graceful SIGTERM shutdown everywhere: stop consuming, finish or cleanly
      abort in-flight work, commit offsets, flush producer, close
- [x] **Media-worker containment** — non-root, read-only rootfs + tmpfs scratch,
      `cap-drop ALL`, `no-new-privileges`, seccomp, no network egress beyond
      Kafka/S3/Postgres, CPU/memory/wall-clock limits, ffmpeg `-threads` cap.
      non-root/read-only/cap-drop/no-new-privileges were already true from
      earlier phases; wall-clock timeouts already existed per ffmpeg call;
      this item added `FFMPEG_THREADS` (capped at 2, threaded through
      transcode/thumbnail's argv builders — hls.py's `-c copy` remux
      deliberately excluded, no decode/encode for `-threads` to bound),
      per-worker `cpus`/`mem_limit` in compose sized from real `docker stats`
      numbers during an actual upload (not guessed), an explicit documented
      decision to use Docker's default seccomp profile rather than author a
      custom one (a real, easy-to-get-wrong undertaking disproportionate to a
      project with no live deployment target), and — landed alongside item
      13's gate script, which is what actually proves it — a new
      `media-internal` Docker network (`internal: true`) carrying kafka/
      postgres/minio plus the three ffmpeg-touching workers (probe/transcode/
      thumbnail) only; verified both directions for real (egress genuinely
      worked before the network existed, genuinely fails after) rather than
      trusting the config alone, per the advisor's explicit warning that
      `docker compose config` accepts plenty that doesn't behave as expected.
- [x] Input allow-list from ffprobe before transcoding; no `shell=True` anywhere;
      filenames derived from `video_id`, never from the uploaded name
- [x] Backpressure / concurrency caps per worker; resource limits in compose
- [ ] Kafka prod profile: RF=3, `min.insync.replicas=2`, `acks=all`, no unclean
      leader election, auto-create off, TLS+SASL, per-topic retention
- [x] Prometheus multiprocess mode wired in the API entrypoint (ADR-0014 gotcha)
- [x] Backups: Postgres PITR with a **rehearsed restore**; object versioning +
      lifecycle rules for `tmp/` and abandoned uploads. Lifecycle rules for
      `tmp/` and stale multipart uploads already existed (Phase 1's
      `infra/bootstrap_minio.sh` — checked, not re-done); object versioning
      did not — added `mc version enable` there (ADR-0015's own backups
      section names sources/renditions as the irreplaceable data, the same
      reasoning as Postgres PITR below, applied to the object store), run
      for real against the live dev bucket and confirmed idempotent
      (`local/videos versioning is enabled`, twice in a row). New
      `docker-compose.prod.yml` (first content of this file — item 7 and
      Phase 13 both extend it later, never replace it): a `postgres` service
      override adding `archive_mode=on`/`archive_command`/`wal_level=replica`
      and a `pg-archive` volume. New `infra/backup_verify.py`
      (`make backup-verify`): writes a "keep" row, captures the exact server
      timestamp via `clock_timestamp()`, writes a "drop" row, destroys the
      live data directory, restores the base backup, replays WAL to that
      timestamp, and asserts keep survived while drop didn't — proving
      recovery reached a specific point, not just "a backup exists."
      Runs under its own isolated `docker compose -p video-pipeline-pitr`
      project, never the dev stack's default one, since destroying a data
      directory on purpose demands it be structurally impossible to touch
      the shared `pg-data` volume other phases depend on — found and fixed
      two real isolation gaps while building this: `container_name` is a
      literal, ignoring the project prefix, so the override had to give it
      a distinct name (`vp-postgres-pitr`) or it would collide with a
      running dev `vp-postgres`; and Compose *concatenates* list-valued
      fields like `ports` across `-f` files rather than replacing them
      (verified via `docker compose config`, not assumed), so an empty
      `ports: []` override left the base file's `5432:5432` mapping intact
      — fixed by overriding `POSTGRES_PORT` in the script's own subprocess
      environment instead, which the base file's own interpolation picks
      up. Also found rehearsing by hand before writing the script: a fresh
      named volume is root-owned, so `archive_command` (running as the
      `postgres` user) failed silently on every attempt until the script
      explicitly `chown`s `/archive` after first boot. `make backup-verify`
      run for real — PASSED — then reran to confirm teardown left nothing:
      no `vp-postgres-pitr` container, no `video-pipeline-pitr_*` volumes.
- [x] CI workflow: lint → unit → integration → build+Trivy scan+SBOM → e2e.
      `.github/workflows/ci.yml`, seven jobs, each calling the exact `make`
      target used locally (no parallel bespoke script) — `lint`/`unit` run
      first, `integration` and `build-and-scan` both gate on them, `e2e`
      needs both, `replay-verify`/`security-verify` (the other two legs of
      `make ci`, plus the phase's own containment gate) run after `e2e`.
      `build-and-scan` builds all ten images, scans each with Trivy at the
      same HIGH/CRITICAL/`--ignore-unfixed` gate `security_verify.py` uses
      (informational here — `exit-code 0` — since `security-verify` is the
      actual pass/fail gate for known findings) and uploads a CycloneDX SBOM
      per image as a build artifact. `make` itself needs no separate
      language setup step: its own `uv`-or-Docker fallback (Makefile's `UV`
      var) means a bare-Docker runner already has everything `lint`/`unit`/
      `integration` need; only frontend lint (`actions/setup-node`) and the
      SBOM step (`aquasec/trivy` image) add anything GitHub's runner doesn't
      ship. Found and fixed while writing this: `security-verify`'s Makefile
      target depended on `up` but not `migrate` — harmless on this session's
      own already-migrated dev volumes, but would crash-loop the app-profile
      containers against an unmigrated schema on a genuinely fresh runner;
      now `security-verify: up migrate`, matching `e2e`/`replay-verify`'s
      existing precedent. No live runner to trigger this against (no push
      this session, per "commit only when asked") — verified instead with
      `actionlint` against the real file (`docker run rhysd/actionlint`,
      clean) and manual review of the job graph, stated as the real limit on
      verification depth here, not glossed over.
- [x] Migrations backward compatible for one release; run as a pre-deploy job.
      Policy written down in `backend/migrations/README.md` (expand/contract:
      add nullable-or-defaulted, never drop/rename/retype in the same release
      that stops reading the old shape). New `infra/check_migration_compat.py`
      (`make migrate-compat`, folded into `make lint`) — stdlib `ast`, walks
      each `versions/*.py`'s `upgrade()` for a bare `drop_column`/
      `drop_table`/`rename_table`, an `alter_column` rename or type change, or
      an `add_column`/`alter_column` setting `nullable=False` with no
      `server_default`; a `# migration-compat: allow <reason>` comment
      exempts a reviewed line without hiding it from the output. Verified
      both directions, not just "it currently passes": a synthetic migration
      file with one of every unsafe shape (built in `/tmp`, discarded after)
      was correctly flagged for all four, including the exemption comment
      correctly suppressing the fifth from `unresolved`; `0001`–`0006` audited
      for real by running the checker against them, not assumed clean —
      PASSED, confirming the "additive so far" claim empirically.
- [x] SLOs defined; alerts fire on symptoms (lag, DLQ depth, SLO burn). The
      symptom-alerts half was already true (5 alerts already firing on
      symptoms in `ops/prometheus/rules/pipeline.yml` from Phase 10, verified
      by reading the file rather than assumed from recon). What this item
      added: a concrete SLO table in ADR-0015 §9 replacing its prose,
      explicitly cross-referencing each target to the alert that watches it.
      One SLO (time-to-first-rendition p95) genuinely has no alert — it spans
      probe→queue→transcode and nothing emits a single duration for that
      today (a named Phase-10 gap, already flagged in
      `pipeline-overview.json`'s own panel description, not newly
      discovered). Deliberately did **not** invent a threshold to alert on:
      this repo's own convention already rejects a guessed number
      (`pipeline.yml`'s "Thresholds are placeholders until Phase 10 measures
      real numbers"), and Phase 14 exists specifically to measure it. Noted
      as observable-but-not-yet-alertable (per-video, via the trace each
      upload already produces) rather than silently left off the table.
- [x] README quickstart; ADR index current; PLAN.md diagram matches reality.
      New root `README.md` (didn't exist at all): quickstart (`make up` →
      `migrate` → `smoke` → app profile → frontend or curl), observability,
      the everyday `make` commands, project layout — verified against the
      real repo, not just written from memory (`make help`'s actual target
      list, the real 4 provisioned Grafana dashboards via `ls
      ops/grafana/dashboards/`). ADR index (`docs/adr/README.md`) already
      current through 0016 — no new ADR landed from any Phase 12 item, so no
      edit needed. `PLAN.md`: diagram and component table already match
      reality (checked — no service is missing that belongs there); fixed a
      real staleness instead: the top status line still read "planning
      complete, implementation not started" despite eleven phases being
      done. `webhook-sink` deliberately **not** added to the component
      table — checked the stated precedent (`devauth`, the other dev/test
      double) and found it has no row there either, so matching precedent
      means neither gets one, not that webhook-sink should.
- [x] **Known gap from Phase 5** — `RenditionRepository.claim()`'s stale window
      (`STALE_AFTER = 2h`) is longer than the retry ladder's total span
      (10s/1m/10m). If a worker crashes mid-claim, siblings retrying the same
      rendition hit `TransientError` (claim denied) on every attempt and the
      message DLQs before the 2h stale window ever frees the claim — even
      though nothing is actually wrong with the rendition. Fixed by shortening
      `STALE_AFTER` to `5m`, comfortably under the ladder's ~11m10s total span
      so the message's last retry (in the 10m tier) finds the claim stale and
      succeeds instead of dead-lettering. `tests/integration/test_repository.py`
      (new — no test exercised this at all before): a live claim blocks a
      second claimant; a claim back-dated past `STALE_AFTER` frees for one.
      `make integration ARGS="-k test_repository"` — 2 passed, 38.16s.
      `make integration ARGS="-k 'transcode or package'"` — 14 passed, 161.36s,
      confirming no regression in the two stages that actually call `claim()`.

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

**No live target, by design (confirmed with the user 2026-08-28, see PLAN.md's
scope note):** this project is never deployed anywhere real or persistent.
"`<staging-url>`" in the gate below means standing the hardened profile up
locally (or a local kind/minikube cluster if K8s is pursued) long enough to
run the e2e suite against it, then tearing it down — not a cloud host or
anything left running.

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
- [ ] Phase 8 finding #6 is mitigated (ADR-0004 follow-on: crash-and-restart
      on a stalled rebalance), not root-caused. What actually stalls
      librdkafka's rejoin for minutes under CPU contention, rather than the
      seconds an ordinary rebalance takes, is still open — this phase's
      sustained pressure (not just a laptop rebuilding five images back to
      back) is the right place to reproduce it deliberately and find out.

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
| 2026-08-28 | Phase 11 closed `[x]`: `worker_notify` (webhook on `video.completed`/`pipeline.failed`), `webhook-sink` test double, `worker_retry_pump` (drains every retry-tier topic, republishes after its delay), rendition-level failure surfacing (`_apply_failure` already wrote the row; `sse_event_name()` was the actual gap), `infra/replay.py` (`make replay`, stdlib CLI), `infra/replay_verify.py`/`make replay-verify` (the DLQ-replay-to-completion leg of the gate, since a truly corrupt file can never prove it) | ADR-0002/0005/0009 all named the retry pump from the start but no phase ever scheduled it, leaving transient failures a silent permanent drop since Phase 5 (flagged in this phase's own checklist note) — the biggest reliability gap left in the system before this phase closed it |
| 2026-08-28 | Phase 10 closed `[x]`: `worker_transcode`'s missing tracer and the missing `video_id` span attribute fixed; manual ffmpeg/ffprobe spans and SQLAlchemy/botocore auto-instrumentation added; `stage_in_flight_seconds`/`sse_connections_active` (both previously dead metrics) fixed and wired; the four remaining ADR-0010 alerts added; four Grafana dashboards written and verified against the live stack; `make obs-verify` implemented and green | Most of ADR-0010's design was already scaffolded in `libs/pipeline/obs.py`/`health.py` before this phase's first commit — reading the actual wiring instead of the checklist found the gaps were in *using* it, not building it, and the most important one (transcode's spans being silent no-ops) is exactly the kind of thing that looks fine until the gate this phase adds actually checks |
| 2026-08-28 | Documented, in `PLAN.md` (scope note near the top) and `PROGRESS.md` (Phase 13's assumption callout): this project is never deployed to a real/live environment — "production ready" is a demonstrated code/architecture standard by the end of development, not an operational target | User confirmed directly, after the server-side-cache discussion; needed to be in the repo itself (not just session memory) so it survives a cleared context or a new session, per the user's own ask |
| 2026-08-28 | Decided **against** a server-side cache in front of `video_media` for now — no code change | No shared/public-video concept exists (ADR-0016: every video is single-owner, checked per request), so "many different clients hammering the same object" isn't a load pattern this app currently produces; the client-side `immutable` cache already covers the realistic case (same owner, repeat requests). A real shared cache/CDN would conflict with the per-request `Authorization` check as designed — a genuine redesign (signed per-segment URLs or an auth-aware CDN), ADR-worthy on its own if load evidence ever justifies it (Phase 12/14), not a quick patch now |
| 2026-08-28 | `video_media` now sends `Cache-Control: private, max-age=31536000, immutable` on every response | It set no caching headers at all, so hls.js re-requesting a rendition it already fetched (its own buffer-flush behavior on every quality switch) round-tripped through MinIO every time — user-reported, seen live. Safe to cache indefinitely: every object this route serves is written once and never overwritten with different content, the same idempotency invariant `worker_transcode`/`worker_package` already rely on |
| 2026-08-28 | `VideoPlayer` gained a manual quality control: a gear button overlaid on the video (top-right, off the native `<video controls>` bar) opening a menu of `Auto` + each rendition, driving hls.js's `currentLevel` directly. Covered by `tests/e2e/hls-quality-selector.spec.ts`. `PLAN.md`'s `web` row updated to mention it | hls.js (already chosen, ADR-0014, specifically over a native player because it exposes level control) already carries `levels`/`currentLevel`; ADR-0012's own scoping test ("new topology or just a flag?") says this isn't new pipeline topology, so no new ADR. Labels come from the rendition name embedded in each level's playlist URL, not `Level.height` — `master.m3u8` carries no `RESOLUTION=` attribute and no rendition has real width/height persisted anywhere, so `Level.height` is `0` for every level |
| 2026-08-28 | Fixed a live bug found by hand (not by any existing test): `useVideoEvents`'s `snapshot` handler now closes the stream itself when the snapshot's own `video.status` is already terminal. Regression covered by `tests/e2e/sse-terminal-video.spec.ts` | ADR-0008's terminal-event contract (Phase 7 note, "not addressed, documented instead") only covers a video *becoming* terminal mid-stream — a fresh connect/reload against a video already completed got only a `snapshot` event before `sse_stream` returned, so `closedForGood` never got set; `EventSource` treated the clean close as an error and reconnected roughly once a second, forever |
| 2026-08-28 | `tests/e2e/hls-playback.spec.ts` added and green against `make e2e ARGS="--grep hls"`; Phase 9 closed `[x]` | The CI browser (`mcr.microsoft.com/playwright:*-noble`) has no H.264 license — verified empirically, recorded in AGENTS.md's environment constraints — so the spec asserts the authenticated fetch chain (master/rendition playlist/segment/poster all reachable through `video_media`), not decoded playback |
| 2026-08-28 | Player landed: `video_media` authenticated media proxy + hls.js `VideoPlayer`; `sse_principal` renamed `query_or_header_principal`, reused as `MediaCaller`. Phase 9's line items closed `[x]`; the phase heading itself stayed `[ ]` pending the e2e gate below | A presigned MinIO URL can't serve a playlist tree — every relative reference inside it (rendition from master, segment from rendition) resolves unsigned |
| 2026-08-27 | `worker_package` landed (ADR-0013 follow-on): fan-in join keyed on worker-owned claim columns, not projector-owned read-model columns; SSE termination widened to `completed` | The originally-specified join read columns written from a different, unordered topic — would have hung the last video of every batch with no error, caught before any code was written |
| 2026-08-27 | `worker_transcode` gained HLS segmentation (remux, not re-encode); idempotency widened to MP4-and-playlist | An attempt dying between the MP4 and playlist promotes is a real, tested state, not hypothetical — a single-object existence check silently missed it |
| 2026-08-27 | Phase 9 schema landed (migration 0005); `worker_thumbnail` landed and its checkbox closed | Poster/sprite/VTT/master-playlist columns needed before any Phase 9 worker could write to them; thumbnail chosen first as the no-join, fastest-verifiable slice |
| 2026-08-25 | Plan, tracker and ADRs 0001–0013 written | Project kickoff |
| 2026-08-26 | Phase 5 transcode workers landed; the rebalance test survives a real KRaft broker with a reduced poll interval | "No duplicate output" proved to be a non-discriminating assertion — rewritten to handler-invocation-count before being trusted |
| 2026-08-26 | Phase 4 probe stage landed; ladder is data-dependent and the plan/fan-out invariant is asserted | Gate split into an ffmpeg-free integration check and an in-image ffprobe check, rather than reinterpreting the original wording |
| 2026-08-26 | Phase 3 upload path landed; integration gate green with 20 tests | Caught three production-fatal bugs (lz4, greenlet, lifespan instrumentation) that no unit test could reach |
| 2026-08-26 | Per-user isolation confirmed (ADR-0016); object keys gain an owner prefix; Phase 3 gains auth/quota tasks and a Phase 14 for load testing | The user confirmed real-world intent and load testing, making tenancy a design input rather than a retrofit |
| 2026-08-26 | Phase 2 contracts library landed; `make unit` green with 71 tests, `make lint` clean | The ADR-0004 eviction loop is now covered by unit tests rather than only by prose |
| 2026-08-26 | Phase 1 infra landed; gate green from a clean slate. Three service-dependent checkboxes moved to Phase 2 | Nothing to configure, health-check or containerise until services exist |
| 2026-08-26 | Frontend baseline corrected from React 18 to React 19 in PLAN.md and ADR-0014 | 18 was a stale default, not a decision; 19 is stable and unblocked by our stack |
| 2026-08-25 | ADR-0014 (library stack) and ADR-0015 (production readiness) added; Phase 12 expanded, Phase 13 added | Target is a production-grade app, so dependency and hardening choices are decided up front |

# ADR-0007: PostgreSQL as a Kafka-fed read model, written only by the projector

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The UI needs to list videos, show each rendition's state, and — critically —
give a client that connects late a **snapshot** of what has already happened
(ADR-0008). Kafka can replay, but scanning topics to answer "what is the state of
video X" is not a query pattern.

The tempting shortcut is: every worker writes its own row *and* emits its event.
That is a dual write. The two can diverge on any partial failure — the DB commits
and the produce fails, or the reverse — and the divergence is silent.

## Decision

**PostgreSQL 16 as a read-model projection. Kafka is the source of truth for the
flow; the database is derived.** A single `projector` service (one consumer
group) consumes `video.status` and is the **only writer** of pipeline state.

Schema sketch:

```sql
videos(id pk, filename, status, duration_s, width, height,
       expected_renditions text[], created_at, updated_at)

renditions(id pk, video_id fk, rendition, status, object_key,
           attempt int, failure_reason, completed_at,
           UNIQUE (video_id, rendition))          -- also the idempotency key

events(id bigserial pk, video_id fk, event_id uuid unique,
       type, payload jsonb, created_at)           -- append-only; Last-Event-ID replay
```

Rules:

- Projector writes are **upserts**, so replaying a partition is safe.
- The offset is committed **after** the DB transaction commits. A crash in
  between replays the event; the upsert absorbs it.
- **Column ownership rule** (the precise form of "only writer"):

  | Column kind | Examples | Owner |
  |---|---|---|
  | *State* — anything the UI reads or the pipeline branches on | `videos.status`, `renditions.status`, `object_key`, `duration_s`, `expected_renditions` | **projector only**, via upsert from `video.status` |
  | *Claim* — worker coordination, never projected, never rendered | `renditions.attempt`, `renditions.claimed_at`, `videos.packaging_claimed_at` | **the claiming worker**, via `UPDATE … WHERE … RETURNING` |

  Claims are how workers elect a single owner for expensive work (ADR-0005's
  transcode claim, ADR-0013's packaging claim). They are mutual exclusion, not
  state: the projector never writes them, and nothing outside the claiming stage
  reads them. Any new column must be classified into one of these two kinds
  before it is added — a worker writing a *state* column is the dual-write bug
  this ADR exists to prevent.
- `events` is append-only and indexed on `(video_id, id)` — it backs SSE resume.
- Migrations with Alembic, checked into the repo, run as a job before services start.

## Alternatives considered

- **Every worker writes its own state row.** Rejected: dual write, silent
  divergence, and no single place to reason about state transitions.
- **Redis only.** Rejected: no durable history for SSE replay or auditing, and no
  relational queries for listing/filtering. Redis remains a candidate for
  rate limiting and presign caching, not for pipeline state.
- **MongoDB.** Rejected: the data is relational (video → renditions), the volume
  is small, and Postgres' `jsonb` already covers the flexible payload need.
- **Event-sourcing from Kafka with no database (query by replay).** Rejected:
  answering "state of video X" would require scanning a topic; a compacted state
  topic plus an interactive-query layer is essentially reimplementing a database.
- **Kafka Streams / ksqlDB state stores.** Rejected: JVM stack in a Python repo.

## Consequences

- The read model is **eventually consistent** with the pipeline. The UI must
  tolerate a rendition being finished on disk a beat before the row updates —
  which is exactly what the SSE delta stream smooths over.
- The projector is a single point of staleness: if it lags, the whole UI lags.
  Its consumer lag gets a dedicated Grafana panel and alert (ADR-0010).
- Rebuilding the read model is a supported operation: truncate, reset the
  projector group to earliest, replay.

## Follow-on decision: `video.status` must be self-sufficient (2026-08-26)

Discovered while building the projector. This ADR's decision text says the
projector "consumes `video.status`", but the schema it upserts into
(`videos.duration_s/width/height/expected_renditions`) only existed, before
now, on `video.probed` — an internal stage topic `video.status` was never meant
to expose (ADR-0002's reshape seam). A projector that read `video.probed`
directly to fill those columns would work, but it would not fix the same gap in
the SSE gateway (ADR-0008), which is architecturally pinned to `video.status`
only and needs the same data for "placeholders from the probed ladder" (Phase
8). Reading two different topic sets to answer the same question in two
services is itself a drift risk, so the fix is made once, upstream:

- **`VideoStatusChanged` gains optional fields**: `duration_s`, `width`,
  `height`, `expected_renditions` (populated by the probe worker),
  `rendition_object_key`, `rendition_size_bytes` (populated by the transcode
  worker, and always paired with `rendition` being non-null — never set for a
  video-level status change). All new fields are `| None = None`; this is
  additive under ADR-0003 and does **not** bump `SCHEMA_VERSION`.
- **The projector's actual input is `video.status` *and* `pipeline.failed`**,
  both already listed as `consumed_by: ["projector", ...]` in
  `topics.json` — the registry had already anticipated this; the ADR's prose
  undersold it. `pipeline.failed` carries `stage`/`reason`/`terminal`/
  `rendition` directly (ADR-0005), which is exactly what `videos.failure_reason`
  and `renditions.failure_reason` need; duplicating those fields onto
  `video.status` instead would be the dual-write shape this ADR exists to
  prevent. Every `pipeline.failed` event this system currently emits has
  `terminal=True` (`_emit_pipeline_failed` is only called on the DLQ branch —
  ADR-0005's follow-on above), so the projector treats its arrival as
  unconditionally terminal: set `status="failed"`, `failure_reason=reason`, and
  — when `rendition` is set — the same on that rendition's row.
- **`object_key` is deliberately not a bare shared field.** The projector
  branches on `rendition is not None` to decide whether an incoming status
  event is about the video or one of its renditions, and only
  `rendition_object_key`/`rendition_size_bytes` are read in the
  rendition-is-set branch — a test asserts this explicitly, so a future field
  addition can't quietly reintroduce the ambiguity a bare `object_key` would
  have created.

### Known limitation: no monotonicity guard on `videos.status`

Per-video ordering on `video.status` (keyed by `video_id`) means normal flow
cannot regress state. A manual DLQ replay that reinjects a stale event out of
order could move `videos.status` backwards (e.g. `completed` → `transcoding`).
Not addressed in Phase 6: DLQ replay is a deliberate, rare, operator-initiated
action (ADR-0005), not a normal-flow concern, and a state-ordering guard adds
real complexity (a total order over `VideoState` including terminal states)
for a scenario that does not occur otherwise. Tracked as a Phase 12 hardening
item alongside the claim-window gap found in Phase 5.

### Consequences

- `duration_s`/`width`/`height`/`expected_renditions` now appear on two event
  types (`VideoProbed` and `VideoStatusChanged`). This is intentional
  duplication across an internal-vs-client-facing seam, not drift: `VideoProbed`
  remains the internal pipeline record: nothing new consumes it.
- Any future stage that must move a *state* column into the read model follows
  the same pattern: add optional fields to `VideoStatusChanged`, never widen
  what the projector or the SSE gateway consume beyond `video.status` +
  `pipeline.failed`.

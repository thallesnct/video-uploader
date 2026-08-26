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

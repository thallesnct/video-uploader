# ADR-0005: Idempotency, retries and dead-letter handling

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

Kafka gives at-least-once delivery. Rebalances, crashes between "work done" and
"offset committed", and manual replays all cause redelivery. A duplicate
transcode is wasted CPU-minutes and, worse, can corrupt a read model
(double-counting) or a downstream join (packaging fired twice).

Failures also differ in kind: a transient S3 timeout should be retried; a corrupt
input file will fail identically forever and must not be retried forever.

## Decision

**Idempotent consumers, tiered retry topics, terminal DLQ.**

**1. Idempotency.** Before doing work, each consumer checks a natural idempotency
key — for transcode, `(video_id, rendition)`:

- if the DB row is `completed` **and** the output object exists → ack and skip;
- otherwise claim it with
  `UPDATE renditions SET status='processing', attempt=attempt+1
   WHERE (video_id, rendition)=… AND status <> 'completed' RETURNING id`
  — no row returned means someone else owns it.

`status='processing'` here is the claim column's value, not the projected state
the UI reads — see ADR-0007's column-ownership rule; the user-visible status
still arrives via the projector from `video.status`. The `(video_id, rendition)`
uniqueness constraint is the backstop. Output objects
are written to a temp key and moved into place, so a partial file is never
mistaken for a completed rendition.

**2. Producer side.** `enable.idempotence=true`, `acks=all`,
`max.in.flight.requests.per.connection=5`, `retries=MAX` — no duplicates from
producer retries, no silent loss on leader failover.

**3. Retries.** Non-blocking, tiered retry topics rather than in-place sleeping:

```
rendition.requested → (transient failure) → rendition.requested.retry.10s
                    → rendition.requested.retry.1m
                    → rendition.requested.retry.10m
                    → rendition.requested.dlq
```

A retry pump consumes a delay topic, waits until `occurred_at + delay`, and
republishes to the source topic. Headers carry `retry_count`, `failure_reason`,
`original_topic`. Retrying in place with `sleep()` would block the partition and
re-trigger ADR-0004's eviction.

**4. Failure classification is explicit**, in code:

| Class | Examples | Action |
|---|---|---|
| Transient | S3 5xx/timeout, broker unavailable, OOM-killed | retry tier |
| Terminal | unparseable input, unsupported codec, ffmpeg exit 1 on valid invocation, timeout exceeded | straight to DLQ |
| Poison | envelope fails schema validation | straight to DLQ, never retried |

**5. DLQ.** One `.dlq` topic per source topic, long retention. A `pipeline.failed`
event is emitted so the UI shows the failure with a reason. Replay is a deliberate
operator action (`make replay TOPIC=… VIDEO=…`), never automatic. An alert fires
when a DLQ becomes non-empty.

## Alternatives considered

- **Exactly-once semantics (transactions + `read_committed`).** Rejected: EOS
  covers Kafka-to-Kafka atomicity, but our side effects are S3 objects and ffmpeg
  runs, which no Kafka transaction can roll back. It would add cost and
  complexity while still requiring idempotent side effects.
- **Blocking retry with in-process backoff.** Rejected: blocks the partition,
  triggers rebalance eviction (ADR-0004), head-of-line blocks unrelated videos.
- **Infinite retry, no DLQ.** Rejected: one corrupt file becomes a permanent
  CPU-burning loop and hides every other failure behind its noise.
- **Dedupe by `event_id` in Redis.** Rejected as primary: an extra dependency and
  a TTL guess; the business key plus the object-exists check is stronger and free.

## Consequences

- Every consumer needs a defined idempotency key. Stages without a natural one
  (notify) dedupe on `event_id` in the `events` table.
- Retry topics multiply the topic count; `make topics` generates them from the
  registry rather than by hand.
- A redelivery test is mandatory in every stage's definition of done (AGENTS.md).

## Follow-on decision: non-retryable topics need a DLQ but not a ladder (2026-08-26)

Discovered while building the projector (Phase 6): `video.status` and
`pipeline.failed` are declared `retries: false` in the topic registry, because
their consumers (`projector`, and later the SSE gateway) only ever perform a
cheap idempotent upsert — retrying with a timed backoff buys nothing an
immediate redelivery doesn't. But `retries: false` also skipped creating a
`.dlq` topic (`TopicRegistry.plan()`'s `if not spec.retries: continue` covered
both the ladder and the DLQ), and auto-topic-creation is off everywhere
(ADR-0002). The first time a projector handler raised — a transient DB
connection error is not a hypothetical, it is expected under the load testing
this system is built for — `RetryPolicy.route()` would have returned
`video.status.retry.10s`, and the produce would fail outright against a broker
that was never asked to create it.

Two changes:

- **DLQ topics are now created unconditionally**, independent of the `retries`
  flag. A poison or terminal message needs somewhere to land regardless of
  whether its topic has a timed retry ladder — the ladder and the DLQ are
  separate concerns that happened to share one boolean.
- **`RetryPolicy.route()` takes a `retryable` flag.** For a `TRANSIENT` failure
  on a non-retryable topic it returns `None`: no retry topic, no DLQ, nothing
  produced. `StageWorker` reads this as "leave the offset uncommitted" — the
  message is redelivered on the next poll, which is nearly free for an
  idempotent upsert and does not compound the way a blind commit-and-drop
  would. `TERMINAL`/`POISON` failures are unaffected: they still route straight
  to the (now always-present) DLQ, because an unparseable message left
  uncommitted forever would livelock the partition behind it.

`infra/bootstrap_topics.py` reimplements `TopicRegistry.plan()`'s derivation by
hand (it is a stdlib-only script that runs before the venv exists) and was
updated in lockstep — the exact drift this ADR's registry-as-data approach
exists to prevent.

### Consequences

- A stage with `retries: false` must be safe to redeliver indefinitely on
  transient failure — true today only for upsert-shaped consumers (projector).
  A future non-retryable topic with a non-idempotent consumer would need its
  own review before relying on this behavior.
- No change for any existing `retries: true` topic or stage; every current
  worker's retry/DLQ behavior is unchanged, verified by the existing test suite.

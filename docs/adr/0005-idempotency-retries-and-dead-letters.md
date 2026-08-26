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

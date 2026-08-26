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
  to produce to. `TERMINAL`/`POISON` failures are unaffected: they still route
  straight to the (now always-present) DLQ, because an unparseable message
  left unhandled forever would livelock the partition behind it.

**Corrected mid-implementation:** the first version of this decision had
`StageWorker` leave the offset uncommitted and keep polling forward when
`route()` returned `None`. That is broken. A consumer's fetch position
advances on every `poll()` regardless of commits; only the *committed* offset
is what a restart resumes from. "Leave it uncommitted and move on" means the
next *successful* commit — for a later, unrelated message — advances the
committed offset past the failed one, permanently. Kafka offsets are
monotonic: there is no mechanism to "come back" for a skipped message once
that happens. Under a real transient outage (the DB blips for even a few
seconds) this would silently drop every message that failed during the
window, the opposite of "safe to redeliver."

The actual fix: **`_route_failure` raises the original error when there is no
route**, and nothing catches it. This crashes the worker's `run()` loop and
propagates out of `main()`, terminating the process before it can poll past
the message. Restarting resumes the consumer group from the last *committed*
offset — still before the failed message — so it is redelivered correctly.
This is deliberately a coarse retry (a whole-process restart, not an in-place
backoff) but it is the only mechanism available without a retry-tier topic to
absorb the message, and it composes with Option B's rejection below.

Considered and rejected: giving `video.status`/`pipeline.failed` a normal
retry ladder after all (i.e. reverting to `retries: true`). Rejected because
`video.status` is ordered per-video (ADR-0002) specifically so state
transitions apply in order; routing a failed message through a retry-tier
topic reintroduces it out of order relative to whatever was published to the
main topic in the meantime, turning a stale `probed` landing after
`transcoding` into normal-flow behavior rather than the rare, documented
DLQ-replay case above. There is also no retry-pump service yet to drain a
timed retry topic even if one existed.

`infra/bootstrap_topics.py` reimplements `TopicRegistry.plan()`'s derivation by
hand (it is a stdlib-only script that runs before the venv exists) and was
updated in lockstep — the exact drift this ADR's registry-as-data approach
exists to prevent.

### Consequences

- A crash-to-retry topic needs something to restart it. No such orchestrator
  exists yet for `projector` — it is deliberately not wired into
  `docker-compose.yml` (Phase 13, alongside the other workers). Until then, a
  transient DB failure stops the projector and requires a manual restart; this
  is acceptable for a not-yet-deployed service and must be revisited before
  Phase 13 calls this done.
- Any exception a non-retryable-topic handler can raise must be correctly
  classified. A TRANSIENT misclassification of what is actually a permanent
  condition (e.g. a foreign-key violation from a genuinely missing row) turns
  into an infinite crash-restart loop on the same message, blocking the
  partition forever. The projector's handler explicitly reclassifies
  SQLAlchemy `IntegrityError` as `TerminalError` for exactly this reason —
  see its docstring.
- No change for any existing `retries: true` topic or stage; every current
  worker's retry/DLQ behavior is unchanged, verified by the existing test suite.

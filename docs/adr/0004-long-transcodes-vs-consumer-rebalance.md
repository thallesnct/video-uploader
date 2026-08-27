# ADR-0004: Long-running transcodes vs. consumer group rebalance

- **Status:** Accepted
- **Date:** 2026-08-25
- **Risk:** highest in the system

## Context

A transcode can take minutes to hours. A Kafka consumer must call `poll()` at
least every `max.poll.interval.ms` (default **300 s**) or the coordinator
considers it dead, removes it from the group, and triggers a rebalance.

The failure mode is vicious and easy to misdiagnose:

1. Worker A picks up a 20-minute 1080p transcode and stops polling.
2. At 5 minutes the group evicts A and reassigns the partition to B.
3. The offset was never committed, so **B starts the same transcode from scratch**.
4. B is evicted at 5 minutes too. So is C.
5. The video never completes, CPU is pegged, and the symptom presented to the
   operator is "the queue is stuck" — with no error anywhere.

`session.timeout.ms` / heartbeat threads do **not** save you: since KIP-62,
heartbeats run on a background thread and cover process liveness only.
`max.poll.interval.ms` specifically polices *progress*, and only `poll()` resets it.

## Decision

**Pause the partition and process off-thread while continuing to poll.**

```
consumer.subscribe(topics)                      # max.poll.records = 1
while running:
    msg = consumer.poll(timeout=1.0)
    if msg is None: continue
    consumer.pause(consumer.assignment())       # stop fetching new work
    future = executor.submit(handle, msg)       # ffmpeg on a worker thread
    while not future.done():
        consumer.poll(0)                        # heartbeat + progress signal
        sleep(1)
    result = future.result()
    consumer.resume(consumer.assignment())
    if result.ok:
        consumer.commit(msg, asynchronous=False)   # manual, after success
```

Supporting settings:

- `enable.auto.commit=false` — commit only after the output is durably written.
- `max.poll.records=1` — never hold a batch hostage to one slow item. (Belt and
  braces with librdkafka, whose `poll()` returns a single message by design;
  verified as an accepted property on librdkafka 2.15.0, where an unknown
  property is rejected at construction.)
- `enable.auto.offset.store=false` alongside `enable.auto.commit=false`, or
  librdkafka stores offsets as messages are delivered and the manual commit
  commits work that has not happened yet.
- `max.poll.interval.ms=600000` (10 min) as defence in depth, **not** as the
  primary mechanism.
- A hard ffmpeg timeout per rendition, scaled to source duration; exceeding it is
  a terminal failure routed to the DLQ (ADR-0005), not an infinite retry.
- Graceful shutdown: stop accepting new messages, let the in-flight job finish or
  cancel it cleanly, commit, close. Use `CooperativeSticky` assignment so
  unrelated partitions are not shuffled on every scale event.
- Metric `transcode_in_flight_seconds` and an alert if it approaches the poll
  interval — the early warning for this exact bug.

## Alternatives considered

- **Just raise `max.poll.interval.ms` to hours.** Rejected as the primary fix: a
  genuinely crashed worker then holds its partition for hours before the group
  notices. Pausing keeps liveness detection fast *and* allows long work.
- **Commit the offset immediately, then process.** Rejected: converts
  at-least-once into at-most-once. A crash silently loses the video.
- **Kafka only as a trigger; real queue in Celery/RQ.** A legitimate industry
  pattern, and it sidesteps this entirely. Rejected here because the point of the
  project is Kafka-native processing, and it would add a second broker
  (Redis/RabbitMQ) with its own failure modes and no lag/ordering story.
- **One consumer per process, one message at a time, no pausing.** Rejected: same
  eviction problem, just with fewer partitions to lose.

## Consequences

- Worker code cannot use naive blocking consumption; everyone uses the shared
  loop in `libs/pipeline/consumer.py`.
- Effective concurrency per process is 1 rendition; scale out with more
  processes/pods against more partitions (ADR-0002).
- Integration test (Phase 5) must include a transcode that deliberately outlives
  `max.poll.interval.ms` and assert no redelivery. This test is the reason the
  ADR exists — without it the bug returns silently.

## Follow-on: a rebalance that never lands a new assignment (2026-08-27)

Hit directly, twice, running the real compose stack on a resource-constrained
dev machine: `probe`, `transcode`, and `projector`'s consumer groups all lost
their coordinator at once (`SESSTMOUT` — "session timed out ... after 45131 ms
without a successful response from the group coordinator") during a burst of
unrelated CPU load (rebuilding another container's image). Each logged
"revoking assignment and rejoining group" and then produced no further
activity — no error, no crash, just silence — while videos already probed and
even transcoded sat unrecorded because `projector` never rejoined to apply
them. A plain restart (no rebuild) recovered each one instantly, which rules
out a stuck handler or a bad message: the broker itself was the bottleneck,
and librdkafka's automatic rejoin, whatever it was doing, was not completing.

This ADR's own pause/resume loop was never the problem — it exists to survive
a *long handler*, not a *broker that stops answering group-coordination
requests*. There was no mechanism at all for the second case: `poll()` keeps
getting called forever, keeps returning nothing, and nothing treats "no
assignment for an abnormally long time" as a failure worth acting on.

**Fix: a stall watchdog, not a longer timeout.** Raising `session.timeout.ms`
would only delay the first rebalance, not guarantee the rejoin afterward
succeeds — the observed stalls lasted minutes, not fractions of a session
timeout. Instead, `StageWorker` now tracks how long it has held *no*
assignment (`on_assign`/`on_revoke`/`on_lost` all wired, where before only the
revoke path was) and, from the poll loop itself, crashes with
`ConsumerGroupStalled` once that exceeds `KafkaSettings.consumer_stall_timeout_s`
(180s default — comfortably above any ordinary rebalance, which resolves in
seconds). This is the exact philosophy ADR-0005 already established for an
unroutable transient failure: there is no in-process fix available, so
crashing and letting a fresh process rejoin cleanly is the only path to
recovery. `docker-compose.yml`'s `api`/`worker-probe`/`worker-transcode`/
`projector` all gained `restart: unless-stopped` to make that crash actually
self-heal — the gap ADR-0005's own Consequences section had already flagged
("no such orchestrator exists yet ... requires a manual restart") and deferred
to Phase 13. Landed now instead, because this bit interactive dev use twice
in one session, not just a hypothetical production concern.

A parallel `seconds_unassigned()` accessor is registered as a `/readyz`
dependency check (`kafka_group`) in each worker — diagnostic visibility only,
never the crash trigger itself. Wiring it to `/healthz` instead would be
exactly the mistake `health.py`'s own docstring warns against: a normal,
harmless rebalance would flip liveness during every scale event.

**Deliberately not extended to the API's SSE broadcaster.** `StatusBroadcaster`
(aiokafka, ADR-0008) hit the same symptom in the same incident — but its
consumer group is ephemeral (`sse-gateway-{uuid}`, recreated with the process),
its failure degrades a live update to "reconnect and get a fresh snapshot"
rather than halting the pipeline, and aiokafka's rebalance callbacks are
async — a structurally different implementation, not a copy-paste of this
fix. Flagged as a known gap with the same root cause, not fixed here.

### Consequences

- `stall_timeout_s` is a constructor parameter (defaults to
  `KafkaSettings.consumer_stall_timeout_s`) specifically so tests can set it
  to milliseconds rather than wait out 180 real seconds.
- A worker that crash-loops on `ConsumerGroupStalled` every ~180s without ever
  recovering means the broker itself is down, not just briefly unresponsive —
  that failure mode was already going to page someone via Kafka's own health
  checks; this does not make it worse, it just stops the affected workers
  from sitting silently instead of visibly restarting.
- Still open: what actually stalls librdkafka's rejoin for minutes under CPU
  contention rather than seconds. Not root-caused — this fix bounds the
  damage instead. Worth a closer look during Phase 14 load testing, which
  will produce sustained pressure for real rather than as an artifact of
  rebuilding images on a laptop.

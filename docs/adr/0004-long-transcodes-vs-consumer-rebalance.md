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
- `max.poll.records=1` — never hold a batch hostage to one slow item.
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

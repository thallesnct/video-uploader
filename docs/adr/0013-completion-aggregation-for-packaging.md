# ADR-0013: Completion aggregation — the fan-in join before packaging

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The HLS master manifest lists every rendition, so it can only be written once
**all** of them are done. The renditions finish on different workers, in
non-deterministic order, at unpredictable times, and the expected set is dynamic
(ADR-0012: it depends on the source resolution).

This is a fan-in join, and it is the hardest coordination problem in the system.
The naive version — "when I finish, check if the others are done, and if so
package" — is a race: two workers finishing near-simultaneously both observe
"all done" and both package. Duplicate manifests, duplicate `video.completed`
events, duplicate webhooks.

## Decision

**Claim-based aggregation in Postgres, driven by `rendition.completed`.**

The `worker_package` consumer, on each `rendition.completed`:

1. Read the video's `expected_renditions` (written by the probe stage) and the
   current `renditions` rows.
2. If not all expected renditions are `completed`, ack and stop.
3. If they are, attempt an atomic claim:

```sql
UPDATE videos
   SET packaging_claimed_at = now()
 WHERE id = :video_id
   AND packaging_claimed_at IS NULL
RETURNING id;
```

4. No row returned → another worker owns packaging; ack and stop.
5. Row returned → write `master.m3u8`, set `status='completed'`, emit
   `video.completed`. If packaging fails, clear the claim so a retry can re-run
   it (with an attempt counter that eventually routes to the DLQ, per ADR-0005).

`packaging_claimed_at` is a **claim column**, not pipeline state: it is
worker-owned under ADR-0007's column-ownership rule, and the projector never
writes or reads it. `videos.status` remains the projector's, set from the
`video.completed` event this stage emits — the packager does not write it directly.

Because all events for a video share a key and therefore a partition (ADR-0002),
the ordering the join relies on is preserved, and the claim handles the remaining
race across concurrent transcode finishers.

**Timeout path:** a periodic reconciler flags videos whose renditions have been
incomplete beyond a threshold and emits `pipeline.failed` with the missing set —
otherwise one permanently-failed rendition leaves a video stuck in `processing`
forever with nothing to alert on. Partial packaging (manifest with the renditions
that succeeded) is a deliberate future option, not the default.

## Alternatives considered

- **Kafka Streams / ksqlDB windowed aggregation.** The canonical answer to
  stream fan-in, and genuinely the right tool at scale. Rejected: JVM stack in a
  Python repo, and windowing fits poorly with an unbounded, variable job duration.
- **Count messages in memory in the packager.** Rejected: state lost on restart
  or rebalance, and wrong the moment there is more than one packager instance.
- **Redis counter with `DECR` to zero.** Workable and fast, but adds a dependency
  and a second source of truth for state Postgres already holds transactionally.
- **Have the last transcode worker package.** Rejected: "last" is exactly what
  cannot be determined without the join this ADR is about.
- **Poll the DB on a timer instead of reacting to events.** Rejected as primary:
  adds latency and load. Retained *only* as the timeout reconciler above.
- **A compacted `rendition.state` topic read at startup.** Rejected: reimplements
  the read model that ADR-0007 already provides.

## Consequences

- Packaging correctness depends on `expected_renditions` being accurate. If the
  probe stage's plan and the emitted `rendition.requested` messages ever diverge,
  the video hangs — one test asserts they are produced from the same computation.
- The claim column makes re-packaging a deliberate operation (clear the claim),
  which is the right default for an idempotent-but-expensive step.
- The same pattern generalizes to any future fan-in (e.g. waiting on subtitles),
  so it lives in `libs/pipeline` as a reusable helper, not inline in the packager.

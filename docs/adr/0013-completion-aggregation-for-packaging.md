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

## Follow-on: step 1's "read expected_renditions and current renditions rows" was itself racy (2026-08-27)

Building `worker_package`, before any code was written: step 1 as originally
worded says read `videos.expected_renditions` and `renditions.status` — both
**projector-owned** columns, written from the projector's own consumption of
`video.status`. `worker_package` consumes `rendition.completed` directly, a
different topic, in a different consumer group, with no ordering guarantee
between the two relative to each other (ADR-0002's ordering guarantee is
*per-partition*, and these are different topics entirely). On the last
rendition, `worker_package` can process `rendition.completed` before the
projector has applied the matching `video.status`, see N−1 of N complete, ack,
and stop — and nothing ever re-triggers it, since that was the last event.
The video hangs in `transcoding` forever; the timeout reconciler above only
emits `pipeline.failed`, it does not finish packaging.

The idiomatic-looking fix — raise `TransientError` when the set looks
incomplete and let redelivery retry it — does not work here: the retry pump
(Phase 11, ADR-0002/ADR-0009) does not exist yet, so a message routed to
`rendition.completed.retry.10s` sits there permanently. The fix has to not
depend on redelivery at all.

**Fix: `worker_package` reads and writes only columns it owns, never the
projector's.** It subscribes to *two* topics — `video.probed` (for the
expected set) and `rendition.completed` (for each finished rendition) — and
**persists first, checks second, in both handlers**:

- `rendition.completed` handler: `UPDATE renditions SET
  packager_playlist_key = :key WHERE video_id = :id AND rendition = :rendition`
  (a plain `UPDATE`, not an upsert — the row is guaranteed to already exist,
  created by `worker_transcode`'s claim before `rendition.completed` is ever
  published). Then check whether every *known* expected rendition now has a
  `packager_playlist_key`.
- `video.probed` handler: `UPDATE videos SET packager_expected_renditions =
  :list WHERE id = :id`. Then run the identical check, against rows already
  persisted by any `rendition.completed` messages that arrived first.

Whichever message lands last is the one that observes "all done" and
attempts the packaging claim — the join no longer cares which topic's message
arrives first, because both handlers write their own fact before reading
anyone's. This is the general fix for a fan-in reading state two different
producers write on two different topics: never read the *other* producer's
column, however tempting, even a projector-owned one that "should" already be
there by the time you look.

`renditions.packager_playlist_key` doubles as both the "is this one seen"
flag and the *data* the master playlist is built from (the relative HLS path
for that rendition) — a separate boolean/timestamp column would still leave
`worker_package` needing to read `renditions.playlist_key` (projector-owned)
to get the actual key, reintroducing the exact race being fixed. One
worker-owned column, both jobs.

`libs/pipeline/repository.py` gains `PackagerRepository`, per this ADR's own
"lives in libs/pipeline as a reusable helper" line — `record_expected`,
`record_rendition`, `ready_playlists`, `claim`, `release_claim`. The next
fan-in (subtitles, say) reuses the shape: two claim columns per join input,
persist-then-check, no reads across ownership boundaries.

### Consequences

- Three new columns, all CLAIM (worker_package-owned), migration 0006:
  `renditions.packager_playlist_key`, `videos.packager_expected_renditions`,
  `videos.packaging_claimed_at`. They deliberately duplicate data the
  projector's columns already hold (`expected_renditions`, rendition
  `status`) — an unusual choice, justified only because there is no retry
  pump yet to fall back on. Revisit once Phase 11 lands: the packager could
  then read the projector's columns and rely on redelivery instead, dropping
  two of the three columns.
- `video.probed` gains `packager` as a second consumer group (alongside
  `thumbnail`), declared in `topics.json`.

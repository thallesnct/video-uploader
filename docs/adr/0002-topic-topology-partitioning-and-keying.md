# ADR-0002: Topic topology, partitioning and keying

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

We need to decide how many topics exist, what a message means, how messages are
keyed, and how parallelism is achieved. Kafka's unit of parallelism is the
partition: a partition is consumed by exactly one member of a group, so topic and
key design *is* the concurrency design.

The base workload is: one uploaded video fans out into N independent transcodes,
which must later be joined for packaging.

## Decision

**Topic per stage transition**, plus one client-facing status topic:

| Topic | Key | Partitions (dev/prod) | Consumer groups |
|---|---|---|---|
| `video.uploaded` | `video_id` | 3 / 6 | probe |
| `video.probed` | `video_id` | 3 / 6 | thumbnail |
| `rendition.requested` | `video_id` | 12 / 24 | transcode |
| `rendition.completed` | `video_id` | 6 / 12 | packager |
| `video.completed` | `video_id` | 3 / 6 | notify |
| `video.status` | `video_id` | 6 / 12 | projector (shared), api (unique per replica) |
| `<topic>.retry`, `<topic>.dlq` | `video_id` | 3 / 3 | retry pump, human |

Rules:

1. **Key every message by `video_id`.** All events for one video land on one
   partition, so their relative order is preserved and the read model never sees
   `completed` before `probed`.
2. **Fan out with one message per `(video_id, rendition)`.** The probe stage
   emits N `rendition.requested` messages. Renditions are then independent work
   items that scale horizontally.
3. **Consumer group per stage.** Stages scale independently; lag per group is the
   diagnostic that tells you which stage is behind (ADR-0010).
4. **`rendition.requested` gets the most partitions** — it is the hot path and
   partition count caps transcode parallelism. Partitions can be added later, but
   adding them changes key→partition mapping, so over-provision now.
5. **Replication factor** 1 in dev, ≥3 with `min.insync.replicas=2` in prod.
6. **`video.status` is a contract seam.** Every stage emits a normalized,
   user-facing progress event there. Internal topics can be reshaped without
   changing what the browser consumes.

## Alternatives considered

- **One `video.events` topic for everything, filtered by type.** Rejected: every
  consumer reads every message, per-stage lag becomes unmeasurable, and one
  stage's backlog delays all others.
- **Key by `(video_id, rendition)`.** Rejected: better spread, but per-video
  ordering is lost, which the read model and the status stream depend on.
- **A topic per rendition (`transcode.720p`, …).** Rejected: the ladder becomes a
  deployment concern; adding a rendition means creating topics and services.
- **One message listing all renditions, worker loops internally.** Rejected: no
  parallelism across workers, and a failure at rendition 4 of 5 has no clean
  retry granularity.

## Consequences

- Per-video ordering is guaranteed; cross-video ordering is not (and is not needed).
- Hot keys are bounded by design: one video produces a small number of messages.
- The join for packaging is a real distributed problem — see ADR-0013.
- Topic creation is explicit and versioned (`make topics`); auto-creation is
  disabled so a typo fails loudly instead of silently creating a dead topic.

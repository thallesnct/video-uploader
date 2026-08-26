# ADR-0001: Claim-check — keep video bytes out of Kafka

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The pipeline moves video files that range from a few MB to several GB. Kafka is
the coordination backbone. The naive design puts the file in the message.

Kafka's broker default `max.message.bytes` is ~1 MiB. It can be raised, but every
increase costs: replication latency grows with record size, `fetch.max.bytes` and
consumer memory must grow to match, a single large record can stall a partition
for every other consumer on it, and page-cache efficiency collapses. Kafka is a
log optimized for many small records, not a file transfer protocol.

## Decision

Use the **claim-check pattern**. The bytes go to the object store; the message
carries a reference:

```json
{
  "event_id": "…", "video_id": "…", "occurred_at": "…", "schema_version": 1,
  "object_key": "videos/{video_id}/source.mp4",
  "rendition": "720p", "size_bytes": 184_320_000
}
```

Every stage reads its input from the object store by key, writes its output back
by key, and publishes the new key. Broker settings stay at their defaults.

## Alternatives considered

- **Large messages with a raised `max.message.bytes`.** Rejected: turns every
  broker and consumer into a memory-sizing exercise, and still caps out below
  real video sizes.
- **Chunking the file across many messages.** Rejected: reimplements a file
  transfer protocol — ordering, reassembly, partial-failure cleanup — on top of a
  log that has no reason to carry the bytes at all.
- **Kafka tiered storage.** Rejected: solves retention cost, not record size.

## Consequences

- Kafka stays fast and its sizing stays boring.
- The object store becomes a hard dependency of every worker (ADR-0006).
- Messages are meaningless without their blob: an object-store lifecycle rule
  that deletes a source before its retry window closes will produce
  unreproducible failures. Retention on both sides must be chosen together.
- Cleanup is our job — a failed or abandoned video leaves orphaned objects.

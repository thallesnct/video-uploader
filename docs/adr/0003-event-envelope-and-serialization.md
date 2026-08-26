# ADR-0003: Event envelope, serialization and schema evolution

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

Seven services produce and consume each other's messages. Without a shared,
enforced shape, a field rename in one worker breaks another at runtime, in
production, on a message that will then be redelivered forever.

The industry-standard answer is Avro or Protobuf with a Schema Registry, which
gives compile-time-ish guarantees and compatibility enforcement at publish time.
That is real infrastructure: a registry service, a build step, generated classes.

## Decision

**JSON on the wire, Pydantic v2 models as the single definition**, in
`libs/pipeline/events.py`, imported by every service. Every message carries:

```
event_id        uuid7   — dedupe / trace identity
video_id        uuid    — also the partition key
occurred_at     RFC3339 UTC
schema_version  int     — bumped only on a breaking change
producer        str     — which service emitted it
payload         typed   — per-event model
```

Kafka headers carry `traceparent` (ADR-0010), `schema_version`, and, on retry/DLQ
messages, `retry_count` / `failure_reason`.

Evolution rules, enforced in review:

- **Additive changes are free.** New fields are optional with defaults.
- **Consumers ignore unknown fields** (`model_config = ConfigDict(extra="ignore")`)
  so a new producer can deploy before its consumers.
- **Removing or retyping a field is breaking**: requires a `schema_version` bump,
  an ADR, and a dual-read window in consumers.
- A contract test asserts that every event model round-trips and that a payload
  containing an unknown field still parses.

Deferred, not rejected: **Avro + Schema Registry**, with a documented trigger —
adopt it when a second team, a non-Python consumer, or a compliance requirement
for enforced compatibility appears.

## Alternatives considered

- **Avro + Confluent Schema Registry.** The production standard, and genuinely
  better at preventing breaking changes. Deferred: one more stateful service, a
  codegen step in every image, and a harder local test story, for a single-repo
  single-language system where Pydantic already gives typed parsing.
- **Protobuf.** Same trade-off, plus a compile step; the wire efficiency is
  irrelevant at our message rate (claim-check keeps messages tiny).
- **Raw dicts / `json.loads` at each call site.** Rejected: this is how the field
  rename outage happens.
- **Pickle.** Rejected: unsafe deserialization, Python-only, unversioned.

## Consequences

- Schema breakage is caught by a contract test and code review rather than by the
  broker. Discipline substitutes for enforcement — acceptable in one repo, and
  the trigger to upgrade is written down.
- JSON costs bytes and parse time; irrelevant here because messages are references.
- `libs/pipeline` becomes a hard coupling point across services. That is
  intentional: one place to change, one place to review.

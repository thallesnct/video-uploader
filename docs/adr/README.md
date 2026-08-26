# Architecture Decision Records

One decision per file, numbered, immutable once accepted. To change a decision,
write a **new** ADR that supersedes the old one and update the status line here —
never rewrite history in place.

Format: Context → Decision → Alternatives considered (with the reason each was
rejected) → Consequences. The rejected alternatives are the valuable part; they
are what stops the same debate from being reopened every quarter.

| # | Title | Status | Why it exists |
|---|---|---|---|
| [0001](0001-claim-check-keep-video-bytes-out-of-kafka.md) | Claim-check: keep video bytes out of Kafka | Accepted | Kafka is a log for small records, not a file transfer protocol |
| [0002](0002-topic-topology-partitioning-and-keying.md) | Topic topology, partitioning and keying | Accepted | Partition and key design *is* the concurrency design |
| [0003](0003-event-envelope-and-serialization.md) | Event envelope, serialization and schema evolution | Accepted | Seven services share one contract; JSON+Pydantic now, Avro trigger written down |
| [0004](0004-long-transcodes-vs-consumer-rebalance.md) | Long transcodes vs. consumer rebalance | Accepted | The eviction/redelivery loop that silently stalls naive pipelines |
| [0005](0005-idempotency-retries-and-dead-letters.md) | Idempotency, retries and dead letters | Accepted | At-least-once means every message arrives twice eventually |
| [0006](0006-object-store-minio-s3-and-presigned-upload.md) | Object store and presigned direct upload | Accepted | Multi-GB uploads must not stream through the API |
| [0007](0007-postgres-as-kafka-fed-read-model.md) | Postgres as a Kafka-fed read model | Accepted | Avoids dual writes; gives SSE its snapshot |
| [0008](0008-sse-for-live-progress.md) | SSE for live progress | Accepted | Multi-replica broadcast + late-subscriber snapshot, the two things SSE gets wrong |
| [0009](0009-python-runtime-and-kafka-client-libraries.md) | Kafka client libraries | Accepted | Blocking workers and an async gateway have different needs |
| [0010](0010-observability-metrics-and-traces.md) | Metrics, consumer lag and traces | Accepted | Prometheus is metrics; per-video questions need traces |
| [0011](0011-testing-strategy.md) | Testing strategy | Accepted | The bugs live in interactions, and ffmpeg isn't on the dev machine |
| [0012](0012-pipeline-scope-which-use-cases.md) | Pipeline scope: which use cases | Accepted | "New topology or just an ffmpeg flag?" as the selection criterion |
| [0013](0013-completion-aggregation-for-packaging.md) | Completion aggregation for packaging | Accepted | The fan-in join, and the race two finishers create |
| [0014](0014-python-and-frontend-library-stack.md) | Library stack | Accepted | Dependency choices are the hardest to reverse |
| [0015](0015-production-readiness.md) | Production readiness | Accepted | ffmpeg parses hostile input; probes and shutdown can't be bolted on |

## Writing a new one

```
cp 0001-*.md 00NN-short-kebab-title.md   # then rewrite it
```

Number sequentially, add a row above, and link it from the phase in
[../../PROGRESS.md](../../PROGRESS.md) that implements it.

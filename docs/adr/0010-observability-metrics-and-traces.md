# ADR-0010: Observability — Prometheus metrics, consumer lag, and OTel traces through Kafka headers

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The stated goal is "see what's going on with the different parts of this system",
with Prometheus and Grafana. Worth naming precisely: **Prometheus and Grafana are
metrics**, not tracing. They answer *how many, how fast, how often failing*, aggregated.

In a fan-out pipeline the more common question is *what happened to **this**
video* — it was uploaded four minutes ago and the 1080p rendition still is not
ready; was it queued, or slow, or retried twice? Metrics cannot answer that;
distributed tracing can. We want both, and they are different tools.

## Decision

**Three signals, one correlation id.**

**1. Metrics — Prometheus scraping every service, Grafana rendering.**

| Metric | Type | Labels |
|---|---|---|
| `stage_duration_seconds` | histogram | `stage`, `rendition`, `outcome` |
| `stage_messages_total` | counter | `stage`, `outcome` (`ok`/`retry`/`dlq`) |
| `stage_in_flight` | gauge | `stage` |
| `transcode_realtime_ratio` | histogram | `rendition` — wall time ÷ video duration, the capacity-planning number |
| `upload_bytes_total`, `object_write_seconds` | counter/histogram | |
| `sse_connections_active` | gauge | |
| `dlq_messages_total` | counter | `topic`, `reason` |

**2. Consumer lag via `kafka-exporter`.** Per-group, per-partition lag is the
single most useful number in a Kafka system: it names the bottleneck stage
directly. Own Grafana row, own alert. Not derivable from application metrics —
it needs the broker's view.

**3. Traces — OpenTelemetry, `traceparent` propagated in Kafka message headers.**
The producer injects W3C context into headers; the consumer extracts it and
starts its span as a child. Result: **one uploaded video is one trace** spanning
API → probe → transcode(×N, as siblings) → package. The queue wait between
stages is visible as the gap between span end and child span start — often the
real latency, and invisible to per-service metrics.

Exporters → OTel Collector → **Tempo** (Grafana-native, cheap to run in compose;
Jaeger is an equally valid swap). Auto-instrumentation for FastAPI, SQLAlchemy
and boto3; manual spans around ffmpeg.

**4. Logs.** Structured JSON via `structlog`, every line carrying `video_id`,
`trace_id`, `stage`. Trace id in logs is what turns a Grafana panel into a
click-through to the failing run. Loki optional in compose; stdout is enough for
dev.

**5. Dashboards as code.** Provisioned JSON in `ops/grafana/dashboards/`, mounted
at startup. Four dashboards: Pipeline Overview (throughput, end-to-end latency),
Stage Detail (duration histograms, failure rate), Kafka Health (lag, partition
skew, DLQ depth), Infrastructure (CPU/memory of workers, MinIO, Postgres).

**6. Alerts** in `ops/prometheus/rules/`: consumer lag above threshold for 5 min,
any DLQ non-empty, stage p99 regression, `stage_in_flight_seconds` approaching
`max.poll.interval.ms` (the ADR-0004 early warning), SSE connection count at cap.

## Alternatives considered

- **Metrics only, as literally asked.** Rejected: it cannot answer per-video
  questions, which is the actual intent behind "see what's going on".
- **Tracing only.** Rejected: no aggregate view, no alerting substrate, sampling
  loses the rare failure.
- **Push metrics via Pushgateway.** Rejected for long-lived services; scraping is
  the Prometheus-native model. Pushgateway stays available for short-lived jobs
  (topic bootstrap, replay CLI) only.
- **Correlating by `video_id` in logs instead of real traces.** Rejected: gives
  you the events but not the timing structure or the queue-wait gaps.
- **A vendor APM (Datadog/New Relic).** Rejected for a self-hosted project;
  OTel keeps the instrumentation vendor-neutral if that changes.

## Consequences

- Every produce/consume path must pass headers through — enforced by the
  `libs/pipeline` wrapper so no service can forget.
- Traces add cost at volume: tail-based sampling in the Collector (keep all
  errors and slow traces, sample the rest).
- Histogram cardinality must be watched — `rendition` is bounded, `video_id`
  must **never** be a metric label (it belongs in traces and logs).
- The observability stack lives in a separate compose file so day-to-day dev does
  not pay for it.

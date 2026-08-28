"""Metrics and tracing (ADR-0010).

Prometheus answers "how slow is transcoding". Only a trace answers "what
happened to *this* video", which is the question a fan-out pipeline actually
raises — so the W3C traceparent travels in Kafka headers and one video's journey
across API, probe, transcode and packaging becomes a single trace.

Cardinality rule: video_id must NEVER be a metric label. It belongs in traces and
logs. A per-video label would multiply every series by the number of videos ever
processed and eventually take Prometheus down.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from time import perf_counter
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from pipeline.settings import observability_settings

KafkaHeaders = list[tuple[str, bytes]]

# Transcoding spans seconds to hours, so the buckets are wide and log-ish.
_DURATION_BUCKETS = (0.1, 0.5, 1, 5, 15, 30, 60, 300, 900, 1800, 3600, 7200)

STAGE_DURATION = Histogram(
    "stage_duration_seconds",
    "Wall time to handle one message",
    ["stage", "rendition", "outcome"],
    buckets=_DURATION_BUCKETS,
)
STAGE_MESSAGES = Counter(
    "stage_messages_total",
    "Messages handled",
    ["stage", "outcome"],
)
STAGE_IN_FLIGHT = Gauge(
    "stage_in_flight",
    "Messages currently being handled",
    ["stage"],
)
# A plain Gauge can't express "how long has this been running" without
# something updating it on a timer — this is instead computed fresh at
# scrape time, no background thread, no drift between updates and scrapes.
# Keyed by stage, one entry per stage: safe because ADR-0004's pause-the-
# whole-consumer design means a single StageWorker process only ever has
# one message in flight at a time, so there's no "oldest of several" to
# track, just "is one running, and since when".
_stage_in_flight_started: dict[str, float] = {}


class _StageInFlightSecondsCollector(Collector):
    def collect(self) -> Iterator[GaugeMetricFamily]:
        family = GaugeMetricFamily(
            "stage_in_flight_seconds",
            "Age of the oldest in-flight message. Alert when this approaches "
            "max.poll.interval.ms — the early warning for the ADR-0004 eviction loop.",
            labels=["stage"],
        )
        now = perf_counter()
        for stage, started in _stage_in_flight_started.items():
            family.add_metric([stage], now - started)
        yield family


REGISTRY.register(_StageInFlightSecondsCollector())

TRANSCODE_REALTIME_RATIO = Histogram(
    "transcode_realtime_ratio",
    "Wall seconds per second of video — the number capacity planning actually needs",
    ["rendition"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32),
)
DLQ_MESSAGES = Counter(
    "dlq_messages_total",
    "Messages routed to a dead-letter topic",
    ["topic", "reason"],
)
RETRY_MESSAGES = Counter(
    "retry_messages_total",
    "Messages routed to a retry tier",
    ["topic", "tier"],
)
SSE_CONNECTIONS = Gauge("sse_connections_active", "Open SSE streams")


@contextmanager
def observe_stage(stage: str, rendition: str = "none") -> Iterator[None]:
    """Time one message's handling and record its outcome."""
    STAGE_IN_FLIGHT.labels(stage=stage).inc()
    started = perf_counter()
    _stage_in_flight_started[stage] = started
    outcome = "ok"
    try:
        yield
    except Exception:
        outcome = "error"
        raise
    finally:
        STAGE_IN_FLIGHT.labels(stage=stage).dec()
        _stage_in_flight_started.pop(stage, None)
        STAGE_DURATION.labels(stage=stage, rendition=rendition, outcome=outcome).observe(
            perf_counter() - started
        )
        STAGE_MESSAGES.labels(stage=stage, outcome=outcome).inc()


# ------------------------------------------------------------------------ tracing


def setup_tracing(service_name: str | None = None) -> None:
    """Wire OTLP export. Safe to call once per process; a no-op if unavailable."""
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    settings = observability_settings()
    resource = Resource.create(
        {
            "service.name": service_name or settings.service_name,
            "deployment.environment": settings.environment,
        }
    )
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(settings.trace_sample_ratio)),
    )
    # An empty endpoint means "trace, but export nowhere". The provider is still
    # installed, so spans have real contexts and traceparent still propagates
    # through Kafka headers — context propagation must not depend on a collector
    # being reachable (ADR-0010).
    if settings.otlp_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True))
        )
    trace.set_tracer_provider(provider)

    # Auto-instrumentation for the two libraries every service in this repo
    # already uses (ADR-0010): DB queries and S3/MinIO calls show up as
    # child spans with no per-call-site code. Both are pinned dependencies
    # already (pyproject.toml) — this is the one place that actually turns
    # them on, once per process, so no service has to remember to. Guarded
    # rather than assumed idempotent: instrumenting twice raises.
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    sqlalchemy_instrumentor = SQLAlchemyInstrumentor()
    if not sqlalchemy_instrumentor.is_instrumented_by_opentelemetry:
        sqlalchemy_instrumentor.instrument()
    botocore_instrumentor = BotocoreInstrumentor()
    if not botocore_instrumentor.is_instrumented_by_opentelemetry:
        botocore_instrumentor.instrument()


def tracer(name: str = "pipeline") -> Any:
    from opentelemetry import trace

    return trace.get_tracer(name)


def inject_trace_headers(headers: KafkaHeaders | None = None) -> KafkaHeaders:
    """Add the current W3C trace context to Kafka headers.

    This one line is what turns per-service spans into a single trace for a
    video. Every produce path goes through it, so no stage can forget.
    """
    from opentelemetry.propagate import inject

    carrier: dict[str, str] = {}
    inject(carrier)
    merged = list(headers or [])
    merged.extend((key, value.encode()) for key, value in carrier.items())
    return merged


def extract_trace_context(headers: Sequence[tuple[str, bytes]] | None) -> Any:
    """Rebuild the parent context from Kafka headers, for the consuming span."""
    from opentelemetry.propagate import extract

    carrier = {
        key: value.decode() if isinstance(value, bytes) else str(value)
        for key, value in (headers or [])
    }
    return extract(carrier)

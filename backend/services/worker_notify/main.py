"""Webhook notifier — outbound side-effect, own consumer group (Phase 11).

The second consumer group on video.completed/pipeline.failed alongside
projector's (topics.json: consumed_by ["projector", "notify"] on
pipeline.failed) — same events, independent offsets and lag, the shape
PLAN.md calls out as the reason this use case earns a slot.

Idempotency is deliberately not a new DB table: the outbound payload
carries event_id, and the contract is "receivers dedupe by event_id" — the
same philosophy ADR-0005 already states for rejecting a Redis dedup table
("the business key... is stronger and free"). This service has no database
at all; a webhook is stateless from this side.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.events import Event, PipelineFailed, VideoCompleted
from pipeline.health import HealthRegistry, serve_health
from pipeline.obs import setup_tracing
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy, TerminalError, TransientError
from pipeline.settings import notify_settings, observability_settings
from pipeline.topics import PIPELINE_FAILED, REGISTRY, VIDEO_COMPLETED

SERVICE = "worker-notify"
GROUP = "notify"

log = logging.getLogger(__name__)


def _webhook_payload(event: Event) -> dict[str, Any]:
    """JSON-safe serialization (UUID/datetime), same idiom services/api/sse.py
    already uses for the same reason."""
    return json.loads(event.model_dump_json())


def _post_webhook(url: str, payload: dict[str, Any], timeout_s: float) -> None:
    # url is NotifySettings.webhook_url, operator-configured at deploy time
    # (env var), never attacker- or event-controlled — not the untrusted
    # input S310 guards against.
    request = urllib.request.Request(  # noqa: S310
        url,
        method="POST",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=timeout_s)  # noqa: S310
    except urllib.error.HTTPError as exc:
        # A rejected payload (4xx) will be rejected identically forever;
        # retrying can't help. A receiver's own outage (5xx) probably will
        # resolve — that's exactly what the retry ladder is for.
        if exc.code >= 500:
            raise TransientError(f"webhook responded {exc.code}") from exc
        raise TerminalError(f"webhook rejected the payload: {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise TransientError(f"webhook unreachable: {exc}") from exc


def build_handler(webhook_url: str, timeout_s: float) -> Handler:
    def handle(event: Event, view: MessageView) -> None:
        if not isinstance(event, (VideoCompleted, PipelineFailed)):
            raise TerminalError(f"worker-notify received unexpected event {event.type}")
        _post_webhook(webhook_url, _webhook_payload(event), timeout_s)

    return handle


def main() -> None:
    from confluent_kafka import Consumer

    logging.basicConfig(level=observability_settings().log_level)
    setup_tracing(SERVICE)

    settings = notify_settings()
    producer = EventProducer(service=SERVICE)
    consumer = Consumer(consumer_config(GROUP))

    worker = StageWorker(
        stage=SERVICE,
        source_topic=VIDEO_COMPLETED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(settings.webhook_url, settings.webhook_timeout_s),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    # No DB/S3 to check — Kafka group assignment is this service's one real
    # dependency (ADR-0015: readiness checks dependencies, not an external,
    # un-owned webhook target this process doesn't control).
    health.register("kafka_group", lambda: worker.seconds_unassigned() is None)
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe(topics=[VIDEO_COMPLETED, PIPELINE_FAILED])
    try:
        worker.run()
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()

"""Publishing side (ADR-0003, ADR-0005, ADR-0010).

Every produce goes through here so three things can never be forgotten: the
message is keyed by video_id (per-video ordering), the trace context travels in
the headers (one trace per video), and the producer is idempotent.
"""

from __future__ import annotations

from typing import Any

from pipeline.events import Event
from pipeline.obs import KafkaHeaders, inject_trace_headers
from pipeline.settings import kafka_settings


def producer_config(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """librdkafka settings for a producer that neither duplicates nor loses.

    acks=all with idempotence means a leader failover cannot silently drop a
    write, and producer-side retries cannot introduce duplicates (ADR-0005).
    """
    config: dict[str, Any] = {
        "bootstrap.servers": kafka_settings().bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "retries": 1_000_000,
        "compression.type": "lz4",
        "linger.ms": 5,
    }
    config.update(extra or {})
    return config


class EventProducer:
    def __init__(self, client: Any | None = None, service: str = "unknown") -> None:
        self._client = client
        self._service = service

    @property
    def client(self) -> Any:
        if self._client is None:
            from confluent_kafka import Producer

            self._client = Producer(producer_config())
        return self._client

    def publish(self, topic: str, event: Event, headers: KafkaHeaders | None = None) -> None:
        """Queue an event. Delivery is asynchronous; call flush() before exiting."""
        all_headers = inject_trace_headers(headers)
        all_headers.append(("event_type", event.type.encode()))
        all_headers.append(("schema_version", str(event.schema_version).encode()))
        self.client.produce(
            topic=topic,
            key=event.key,
            value=event.serialize(),
            headers=all_headers,
        )
        # Serve delivery callbacks without blocking; failures surface on flush().
        self.client.poll(0)

    def publish_raw(
        self, topic: str, key: bytes, value: bytes, headers: KafkaHeaders | None = None
    ) -> None:
        """Republish an untouched payload — used by retry and DLQ routing."""
        self.client.produce(topic=topic, key=key, value=value, headers=headers or [])
        self.client.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until queued messages are delivered. Returns messages still queued."""
        return int(self.client.flush(timeout))

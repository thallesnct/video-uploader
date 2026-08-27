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


def build_headers(event: Event, extra: KafkaHeaders | None = None) -> KafkaHeaders:
    """Headers every published event carries, whichever client publishes it.

    Shared by both producers on purpose: a second copy is how the sync path and
    the async path end up disagreeing about what a message looks like.
    """
    headers = inject_trace_headers(extra)
    headers.append(("event_type", event.type.encode()))
    headers.append(("schema_version", str(event.schema_version).encode()))
    return headers


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
        all_headers = build_headers(event, headers)
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


class AsyncEventProducer:
    """The asyncio counterpart, for the API (ADR-0009).

    The API holds hundreds of long-lived SSE connections, so a blocking producer
    would stall every open stream on each publish. Same envelope, same headers,
    same trace injection as the sync producer — only the client differs.

    start() and stop() are bound to the app lifespan: a producer that was never
    started fails on first publish, and one never stopped drops buffered
    messages on shutdown.
    """

    def __init__(self, client: Any | None = None, service: str = "unknown") -> None:
        self._client = client
        self._service = service
        self._started = client is not None

    async def start(self) -> None:
        if self._client is None:
            from aiokafka import AIOKafkaProducer

            self._client = AIOKafkaProducer(
                bootstrap_servers=kafka_settings().bootstrap_servers,
                enable_idempotence=True,
                acks="all",
                compression_type="lz4",
                linger_ms=5,
            )
        if not self._started:
            await self._client.start()
            self._started = True

    async def stop(self) -> None:
        if self._client is not None and self._started:
            await self._client.stop()
            self._started = False

    async def publish(self, topic: str, event: Event, headers: KafkaHeaders | None = None) -> None:
        client = self._client  # a local, so the None check actually narrows
        if not self._started or client is None:
            raise RuntimeError(
                "AsyncEventProducer.start() was never awaited — wire it to the application lifespan"
            )
        await client.send_and_wait(
            topic,
            key=event.key,
            value=event.serialize(),
            headers=build_headers(event, headers),
        )

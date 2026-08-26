"""The transcode stage against real Kafka, Postgres and MinIO (Phase 5 gate).

ffmpeg itself is injected: ffmpeg lives only in the worker image (ADR-0011).
This file covers the Kafka round trip, idempotency, the DB claim, DLQ routing —
and the reason this is "the highest-risk phase": whether a handler that runs
longer than `max.poll.interval.ms` survives without being evicted from its
consumer group (ADR-0004).
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import pytest
from pipeline.consumer import StageWorker, consumer_config
from pipeline.db import create_sync_engine, sync_sessions
from pipeline.events import RenditionRequested
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy, TerminalError
from pipeline.storage import object_store
from pipeline.topics import (
    PIPELINE_FAILED,
    REGISTRY,
    RENDITION_COMPLETED,
    RENDITION_REQUESTED,
    VIDEO_STATUS,
)
from pipeline.transcode import TranscodeResult

from services.worker_transcode.main import build_handler

OWNER = "user|transcode"


def messages_for(bootstrap: str, topic: str, video_id: str, seconds: float = 12.0) -> list[dict]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"assert-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    found: list[dict] = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        payload = json.loads(message.value())
        if payload.get("video_id") == video_id:
            found.append(payload)
    consumer.close()
    return found


def a_request(video_id: uuid.UUID, target_key: str, duration_s: float = 5.0) -> RenditionRequested:
    return RenditionRequested(
        video_id=video_id,
        owner_id=OWNER,
        producer="test",
        rendition="360p",
        source_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        target_key=target_key,
        duration_s=duration_s,
    )


@pytest.fixture()
def sessions_factory(environment: None) -> Any:
    engine = create_sync_engine()
    factory = sync_sessions(engine)
    yield factory
    engine.dispose()


def insert_video_row(sessions_factory: Any, video_id: uuid.UUID) -> None:
    """The FK on renditions.video_id expects a videos row to already exist —
    which in production is always true, since the probe/transcode stages only
    ever run after the API creates it at upload time (Phase 3). Tests that skip
    the API and publish straight to Kafka must recreate that precondition."""
    from pipeline.db import sync_session_scope
    from pipeline.models import VideoRow

    with sync_session_scope(sessions_factory) as session:
        session.add(
            VideoRow(
                id=video_id,
                owner_id=OWNER,
                filename="clip.mp4",
                content_type="video/mp4",
                declared_size_bytes=256,
                object_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
                status="uploaded",
            )
        )


# ------------------------------------------------------------------ happy path


def test_transcode_publishes_completion_and_writes_the_object(
    environment: None, kafka_bootstrap: str, sessions_factory: Any
) -> None:
    store = object_store()
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    target_key = f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4"
    invocations: list[str] = []

    def fake_transcode(
        source: str, destination: str, rendition: str, *, timeout_s: float
    ) -> TranscodeResult:
        invocations.append(rendition)
        with open(destination, "wb") as handle:
            handle.write(b"\x00" * 1024)
        return TranscodeResult(output_path=destination)

    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    from confluent_kafka import Consumer

    producer = EventProducer(service="test")
    consumer = Consumer(
        consumer_config(f"transcode-{uuid.uuid4()}", {"auto.offset.reset": "latest"})
    )
    worker = StageWorker(
        stage="worker-transcode",
        source_topic=RENDITION_REQUESTED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, sessions_factory, transcode_fn=fake_transcode),
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe()
    worker.wait_for_assignment()

    producer.publish(RENDITION_REQUESTED, a_request(video_id, target_key))
    producer.flush()

    assert worker.run(max_messages=1) == 1
    producer.flush()
    consumer.close()

    assert invocations == ["360p"]
    assert store.exists(target_key)

    completed = messages_for(kafka_bootstrap, RENDITION_COMPLETED, str(video_id))
    assert len(completed) == 1
    assert completed[0]["object_key"] == target_key
    assert completed[0]["size_bytes"] == 1024

    statuses = messages_for(kafka_bootstrap, VIDEO_STATUS, str(video_id))
    assert any(s["state"] == "transcoding" and s["rendition"] == "360p" for s in statuses)


# -------------------------------------------------------------- the whole point


def test_a_transcode_longer_than_the_poll_interval_survives_without_eviction(
    environment: None, kafka_bootstrap: str, sessions_factory: Any
) -> None:
    """ADR-0004: a handler that outlives max.poll.interval.ms must not get its
    consumer evicted, or the message is redelivered and reprocessed forever.

    Discriminating assertions, not just "no duplicate output" — an idempotent
    skip on a second delivery would mask a real eviction (a broken
    implementation would still end up with exactly one file on disk, just via
    two handler invocations instead of one):

    1. The handler is invoked exactly once — a rebalance-and-redeliver bug
       shows up here as 2, not as extra files.
    2. After the run completes, polling the SAME consumer for more messages on
       this topic returns nothing: the offset was committed and nothing is
       waiting to be redelivered.
    """
    store = object_store()
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    target_key = f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4"
    invocation_count = 0

    def slow_transcode(
        source: str, destination: str, rendition: str, *, timeout_s: float
    ) -> TranscodeResult:
        nonlocal invocation_count
        invocation_count += 1
        # Longer than the reduced max.poll.interval.ms below. A correct
        # implementation keeps polling (heartbeating) throughout this sleep;
        # a broken one (no pause/resume, or no polling during work) gets
        # evicted well before this returns.
        time.sleep(8.0)
        with open(destination, "wb") as handle:
            handle.write(b"\x00" * 512)
        return TranscodeResult(output_path=destination)

    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    from confluent_kafka import Consumer

    producer = EventProducer(service="test")
    # Reduced from the production default (600_000ms) so the test is fast.
    # Verified empirically: librdkafka requires max.poll.interval.ms >=
    # session.timeout.ms, so both are lowered together.
    consumer = Consumer(
        consumer_config(
            f"transcode-rebalance-{uuid.uuid4()}",
            {
                "auto.offset.reset": "latest",
                "max.poll.interval.ms": "6000",
                "session.timeout.ms": "6000",
            },
        )
    )
    worker = StageWorker(
        stage="worker-transcode",
        source_topic=RENDITION_REQUESTED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, sessions_factory, transcode_fn=slow_transcode),
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.2,  # frequent heartbeat, well under the 6s ceiling
    )
    worker.subscribe()
    worker.wait_for_assignment()

    producer.publish(RENDITION_REQUESTED, a_request(video_id, target_key, duration_s=1.0))
    producer.flush()

    assert worker.run(max_messages=1) == 1

    # The discriminating check: nothing left to redeliver. A broken
    # implementation would have been evicted mid-handler, failed to commit,
    # and this poll would return the SAME message again.
    leftover = consumer.poll(5.0)
    assert leftover is None, (
        "a message is still waiting to be redelivered — the consumer was "
        "evicted during the long transcode and its offset was never committed"
    )

    producer.flush()
    consumer.close()

    assert invocation_count == 1, (
        f"handler invoked {invocation_count} times for one message — "
        "eviction caused redelivery and reprocessing"
    )
    assert store.exists(target_key)

    completed = messages_for(kafka_bootstrap, RENDITION_COMPLETED, str(video_id), seconds=5.0)
    assert len(completed) == 1


# --------------------------------------------------------------------- redelivery


def test_a_redelivered_message_produces_one_object_and_one_db_row(
    environment: None, sessions_factory: Any
) -> None:
    """Gate item 2: at-least-once means every message arrives twice eventually
    (ADR-0005). The second delivery must skip re-encoding and must not create
    a second claim row."""
    from pipeline.models import RenditionRow
    from sqlalchemy import select

    store = object_store()
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    target_key = f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4"
    invocations: list[str] = []

    def fake_transcode(
        source: str, destination: str, rendition: str, *, timeout_s: float
    ) -> TranscodeResult:
        invocations.append(rendition)
        with open(destination, "wb") as handle:
            handle.write(b"\x01" * 777)
        return TranscodeResult(output_path=destination)

    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory, transcode_fn=fake_transcode)
    event = a_request(video_id, target_key)

    class _View:
        headers: list[tuple[str, bytes]] = []

    handler(event, _View())  # first delivery — does the real work
    handler(event, _View())  # redelivery — must be a no-op encode

    producer.flush()

    assert invocations == ["360p"], "the second delivery re-ran the transcode"

    with sessions_factory() as session:
        rows = (
            session.execute(
                select(RenditionRow).where(
                    RenditionRow.video_id == video_id, RenditionRow.rendition == "360p"
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1, f"expected exactly one DB row, found {len(rows)}"
    assert rows[0].attempt == 1, "the idempotent-skip path must not touch the claim"


# ----------------------------------------------------------------------- failure


def test_a_terminal_failure_lands_in_the_dlq_with_reason_and_pipeline_failed(
    environment: None, kafka_bootstrap: str, sessions_factory: Any
) -> None:
    """Gate item 3: a corrupt input is terminal, not retryable (ADR-0005), and
    the rest of the system (projector, notify) must hear about it."""
    store = object_store()
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    target_key = f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4"

    def broken_transcode(
        source: str, destination: str, rendition: str, *, timeout_s: float
    ) -> TranscodeResult:
        raise TerminalError("simulated corrupt input — unsupported codec")

    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    from confluent_kafka import Consumer

    producer = EventProducer(service="test")
    consumer = Consumer(
        consumer_config(f"transcode-{uuid.uuid4()}", {"auto.offset.reset": "latest"})
    )
    worker = StageWorker(
        stage="worker-transcode",
        source_topic=RENDITION_REQUESTED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, sessions_factory, transcode_fn=broken_transcode),
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe()
    worker.wait_for_assignment()

    producer.publish(RENDITION_REQUESTED, a_request(video_id, target_key))
    producer.flush()

    assert worker.run(max_messages=1) == 1
    producer.flush()
    consumer.close()

    dlq = messages_for(kafka_bootstrap, f"{RENDITION_REQUESTED}.dlq", str(video_id))
    assert len(dlq) == 1

    from confluent_kafka import Consumer as HeaderConsumer

    header_consumer = HeaderConsumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"headers-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    header_consumer.subscribe([f"{RENDITION_REQUESTED}.dlq"])
    deadline = time.monotonic() + 10
    reason = None
    while time.monotonic() < deadline and reason is None:
        message = header_consumer.poll(0.5)
        if message is None or message.error():
            continue
        if json.loads(message.value()).get("video_id") == str(video_id):
            reason = dict(message.headers() or {}).get("failure_reason", b"").decode()
    header_consumer.close()
    assert reason is not None and "corrupt input" in reason

    failed = messages_for(kafka_bootstrap, PIPELINE_FAILED, str(video_id))
    assert len(failed) == 1
    assert failed[0]["terminal"] is True
    assert failed[0]["stage"] == "worker-transcode"
    assert failed[0]["rendition"] == "360p"

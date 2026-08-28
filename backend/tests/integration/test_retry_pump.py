"""The retry pump's own redelivery test (PROGRESS.md's Phase 11 gate,
stated explicitly): produce a TRANSIENT failure, assert the message reaches
the source topic again after — not before — its tier's delay.

Real Kafka, real time.sleep, not mocked (unit/test_worker_retry_pump.py
already covers the timing *calculation* with mocked sleep — this proves the
end-to-end behavior actually holds). worker.run(max_messages=1) blocks
until the pump's handler thread finishes, sleep included, and only
republishes after that sleep returns — so measuring elapsed time across
that one call already proves "not before": the produce call physically
cannot happen until the measured time has passed.
"""

from __future__ import annotations

import json
import time
import uuid

import pytest
from pipeline.consumer import StageWorker, consumer_config
from pipeline.events import VideoUploaded
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy
from pipeline.topics import REGISTRY

from services.worker_retry_pump.main import build_handler

OWNER = "user|retry-pump"


def messages_for(bootstrap: str, topic: str, video_id: str, seconds: float = 8.0) -> list[dict]:
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


@pytest.mark.slow
def test_a_transient_failure_is_redelivered_after_its_tier_delay_not_before(
    environment: None, kafka_bootstrap: str
) -> None:
    video_id = uuid.uuid4()
    producer = EventProducer(service="test")

    # Simulates what _route_failure already does on a real transient
    # failure (covered by its own tests) — this test's job starts from a
    # message already sitting on the retry-tier topic.
    event = VideoUploaded(
        video_id=video_id,
        owner_id=OWNER,
        producer="probe",
        object_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        filename="clip.mp4",
        size_bytes=256,
        content_type="video/mp4",
    )
    producer.publish("video.uploaded.retry.10s", event, headers=[("retry_count", b"1")])
    producer.flush()

    from confluent_kafka import Consumer

    consumer = Consumer(
        consumer_config(f"retry-pump-{uuid.uuid4()}", {"auto.offset.reset": "earliest"})
    )
    worker = StageWorker(
        stage="worker-retry-pump",
        source_topic="video.uploaded.retry.10s",
        consumer=consumer,
        producer=producer,
        handler=build_handler(producer),
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe(topics=["video.uploaded.retry.10s"])
    worker.wait_for_assignment()

    started = time.monotonic()
    assert worker.run(max_messages=1) == 1
    elapsed = time.monotonic() - started
    consumer.close()

    # Not before: the pump's own handler blocks on time.sleep for the
    # remaining delay before it ever calls publish_raw, so run() itself
    # cannot return this fast unless the wait actually happened. Some slack
    # below the nominal 10s for the gap between occurred_at and the
    # publish() call above.
    assert elapsed >= 8, f"redelivered after only {elapsed:.1f}s, tier delay is 10s"

    redelivered = messages_for(kafka_bootstrap, "video.uploaded", str(video_id))
    assert len(redelivered) == 1
    assert redelivered[0]["video_id"] == str(video_id)

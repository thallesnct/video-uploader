"""The thumbnail stage against real Kafka, Postgres and MinIO (Phase 9 gate).

ffmpeg itself is injected: ffmpeg lives only in the worker image (ADR-0011).
No DB claim table is involved here (unlike worker_transcode) — idempotency is
the object-existence check alone, since there is one poster/sprite/vtt set per
video rather than one row per rendition to contend over.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import pytest
from pipeline.consumer import StageWorker, consumer_config
from pipeline.events import VideoProbed
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy, TerminalError
from pipeline.storage import object_store
from pipeline.topics import PIPELINE_FAILED, REGISTRY, VIDEO_PROBED, VIDEO_STATUS

from services.worker_thumbnail.main import build_handler

OWNER = "user|thumbnail"


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


def a_probe(video_id: uuid.UUID, duration_s: float = 12.0) -> VideoProbed:
    return VideoProbed(
        video_id=video_id,
        owner_id=OWNER,
        producer="test",
        duration_s=duration_s,
        width=640,
        height=360,
        video_codec="h264",
        expected_renditions=["360p"],
        source_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
    )


def _fake_poster(source: str, destination: str, *, duration_s: float) -> None:
    with open(destination, "wb") as handle:
        handle.write(b"\xff" * 128)


def _fake_sprite(source: str, destination: str, layout: Any) -> None:
    with open(destination, "wb") as handle:
        handle.write(b"\xfe" * 256)


class _View:
    headers: list[tuple[str, bytes]] = []


def test_thumbnail_publishes_keys_and_writes_all_three_objects(
    environment: None, kafka_bootstrap: str
) -> None:
    store = object_store()
    video_id = uuid.uuid4()
    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    from confluent_kafka import Consumer

    producer = EventProducer(service="test")
    consumer = Consumer(
        consumer_config(f"thumbnail-{uuid.uuid4()}", {"auto.offset.reset": "latest"})
    )
    worker = StageWorker(
        stage="worker-thumbnail",
        source_topic=VIDEO_PROBED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, poster_fn=_fake_poster, sprite_fn=_fake_sprite),
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe()
    worker.wait_for_assignment()

    producer.publish(VIDEO_PROBED, a_probe(video_id))
    producer.flush()

    assert worker.run(max_messages=1) == 1
    producer.flush()
    consumer.close()

    prefix = f"users/{OWNER}/videos/{video_id}"
    assert store.exists(f"{prefix}/thumbs/poster.jpg")
    assert store.exists(f"{prefix}/thumbs/sprite.jpg")
    assert store.exists(f"{prefix}/thumbs/sprite.vtt")

    statuses = messages_for(kafka_bootstrap, VIDEO_STATUS, str(video_id))
    assert len(statuses) == 1
    assert statuses[0]["state"] == "transcoding"
    assert statuses[0]["poster_key"] == f"{prefix}/thumbs/poster.jpg"
    assert statuses[0]["sprite_key"] == f"{prefix}/thumbs/sprite.jpg"
    assert statuses[0]["vtt_key"] == f"{prefix}/thumbs/sprite.vtt"


def test_a_redelivered_message_does_not_regenerate(environment: None) -> None:
    """At-least-once means every message arrives twice eventually (ADR-0005).
    The second delivery must skip regenerating and re-announce instead."""
    store = object_store()
    video_id = uuid.uuid4()
    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    invocations: list[str] = []

    def counting_poster(source: str, destination: str, *, duration_s: float) -> None:
        invocations.append("poster")
        _fake_poster(source, destination, duration_s=duration_s)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, poster_fn=counting_poster, sprite_fn=_fake_sprite)
    event = a_probe(video_id)

    handler(event, _View())  # first delivery — does the real work
    handler(event, _View())  # redelivery — must be a no-op regenerate

    producer.flush()

    assert invocations == ["poster"], "the second delivery regenerated the poster"


def test_a_probe_event_with_no_source_key_is_terminal(environment: None) -> None:
    """Only reachable from a video.probed published by a pre-Phase-9 producer
    (VideoProbed.source_key is optional per ADR-0003) — a poison message for
    this consumer, not something a retry could ever resolve (ADR-0005)."""
    producer = EventProducer(service="test")
    handler = build_handler(
        object_store(), producer, poster_fn=_fake_poster, sprite_fn=_fake_sprite
    )
    event = a_probe(uuid.uuid4()).model_copy(update={"source_key": None})

    with pytest.raises(TerminalError, match="source_key"):
        handler(event, _View())


def test_a_terminal_failure_lands_in_the_dlq_with_reason(
    environment: None, kafka_bootstrap: str
) -> None:
    store = object_store()
    video_id = uuid.uuid4()
    store.client.put_object(
        Bucket=store.bucket,
        Key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        Body=b"\x00" * 256,
    )

    def broken_poster(source: str, destination: str, *, duration_s: float) -> None:
        raise TerminalError("simulated corrupt input — no video stream")

    from confluent_kafka import Consumer

    producer = EventProducer(service="test")
    consumer = Consumer(
        consumer_config(f"thumbnail-{uuid.uuid4()}", {"auto.offset.reset": "latest"})
    )
    worker = StageWorker(
        stage="worker-thumbnail",
        source_topic=VIDEO_PROBED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, poster_fn=broken_poster, sprite_fn=_fake_sprite),
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe()
    worker.wait_for_assignment()

    producer.publish(VIDEO_PROBED, a_probe(video_id))
    producer.flush()

    assert worker.run(max_messages=1) == 1
    producer.flush()
    consumer.close()

    dlq = messages_for(kafka_bootstrap, f"{VIDEO_PROBED}.dlq", str(video_id))
    assert len(dlq) == 1

    failed = messages_for(kafka_bootstrap, PIPELINE_FAILED, str(video_id))
    assert len(failed) == 1
    assert failed[0]["terminal"] is True
    assert failed[0]["stage"] == "worker-thumbnail"

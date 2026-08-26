"""The probe stage against real Kafka, Postgres and MinIO (Phase 4 gate).

ffprobe itself is injected here: ffmpeg lives only in the worker image
(ADR-0011), so this file covers the Kafka round trip, the fan-out and the
plan/fan-out consistency. The real ffprobe path is covered by the tests marked
`ffmpeg`, which run inside that image.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from pipeline.consumer import StageWorker, consumer_config
from pipeline.events import VideoUploaded
from pipeline.media import MediaInfo
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy
from pipeline.storage import object_store
from pipeline.topics import (
    REGISTRY,
    RENDITION_REQUESTED,
    VIDEO_PROBED,
    VIDEO_STATUS,
    VIDEO_UPLOADED,
)

from services.worker_probe.main import build_handler

OWNER = "user|probe"


def messages_for(bootstrap: str, topic: str, video_id: str, seconds: float = 10.0) -> list[dict]:
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
    import time

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


@pytest.fixture()
def run_probe(environment: None, kafka_bootstrap: str) -> Any:
    """Publish one video.uploaded and let the probe worker handle exactly it."""

    def _run(info: MediaInfo, *, object_body: bytes = b"\x00" * 256) -> str:
        video_id = uuid.uuid4()
        store = object_store()
        object_key = f"users/{OWNER}/videos/{video_id}/source.mp4"
        store.client.put_object(Bucket=store.bucket, Key=object_key, Body=object_body)

        producer = EventProducer(service="test")
        from confluent_kafka import Consumer

        consumer = Consumer(
            consumer_config(f"probe-{uuid.uuid4()}", {"auto.offset.reset": "latest"})
        )
        worker = StageWorker(
            stage="worker-probe",
            source_topic=VIDEO_UPLOADED,
            consumer=consumer,
            producer=producer,
            handler=build_handler(store, producer, prober=lambda _path: info),
            policy=RetryPolicy(REGISTRY.retry_tiers),
            poll_timeout=0.5,
        )
        worker.subscribe()
        # Publishing before assignment would drop the message into a topic
        # nobody is listening to yet.
        worker.wait_for_assignment()

        producer.publish(
            VIDEO_UPLOADED,
            VideoUploaded(
                video_id=video_id,
                owner_id=OWNER,
                producer="test",
                object_key=object_key,
                filename="clip.mp4",
                size_bytes=len(object_body),
                content_type="video/mp4",
            ),
        )
        producer.flush()

        assert worker.run(max_messages=1) == 1
        producer.flush()
        consumer.close()
        return str(video_id)

    return _run


def test_ladder_is_planned_from_the_source_not_from_configuration(
    run_probe: Any, kafka_bootstrap: str
) -> None:
    """A 640x360 source must never be asked to produce 1080p (ADR-0012)."""
    video_id = run_probe(
        MediaInfo(duration_s=2.0, width=640, height=360, video_codec="h264", audio_codec="aac")
    )

    probed = messages_for(kafka_bootstrap, VIDEO_PROBED, video_id)
    requested = messages_for(kafka_bootstrap, RENDITION_REQUESTED, video_id)

    assert len(probed) == 1
    assert probed[0]["expected_renditions"] == ["360p"]
    assert {message["rendition"] for message in requested} == {"360p"}
    assert not any(m["rendition"] == "1080p" for m in requested)


def test_the_plan_and_the_fan_out_cannot_disagree(run_probe: Any, kafka_bootstrap: str) -> None:
    """The invariant packaging depends on (ADR-0013).

    If expected_renditions and the emitted requests ever diverge, the master
    manifest waits forever for a rendition nobody was asked to produce, and the
    video hangs in `transcoding` with nothing to alert on. Exactly equal — not a
    subset in either direction.
    """
    video_id = run_probe(
        MediaInfo(duration_s=30.0, width=1920, height=1080, video_codec="h264", audio_codec="aac")
    )

    probed = messages_for(kafka_bootstrap, VIDEO_PROBED, video_id)
    requested = messages_for(kafka_bootstrap, RENDITION_REQUESTED, video_id)

    planned = set(probed[0]["expected_renditions"])
    emitted = {message["rendition"] for message in requested}

    assert planned == emitted
    assert len(requested) == len(planned), "a rendition was requested twice"
    assert planned == {"360p", "480p", "720p", "1080p"}


def test_each_rendition_gets_its_own_message_keyed_by_video(
    run_probe: Any, kafka_bootstrap: str
) -> None:
    """One message per (video, rendition) is what lets renditions scale
    independently across partitions (ADR-0002)."""
    video_id = run_probe(
        MediaInfo(duration_s=10.0, width=1280, height=720, video_codec="h264", audio_codec=None)
    )

    requested = messages_for(kafka_bootstrap, RENDITION_REQUESTED, video_id)

    assert len(requested) == 3
    for message in requested:
        assert message["owner_id"] == OWNER
        assert message["target_key"].startswith(f"users/{OWNER}/videos/{video_id}/")
        assert message["rendition"] in message["target_key"]


def test_portrait_video_is_not_transcoded_into_upscales(
    run_probe: Any, kafka_bootstrap: str
) -> None:
    video_id = run_probe(
        MediaInfo(duration_s=5.0, width=1080, height=1920, video_codec="h264", audio_codec="aac")
    )

    probed = messages_for(kafka_bootstrap, VIDEO_PROBED, video_id)

    assert probed[0]["expected_renditions"] == ["360p", "480p", "720p", "1080p"]
    # Reported as the viewer sees it.
    assert (probed[0]["width"], probed[0]["height"]) == (1080, 1920)


def test_status_carries_the_plan_so_the_ui_can_render_placeholders(
    run_probe: Any, kafka_bootstrap: str
) -> None:
    """The browser shows the whole ladder as pending before any transcode
    finishes (ADR-0008)."""
    video_id = run_probe(
        MediaInfo(duration_s=4.0, width=854, height=480, video_codec="h264", audio_codec="aac")
    )

    statuses = messages_for(kafka_bootstrap, VIDEO_STATUS, video_id)

    probed_status = [s for s in statuses if s["state"] == "probed"]
    assert len(probed_status) == 1
    assert "360p" in probed_status[0]["detail"]
    assert "480p" in probed_status[0]["detail"]


def test_a_source_with_no_audio_still_probes(run_probe: Any, kafka_bootstrap: str) -> None:
    video_id = run_probe(
        MediaInfo(duration_s=3.0, width=640, height=360, video_codec="h264", audio_codec=None)
    )

    probed = messages_for(kafka_bootstrap, VIDEO_PROBED, video_id)

    assert probed[0]["audio_codec"] is None

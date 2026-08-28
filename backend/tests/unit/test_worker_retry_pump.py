"""The retry pump is the component ADR-0002/0005/0009 all name and none of
them scheduled building — without it, a transient failure has been
equivalent to a silent, permanent drop since Phase 5 (PROGRESS.md)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pipeline.consumer import MessageView
from pipeline.events import RenditionRequested

from services.worker_retry_pump.main import build_handler, retry_tier_topics
from tests.unit.fakes import FakeMessage, FakeProducer


def _event(occurred_at: datetime) -> RenditionRequested:
    return RenditionRequested(
        video_id=uuid4(),
        owner_id="user|test",
        producer="probe",
        rendition="720p",
        source_key="videos/x/source.mp4",
        target_key="videos/x/renditions/720p.mp4",
        duration_s=42.0,
        occurred_at=occurred_at,
    )


def test_retry_tier_topics_covers_every_retryable_topic_and_tier() -> None:
    topics = retry_tier_topics()

    assert "video.uploaded.retry.10s" in topics
    assert "video.uploaded.retry.1m" in topics
    assert "video.uploaded.retry.10m" in topics
    # video.status/pipeline.failed have retries: false — never a pump target.
    assert not any(t.startswith("video.status.retry.") for t in topics)
    assert not any(t.startswith("pipeline.failed.retry.") for t in topics)


def test_an_already_due_message_is_republished_without_sleeping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))

    event = _event(occurred_at=datetime.now(UTC) - timedelta(minutes=5))
    view = MessageView(
        FakeMessage(
            value=b"unused",
            topic="video.uploaded.retry.10s",
            key=b"video-key",
            headers=[("retry_count", b"1")],
        )
    )
    producer = FakeProducer()

    build_handler(producer)(event, view)

    assert slept == []
    assert producer.published == [
        ("video.uploaded", b"video-key", b"unused", [("retry_count", b"1")])
    ]
    assert producer.flushes == 1


def test_a_not_yet_due_message_sleeps_for_roughly_the_remaining_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))

    # Occurred 2s ago on the 10s tier: ~8s left to wait.
    event = _event(occurred_at=datetime.now(UTC) - timedelta(seconds=2))
    view = MessageView(FakeMessage(value=b"x", topic="video.uploaded.retry.10s"))
    producer = FakeProducer()

    build_handler(producer)(event, view)

    assert len(slept) == 1
    assert 7 <= slept[0] <= 9
    assert producer.published[0][0] == "video.uploaded"

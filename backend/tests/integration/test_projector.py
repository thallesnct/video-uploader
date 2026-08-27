"""The projector against a real Postgres and Kafka (Phase 6 gate).

ADR-0007: every write is an upsert, so replaying a Kafka partition twice must
produce identical rows. Replay safety is tested by calling the built handler
directly with the same batch of events twice — the same level test_transcode.py
uses for its own redelivery test, since DB behavior under replay does not
depend on the transport.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from pipeline.db import create_sync_engine, sync_session_scope, sync_sessions
from pipeline.events import PipelineFailed, VideoState, VideoStatusChanged
from pipeline.models import EventRow, RenditionRow, VideoRow
from pipeline.retry import RetryPolicy, TerminalError
from pipeline.topics import PIPELINE_FAILED, REGISTRY, VIDEO_STATUS
from sqlalchemy import select

from services.projector.main import build_handler

OWNER = "user|projector"


@pytest.fixture()
def sessions_factory(environment: None) -> Any:
    engine = create_sync_engine()
    factory = sync_sessions(engine)
    yield factory
    engine.dispose()


def insert_video_row(sessions_factory: Any, video_id: uuid.UUID) -> None:
    """The FK on renditions.video_id (and events.video_id) expects a videos
    row to already exist — true in production from upload time (Phase 3)."""
    with sync_session_scope(sessions_factory) as session:
        session.add(
            VideoRow(
                id=video_id,
                owner_id=OWNER,
                filename="clip.mp4",
                content_type="video/mp4",
                declared_size_bytes=256,
                object_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
                status=VideoState.UPLOADED.value,
            )
        )


class _View:
    headers: list[tuple[str, bytes]] = []


def test_projector_writes_probe_data_onto_the_video_row(sessions_factory: Any) -> None:
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    handler = build_handler(sessions_factory)

    handler(
        VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="probe",
            state=VideoState.PROBED,
            duration_s=12.5,
            width=1280,
            height=720,
            expected_renditions=["360p", "720p"],
        ),
        _View(),
    )

    with sync_session_scope(sessions_factory) as session:
        row = session.execute(select(VideoRow).where(VideoRow.id == video_id)).scalar_one()
        assert row.status == VideoState.PROBED.value
        assert float(row.duration_s) == 12.5
        assert row.width == 1280
        assert row.height == 720
        assert row.expected_renditions == ["360p", "720p"]


def test_projector_upserts_a_completed_rendition(sessions_factory: Any) -> None:
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    handler = build_handler(sessions_factory)
    target_key = f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4"

    handler(
        VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="transcode",
            state=VideoState.TRANSCODING,
            rendition="360p",
            rendition_object_key=target_key,
            rendition_size_bytes=4096,
        ),
        _View(),
    )

    with sync_session_scope(sessions_factory) as session:
        video = session.execute(select(VideoRow).where(VideoRow.id == video_id)).scalar_one()
        assert video.status == VideoState.TRANSCODING.value

        rendition = session.execute(
            select(RenditionRow).where(
                RenditionRow.video_id == video_id, RenditionRow.rendition == "360p"
            )
        ).scalar_one()
        assert rendition.status == VideoState.COMPLETED.value
        assert rendition.object_key == target_key
        assert rendition.completed_at is not None
        # CLAIM columns untouched — the projector never writes them (ADR-0007).
        assert rendition.attempt == 0
        assert rendition.claimed_at is None


def test_projector_applies_a_terminal_pipeline_failure(sessions_factory: Any) -> None:
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    handler = build_handler(sessions_factory)

    handler(
        PipelineFailed(
            video_id=video_id,
            owner_id=OWNER,
            producer="transcode",
            stage="worker-transcode",
            reason="TerminalError: unsupported codec",
            terminal=True,
            rendition="720p",
        ),
        _View(),
    )

    with sync_session_scope(sessions_factory) as session:
        video = session.execute(select(VideoRow).where(VideoRow.id == video_id)).scalar_one()
        assert video.status == VideoState.FAILED.value
        assert "unsupported codec" in video.failure_reason

        rendition = session.execute(
            select(RenditionRow).where(
                RenditionRow.video_id == video_id, RenditionRow.rendition == "720p"
            )
        ).scalar_one()
        assert rendition.status == VideoState.FAILED.value
        assert "unsupported codec" in rendition.failure_reason


def test_replaying_the_same_batch_twice_is_a_no_op(sessions_factory: Any) -> None:
    """The Phase 6 gate: replay the same event batch twice, assert identical
    final rows, no duplicate renditions, and no duplicate events rows."""
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)
    handler = build_handler(sessions_factory)

    batch = [
        VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="probe",
            state=VideoState.PROBED,
            duration_s=8.0,
            width=640,
            height=360,
            expected_renditions=["360p"],
        ),
        VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="transcode",
            state=VideoState.TRANSCODING,
            rendition="360p",
            rendition_object_key=f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4",
            rendition_size_bytes=2048,
        ),
    ]

    for event in batch:
        handler(event, _View())
    for event in batch:  # replay: identical events, identical event_id each
        handler(event, _View())

    with sync_session_scope(sessions_factory) as session:
        videos = session.execute(select(VideoRow).where(VideoRow.id == video_id)).scalars().all()
        assert len(videos) == 1
        assert videos[0].status == VideoState.TRANSCODING.value

        renditions = (
            session.execute(select(RenditionRow).where(RenditionRow.video_id == video_id))
            .scalars()
            .all()
        )
        assert len(renditions) == 1, "replay must not duplicate a rendition row"
        assert renditions[0].status == VideoState.COMPLETED.value

        events = (
            session.execute(select(EventRow).where(EventRow.video_id == video_id)).scalars().all()
        )
        assert len(events) == len(batch), "replay must not duplicate events rows"


def test_a_missing_video_row_is_terminal_not_an_infinite_crash_loop(
    sessions_factory: Any,
) -> None:
    """apply()'s videos UPDATE on a nonexistent id is a silent no-op, but the
    FK on events.video_id then raises IntegrityError. This must classify as
    TERMINAL (dead-letter) — left as an unclassified TRANSIENT default, it
    would crash-restart the worker on the same unprocessable message forever,
    since video.status has no retry ladder to fall back to (ADR-0005
    follow-on)."""
    handler = build_handler(sessions_factory)
    video_id = uuid.uuid4()  # deliberately never inserted

    with pytest.raises(TerminalError):
        handler(
            VideoStatusChanged(
                video_id=video_id,
                owner_id=OWNER,
                producer="probe",
                state=VideoState.PROBED,
            ),
            _View(),
        )


def test_a_transient_failure_on_video_status_crashes_the_worker(
    environment: None, kafka_bootstrap: str
) -> None:
    """video.status has no retry ladder — a StageWorker must propagate a
    transient failure so the process crashes, rather than silently continuing
    to poll past a message that can never be committed for it (ADR-0005
    follow-on: committing a later message would skip this one forever)."""
    from confluent_kafka import Consumer
    from pipeline.consumer import StageWorker, consumer_config
    from pipeline.producer import EventProducer

    video_id = uuid.uuid4()

    def flaky_handler(event: Any, view: Any) -> None:
        raise RuntimeError("connection reset")

    producer = EventProducer(service="test")
    consumer = Consumer(
        consumer_config(f"projector-{uuid.uuid4()}", {"auto.offset.reset": "latest"})
    )
    worker = StageWorker(
        stage="projector",
        source_topic=VIDEO_STATUS,
        consumer=consumer,
        producer=producer,
        handler=flaky_handler,
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe(topics=[VIDEO_STATUS, PIPELINE_FAILED])
    worker.wait_for_assignment()

    producer.publish(
        VIDEO_STATUS,
        VideoStatusChanged(
            video_id=video_id, owner_id=OWNER, producer="probe", state=VideoState.PROBED
        ),
    )
    producer.flush()

    with pytest.raises(RuntimeError, match="connection reset"):
        worker.run(max_messages=1)

    producer.flush()
    consumer.close()

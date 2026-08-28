"""The packaging stage against real Postgres and MinIO (Phase 9 gate,
ADR-0013 and its follow-on).

The point of this file: the completion join must produce exactly one
packaging run regardless of which of its two input topics finishes last, and
regardless of two workers finishing "at the same time". Neither property
follows from testing either handler branch alone.
"""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from pipeline.db import create_sync_engine, sync_session_scope, sync_sessions
from pipeline.events import RenditionCompleted, VideoProbed
from pipeline.producer import EventProducer
from pipeline.repository import PackagerRepository
from pipeline.retry import TerminalError
from pipeline.storage import hls_master_key, object_store
from pipeline.topics import VIDEO_COMPLETED, VIDEO_STATUS

from services.worker_package.main import build_handler

OWNER = "user|package"


class _View:
    headers: list[tuple[str, bytes]] = []


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


@pytest.fixture()
def sessions_factory(environment: None) -> Any:
    engine = create_sync_engine()
    factory = sync_sessions(engine)
    yield factory
    engine.dispose()


def insert_video_and_renditions(
    sessions_factory: Any, video_id: uuid.UUID, renditions: list[str]
) -> None:
    """Both FKs (renditions.video_id) and the invariant worker_package relies
    on (a renditions row always exists by the time rendition.completed fires,
    per worker_transcode's claim()) must hold for tests that skip the API and
    worker_transcode and publish straight into this stage."""
    from pipeline.models import RenditionRow, VideoRow

    with sync_session_scope(sessions_factory) as session:
        session.add(
            VideoRow(
                id=video_id,
                owner_id=OWNER,
                filename="clip.mp4",
                content_type="video/mp4",
                declared_size_bytes=256,
                object_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
                status="transcoding",
            )
        )

    with sync_session_scope(sessions_factory) as session:
        for rendition in renditions:
            session.add(
                RenditionRow(
                    id=uuid.uuid4(),
                    video_id=video_id,
                    owner_id=OWNER,
                    rendition=rendition,
                    attempt=1,
                )
            )


def a_probe(video_id: uuid.UUID, renditions: list[str]) -> VideoProbed:
    return VideoProbed(
        video_id=video_id,
        owner_id=OWNER,
        producer="test",
        duration_s=12.0,
        width=640,
        height=360,
        video_codec="h264",
        expected_renditions=renditions,
        source_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
    )


def a_rendition_completed(video_id: uuid.UUID, rendition: str) -> RenditionCompleted:
    prefix = f"users/{OWNER}/videos/{video_id}"
    return RenditionCompleted(
        video_id=video_id,
        owner_id=OWNER,
        producer="test",
        rendition=rendition,
        object_key=f"{prefix}/renditions/{rendition}.mp4",
        size_bytes=1024,
        transcode_seconds=1.0,
        playlist_key=f"{prefix}/hls/{rendition}/playlist.m3u8",
    )


# --------------------------------------------------------------- order independence


def test_packaging_completes_when_video_probed_arrives_last(
    environment: None, kafka_bootstrap: str, sessions_factory: Any
) -> None:
    """The case the original ADR-0013 wording already handled: expected set
    known, then renditions complete one by one."""
    store = object_store()
    video_id = uuid.uuid4()
    renditions = ["360p", "720p"]
    insert_video_and_renditions(sessions_factory, video_id, renditions)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)

    handler(a_probe(video_id, renditions), _View())
    for rendition in renditions:
        handler(a_rendition_completed(video_id, rendition), _View())
    producer.flush()

    assert store.exists(hls_master_key(OWNER, video_id))

    completed = messages_for(kafka_bootstrap, VIDEO_COMPLETED, str(video_id))
    assert len(completed) == 1
    assert set(completed[0]["renditions"]) == set(renditions)


def test_packaging_completes_when_video_probed_arrives_first_among_renditions(
    environment: None, kafka_bootstrap: str, sessions_factory: Any
) -> None:
    """The race this follow-on fixes, proven directly: every rendition.completed
    is processed BEFORE video.probed ever arrives here — no packaging must
    happen until the expected set is known, and the message that finally
    supplies it must be the one that triggers packaging."""
    store = object_store()
    video_id = uuid.uuid4()
    renditions = ["360p", "720p"]
    insert_video_and_renditions(sessions_factory, video_id, renditions)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)

    for rendition in renditions:
        handler(a_rendition_completed(video_id, rendition), _View())

    assert not store.exists(hls_master_key(OWNER, video_id)), (
        "must not package before the expected set is known"
    )

    handler(a_probe(video_id, renditions), _View())
    producer.flush()

    assert store.exists(hls_master_key(OWNER, video_id))
    completed = messages_for(kafka_bootstrap, VIDEO_COMPLETED, str(video_id))
    assert len(completed) == 1


def test_packaging_waits_for_every_expected_rendition(
    environment: None, sessions_factory: Any
) -> None:
    store = object_store()
    video_id = uuid.uuid4()
    renditions = ["360p", "720p"]
    insert_video_and_renditions(sessions_factory, video_id, renditions)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)

    handler(a_probe(video_id, renditions), _View())
    handler(a_rendition_completed(video_id, "360p"), _View())

    assert not store.exists(hls_master_key(OWNER, video_id))


# ------------------------------------------------------------- concurrent finishers


def test_concurrent_claims_elect_exactly_one_packager(
    environment: None, sessions_factory: Any
) -> None:
    """ADR-0013's gate item, at the mechanism it depends on: two real threads
    racing PackagerRepository.claim() for the same video, each with its own
    DB session, must not both win."""
    video_id = uuid.uuid4()
    insert_video_and_renditions(sessions_factory, video_id, [])

    def attempt_claim() -> bool:
        with sync_session_scope(sessions_factory) as session:
            return PackagerRepository(session).claim(video_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt_claim(), range(2)))

    assert sorted(results) == [False, True], "exactly one concurrent claim must win"


def test_a_redelivered_final_message_does_not_repackage(
    environment: None, sessions_factory: Any
) -> None:
    """At-least-once means the message that completed the join can arrive
    twice (ADR-0005). The second delivery must skip writing master.m3u8
    again — object existence, checked before any claim attempt."""
    store = object_store()
    video_id = uuid.uuid4()
    renditions = ["360p"]
    insert_video_and_renditions(sessions_factory, video_id, renditions)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)
    handler(a_probe(video_id, renditions), _View())

    final_event = a_rendition_completed(video_id, "360p")
    handler(final_event, _View())  # first delivery — packages
    first_write = store.head(hls_master_key(OWNER, video_id))
    assert first_write is not None

    handler(final_event, _View())  # redelivery — must not re-claim or re-write
    second_write = store.head(hls_master_key(OWNER, video_id))

    assert second_write is not None
    assert first_write.get("ETag") == second_write.get("ETag"), (
        "a redelivery must not rewrite master.m3u8"
    )


# --------------------------------------------------------------------- content


def test_master_playlist_lists_every_rendition_exactly_once(
    environment: None, sessions_factory: Any
) -> None:
    store = object_store()
    video_id = uuid.uuid4()
    renditions = ["360p", "720p", "1080p"]
    insert_video_and_renditions(sessions_factory, video_id, renditions)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)

    handler(a_probe(video_id, renditions), _View())
    for rendition in renditions:
        handler(a_rendition_completed(video_id, rendition), _View())
    producer.flush()

    with tempfile.NamedTemporaryFile() as tmp:
        store.download(hls_master_key(OWNER, video_id), tmp.name)
        with open(tmp.name, encoding="utf-8") as fh:
            text = fh.read()

    for rendition in renditions:
        assert text.count(f"{rendition}/playlist.m3u8") == 1


# ---------------------------------------------------------------------- failure


def test_a_rendition_completed_with_no_playlist_key_is_terminal(
    environment: None, sessions_factory: Any
) -> None:
    """Only reachable from a pre-Phase-9 producer (playlist_key is optional
    per ADR-0003) — a poison message, not a transient one (ADR-0005)."""
    store = object_store()
    video_id = uuid.uuid4()
    insert_video_and_renditions(sessions_factory, video_id, ["360p"])

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)
    event = a_rendition_completed(video_id, "360p").model_copy(update={"playlist_key": None})

    with pytest.raises(TerminalError, match="playlist_key"):
        handler(event, _View())


def test_video_status_completed_carries_the_master_playlist_key(
    environment: None, kafka_bootstrap: str, sessions_factory: Any
) -> None:
    store = object_store()
    video_id = uuid.uuid4()
    renditions = ["360p"]
    insert_video_and_renditions(sessions_factory, video_id, renditions)

    producer = EventProducer(service="test")
    handler = build_handler(store, producer, sessions_factory)
    handler(a_probe(video_id, renditions), _View())
    handler(a_rendition_completed(video_id, "360p"), _View())
    producer.flush()

    statuses = messages_for(kafka_bootstrap, VIDEO_STATUS, str(video_id))
    completed_statuses = [s for s in statuses if s["state"] == "completed"]
    assert len(completed_statuses) == 1
    assert completed_statuses[0]["master_playlist_key"] == hls_master_key(OWNER, video_id)

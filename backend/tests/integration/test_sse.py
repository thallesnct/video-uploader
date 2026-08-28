"""The SSE gateway against real Kafka and Postgres (Phase 7 gate, ADR-0008).

Generator-level tests call sse_stream directly by async iteration: FastAPI's
TestClient was verified (empirically — see the probe that led to this file's
structure) to buffer an entire streaming response before returning any of it,
which makes it useless for asserting on live timing. One HTTP-level test
covers the route wiring itself, using a stream that terminates on its own
(an already-failed video), where TestClient's buffering doesn't matter.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import pytest
from pipeline.broadcast import StatusBroadcaster
from pipeline.db import create_engine, create_sync_engine, session_scope, sessions, sync_sessions
from pipeline.events import PipelineFailed, VideoState, VideoStatusChanged
from pipeline.models import VideoRow
from pipeline.producer import EventProducer
from pipeline.topics import PIPELINE_FAILED, VIDEO_STATUS

from services.api.sse import sse_stream
from services.projector.main import build_handler as projector_handler

OWNER = "user|sse"
TIMEOUT = 10.0


class _View:
    headers: list[tuple[str, bytes]] = []


@pytest.fixture()
async def async_sessions(environment: None) -> Any:
    engine = create_engine()
    yield sessions(engine)
    await engine.dispose()


@pytest.fixture()
def sync_sessions_factory(environment: None) -> Any:
    engine = create_sync_engine()
    factory = sync_sessions(engine)
    yield factory
    engine.dispose()


async def insert_video_row(async_sessions: Any, video_id: uuid.UUID) -> None:
    async with session_scope(async_sessions) as session:
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


async def anext_within(agen: Any, seconds: float = TIMEOUT) -> Any:
    return await asyncio.wait_for(agen.__anext__(), timeout=seconds)


async def test_a_late_connect_sees_finished_renditions_in_the_snapshot_then_live(
    environment: None, kafka_bootstrap: str, async_sessions: Any, sync_sessions_factory: Any
) -> None:
    """Gate (a): a client connecting after two renditions finished still
    receives both, then the third live."""
    video_id = uuid.uuid4()
    await insert_video_row(async_sessions, video_id)
    handler = projector_handler(sync_sessions_factory)

    handler(
        VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="probe",
            state=VideoState.PROBED,
            expected_renditions=["360p", "720p", "1080p"],
        ),
        _View(),
    )
    for rendition in ("360p", "720p"):
        handler(
            VideoStatusChanged(
                video_id=video_id,
                owner_id=OWNER,
                producer="transcode",
                state=VideoState.TRANSCODING,
                rendition=rendition,
                rendition_object_key=f"users/{OWNER}/videos/{video_id}/renditions/{rendition}.mp4",
                rendition_size_bytes=1024,
            ),
            _View(),
        )

    broadcaster = StatusBroadcaster()
    await broadcaster.start()
    try:
        agen = sse_stream(async_sessions, broadcaster, OWNER, video_id, None)
        snapshot = await anext_within(agen)
        assert snapshot["event"] == "snapshot"
        payload = json.loads(snapshot["data"])
        completed = {r["rendition"]: r for r in payload["renditions"] if r["status"] == "completed"}
        assert set(completed) == {"360p", "720p"}, (
            "already-finished renditions must be in the snapshot"
        )

        # Now the third rendition finishes live, for real — through the real
        # projector, publishing to real Kafka, waking the real broadcaster.
        producer = EventProducer(service="test")
        event = VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="transcode",
            state=VideoState.TRANSCODING,
            rendition="1080p",
            rendition_object_key=f"users/{OWNER}/videos/{video_id}/renditions/1080p.mp4",
            rendition_size_bytes=2048,
        )
        handler(event, _View())
        producer.publish(VIDEO_STATUS, event)
        producer.flush()

        live = await anext_within(agen)
        assert live["event"] == "rendition.completed"
        assert json.loads(live["data"])["rendition"] == "1080p"

        await agen.aclose()
    finally:
        await broadcaster.stop()


async def test_two_replicas_both_see_an_event_from_either(
    environment: None, kafka_bootstrap: str
) -> None:
    """Gate (b): with two API replicas, a client on replica A receives an
    event produced via replica B. Unique group ids are what makes this work —
    proven by the negative case below, where a shared group id delivers to
    only one."""
    video_id = uuid.uuid4()
    replica_a = StatusBroadcaster()
    replica_b = StatusBroadcaster()
    await replica_a.start()
    await replica_b.start()
    try:
        wakeup_a = replica_a.subscribe(video_id)
        wakeup_b = replica_b.subscribe(video_id)

        producer = EventProducer(service="test")
        producer.publish(
            VIDEO_STATUS,
            VideoStatusChanged(
                video_id=video_id, owner_id=OWNER, producer="probe", state=VideoState.PROBED
            ),
        )
        producer.flush()

        await asyncio.wait_for(wakeup_a.wait(), timeout=TIMEOUT)
        await asyncio.wait_for(wakeup_b.wait(), timeout=TIMEOUT)
    finally:
        await replica_a.stop()
        await replica_b.stop()


async def test_a_shared_group_id_delivers_to_only_one_replica(
    environment: None, kafka_bootstrap: str
) -> None:
    """The negative case: this is the failure mode ADR-0008 names — a shared
    group id load-balances partitions, so each event reaches only one
    replica. Asserted over a batch, not one message: a single message's
    delivery race is sensitive to sub-second rebalance timing between the
    two joins (empirically flaky even with a settle delay + re-seek); the
    aggregate split — every event delivered, none delivered twice — is the
    actual property this test exists to prove and doesn't depend on it."""
    from aiokafka import AIOKafkaConsumer
    from pipeline.settings import kafka_settings

    group_id = f"sse-test-shared-{uuid.uuid4()}"

    def make_consumer() -> Any:
        return AIOKafkaConsumer(
            VIDEO_STATUS,
            PIPELINE_FAILED,
            bootstrap_servers=kafka_settings().bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )

    replica_a = StatusBroadcaster(client=make_consumer())
    replica_b = StatusBroadcaster(client=make_consumer())
    await replica_a.start()
    await replica_b.start()
    # Both joining the same group triggers a rebalance that splits partitions
    # between them, which can leave either side's fetch position stale from
    # before the split (StatusBroadcaster.start()'s own seek_to_end() ran
    # before replica_b existed). Give the rebalance time to settle, then
    # re-seek both — same reasoning as start()'s own fix, needed again
    # because membership changed again.
    await asyncio.sleep(2.0)
    await replica_a._client.seek_to_end()
    await replica_b._client.seek_to_end()
    await asyncio.sleep(0.2)
    try:
        video_ids = [uuid.uuid4() for _ in range(20)]
        wakeups_a = {vid: replica_a.subscribe(vid) for vid in video_ids}
        wakeups_b = {vid: replica_b.subscribe(vid) for vid in video_ids}

        producer = EventProducer(service="test")
        for vid in video_ids:
            producer.publish(
                VIDEO_STATUS,
                VideoStatusChanged(
                    video_id=vid, owner_id=OWNER, producer="probe", state=VideoState.PROBED
                ),
            )
        producer.flush()
        await asyncio.sleep(3.0)

        woke_a = {vid for vid, event in wakeups_a.items() if event.is_set()}
        woke_b = {vid for vid, event in wakeups_b.items() if event.is_set()}

        assert woke_a, "replica A should have received some share of the events"
        assert woke_b, "replica B should have received some share of the events"
        assert woke_a.isdisjoint(woke_b), (
            "a shared group id must not deliver the same event to both replicas"
        )
        assert woke_a | woke_b == set(video_ids), "every event must be delivered to someone"
    finally:
        await replica_a.stop()
        await replica_b.stop()


async def test_reconnect_with_last_event_id_replays_no_duplicates(
    environment: None, kafka_bootstrap: str, async_sessions: Any, sync_sessions_factory: Any
) -> None:
    """Gate (c): reconnect with Last-Event-ID replays no duplicates."""
    video_id = uuid.uuid4()
    await insert_video_row(async_sessions, video_id)
    handler = projector_handler(sync_sessions_factory)

    handler(
        VideoStatusChanged(
            video_id=video_id,
            owner_id=OWNER,
            producer="probe",
            state=VideoState.PROBED,
            expected_renditions=["360p"],
        ),
        _View(),
    )

    broadcaster = StatusBroadcaster()
    await broadcaster.start()
    try:
        first = sse_stream(async_sessions, broadcaster, OWNER, video_id, None)
        snapshot = await anext_within(first)
        last_event_id = int(snapshot["id"])
        await first.aclose()

        handler(
            VideoStatusChanged(
                video_id=video_id,
                owner_id=OWNER,
                producer="transcode",
                state=VideoState.TRANSCODING,
                rendition="360p",
                rendition_object_key=f"users/{OWNER}/videos/{video_id}/renditions/360p.mp4",
                rendition_size_bytes=512,
            ),
            _View(),
        )

        second = sse_stream(async_sessions, broadcaster, OWNER, video_id, last_event_id)
        resumed = await anext_within(second)
        assert resumed["event"] == "rendition.completed"
        assert int(resumed["id"]) > last_event_id
        seen_ids = {int(resumed["id"])}

        # Nothing else pending: the video isn't failed, so this would hang
        # waiting for the next wake-up — bound it and treat the timeout as
        # "correctly nothing more to replay" rather than a real assertion gap.
        with pytest.raises(TimeoutError):
            await anext_within(second, seconds=1.0)
        await second.aclose()

        assert len(seen_ids) == 1, "no event should be replayed twice"
    finally:
        await broadcaster.stop()


async def test_the_subscriber_registry_is_empty_after_disconnect(
    environment: None, kafka_bootstrap: str, async_sessions: Any, sync_sessions_factory: Any
) -> None:
    """A leaked subscriber is silent — the stream still works, memory grows.
    Prove the finally: unsubscribe(...) actually runs."""
    video_id = uuid.uuid4()
    await insert_video_row(async_sessions, video_id)

    broadcaster = StatusBroadcaster()
    await broadcaster.start()
    try:
        agen = sse_stream(async_sessions, broadcaster, OWNER, video_id, None)
        await anext_within(agen)  # the snapshot
        assert video_id in broadcaster._wakeups

        await agen.aclose()
        assert video_id not in broadcaster._wakeups
    finally:
        await broadcaster.stop()


def test_a_failed_video_streams_the_snapshot_then_closes(
    environment: None, kafka_bootstrap: str, client: Any, auth: Any, sync_sessions_factory: Any
) -> None:
    """HTTP-level: proves the route wiring, auth, and the id:/event:/data:
    wire format — not live timing, which TestClient cannot demonstrate (it
    buffers a whole response before returning any of it)."""
    resp = client.post(
        "/videos",
        json={"filename": "clip.mp4", "content_type": "video/mp4", "size_bytes": 256},
        headers=auth(OWNER),
    )
    video_id = resp.json()["video_id"]

    handler = projector_handler(sync_sessions_factory)
    handler(
        PipelineFailed(
            video_id=uuid.UUID(video_id),
            owner_id=OWNER,
            producer="transcode",
            stage="worker-transcode",
            reason="TerminalError: unsupported codec",
            terminal=True,
        ),
        _View(),
    )

    with client.stream("GET", f"/videos/{video_id}/events", headers=auth(OWNER)) as stream:
        body = "".join(stream.iter_text())

    assert "event: snapshot" in body
    assert '"status": "failed"' in body


def test_a_completed_video_streams_the_snapshot_then_closes(
    environment: None, kafka_bootstrap: str, client: Any, auth: Any, sync_sessions_factory: Any
) -> None:
    """Phase 9 follow-on: completed is terminal too, same as failed — a
    reconnect to an already-finished video must not hang forever."""
    resp = client.post(
        "/videos",
        json={"filename": "clip.mp4", "content_type": "video/mp4", "size_bytes": 256},
        headers=auth(OWNER),
    )
    video_id = resp.json()["video_id"]

    handler = projector_handler(sync_sessions_factory)
    handler(
        VideoStatusChanged(
            video_id=uuid.UUID(video_id),
            owner_id=OWNER,
            producer="package",
            state=VideoState.COMPLETED,
            master_playlist_key=f"users/{OWNER}/videos/{video_id}/hls/master.m3u8",
        ),
        _View(),
    )

    with client.stream("GET", f"/videos/{video_id}/events", headers=auth(OWNER)) as stream:
        body = "".join(stream.iter_text())

    assert "event: snapshot" in body
    assert '"status": "completed"' in body


def test_another_tenants_video_is_not_found(
    environment: None, kafka_bootstrap: str, client: Any, auth: Any
) -> None:
    resp = client.post(
        "/videos",
        json={"filename": "clip.mp4", "content_type": "video/mp4", "size_bytes": 256},
        headers=auth(OWNER),
    )
    video_id = resp.json()["video_id"]

    other = client.get(f"/videos/{video_id}/events", headers=auth("user|someone-else"))
    assert other.status_code == 404


def test_the_sse_route_also_accepts_a_query_param_token(
    environment: None,
    kafka_bootstrap: str,
    client: Any,
    auth: Any,
    mint: Any,
    sync_sessions_factory: Any,
) -> None:
    """EventSource cannot set headers (ADR-0008 follow-on) — this is the only
    route that accepts ?access_token=, and it must actually authenticate,
    not just be accepted syntactically. The video is failed before any of
    these requests: TestClient blocks until the stream terminates on its own
    (verified in Phase 7 — it buffers a whole response before returning any
    of it), so a non-terminal video here would hang the test forever."""
    resp = client.post(
        "/videos",
        json={"filename": "clip.mp4", "content_type": "video/mp4", "size_bytes": 256},
        headers=auth(OWNER),
    )
    video_id = resp.json()["video_id"]

    projector_handler(sync_sessions_factory)(
        PipelineFailed(
            video_id=uuid.UUID(video_id),
            owner_id=OWNER,
            producer="transcode",
            stage="worker-transcode",
            reason="TerminalError: unsupported codec",
            terminal=True,
        ),
        _View(),
    )

    ok = client.get(f"/videos/{video_id}/events?access_token={mint(OWNER)}")
    assert ok.status_code == 200

    wrong_owner = client.get(f"/videos/{video_id}/events?access_token={mint('user|someone-else')}")
    assert wrong_owner.status_code == 404

    no_token = client.get(f"/videos/{video_id}/events")
    assert no_token.status_code == 401

    bad_token = client.get(f"/videos/{video_id}/events?access_token=not-a-real-token")
    assert bad_token.status_code == 401

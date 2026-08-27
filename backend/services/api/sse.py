"""The SSE stream generator (ADR-0008, follow-on 2026-08-27).

Kept separate from main.py's route so it can be tested by direct async
iteration against real Kafka/Postgres, bypassing FastAPI's TestClient — which
was verified (empirically, not assumed) to buffer an entire streaming response
before returning any of it, making it useless for asserting on live timing.

sessions and broadcaster are parameters, not read from app.state, so a test
can drive this function without running the app's lifespan.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from pipeline.broadcast import StatusBroadcaster
from pipeline.db import session_scope
from pipeline.events import VideoState
from pipeline.models import EventRow
from pipeline.repository import SSERepository, VideoRepository
from pipeline.settings import sse_settings
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def sse_event_name(row: EventRow) -> str:
    """The wire `event:` name (ADR-0008's fixed list, as far as current stages
    populate it). Not used for stream termination — that re-checks the video
    row's status column fresh every poll, not this payload shape."""
    if row.type == "pipeline.failed":
        return "failed"
    if row.type == "video.status":
        payload = row.payload
        if payload.get("rendition") is not None and payload.get("rendition_object_key") is not None:
            return "rendition.completed"
        if payload.get("state") == VideoState.PROBED.value:
            return "probed"
        return "status"
    return row.type


def _to_snapshot(video_row: Any, rendition_rows: list[Any]) -> dict[str, Any]:
    from services.api.main import to_response  # local: avoid a circular import
    from services.api.schemas import RenditionSnapshot, VideoSnapshot

    snapshot = VideoSnapshot(
        video=to_response(video_row),
        renditions=[
            RenditionSnapshot(
                rendition=r.rendition,
                status=r.status,
                object_key=r.object_key,
                failure_reason=r.failure_reason,
                completed_at=r.completed_at,
            )
            for r in rendition_rows
        ],
    )
    return json.loads(snapshot.model_dump_json())


async def sse_stream(
    sessions: async_sessionmaker[AsyncSession],
    broadcaster: StatusBroadcaster,
    owner_id: str,
    video_id: UUID,
    last_event_id: int | None,
) -> AsyncIterator[dict[str, str]]:
    """Snapshot-then-deltas (fresh connect) or replay-then-deltas (reconnect).

    subscribe() happens before any DB read, on purpose: a status change
    published after this point is guaranteed to set the wake-up flag even
    while the snapshot/replay query is still in flight, so nothing between
    "start listening" and "finish reading" can be lost. What can duplicate
    (a wake-up for something the read already saw) is harmless — the loop
    below queries by watermark, not by wake-up count.
    """
    limits = sse_settings()
    wakeup = broadcaster.subscribe(video_id)
    try:
        if last_event_id is None:
            async with session_scope(sessions) as session:
                video_row = await VideoRepository(session).get(owner_id, video_id)
                reader = SSERepository(session)
                rendition_rows = await reader.list_renditions(owner_id, video_id)
                watermark = await reader.max_event_id(video_id)
            if video_row is None:
                return
            yield {
                "event": "snapshot",
                "id": str(watermark),
                "data": json.dumps(_to_snapshot(video_row, rendition_rows)),
            }
        else:
            watermark = last_event_id

        while True:
            async with session_scope(sessions) as session:
                video_row = await VideoRepository(session).get(owner_id, video_id)
                new_rows = await SSERepository(session).list_events_after(video_id, watermark)

            for row in new_rows:
                yield {
                    "event": sse_event_name(row),
                    "id": str(row.id),
                    "data": json.dumps(row.payload),
                }
                watermark = row.id

            # Re-checked every pass, not inferred from event payloads: the
            # authoritative "is this over" answer is the projector-owned
            # status column (ADR-0007), which also makes a reconnect after
            # the stream already ended terminate immediately instead of
            # polling Postgres forever for a client that should have closed.
            if video_row is None or video_row.status == VideoState.FAILED.value:
                return

            wakeup.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=limits.wakeup_poll_backstop_seconds)
    finally:
        broadcaster.unsubscribe(video_id, wakeup)

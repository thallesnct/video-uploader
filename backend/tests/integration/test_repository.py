"""RenditionRepository.claim()'s staleness window against real Postgres
(Phase 12, known gap from Phase 5).

STALE_AFTER used to be 2h — comfortably longer than the retry ladder's total
span (10s + 1m + 10m ~= 11m10s). A worker that crashed mid-claim left every
redelivery of the same message hitting TransientError (claim denied) against
its own still-live claim, so the message dead-lettered at ~11m10s, long
before the 2h window would ever have freed it, even though nothing was
actually wrong with the rendition. This test proves the shortened window
(5m) actually closes that gap: a claim held past STALE_AFTER is re-claimable,
one held within it is not.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pipeline.db import create_sync_engine, sync_session_scope, sync_sessions
from pipeline.models import RenditionRow, VideoRow
from pipeline.repository import RenditionRepository

OWNER = "user|repository"


@pytest.fixture()
def sessions_factory(environment: None) -> Any:
    engine = create_sync_engine()
    factory = sync_sessions(engine)
    yield factory
    engine.dispose()


def insert_video_row(sessions_factory: Any, video_id: uuid.UUID) -> None:
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


def test_a_claim_within_the_stale_window_blocks_a_second_claimant(
    sessions_factory: Any,
) -> None:
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)

    with sync_session_scope(sessions_factory) as session:
        first = RenditionRepository(session).claim(OWNER, video_id, "360p")
    assert first is True

    with sync_session_scope(sessions_factory) as session:
        second = RenditionRepository(session).claim(OWNER, video_id, "360p")
    assert second is False, "a live claim (well within STALE_AFTER) must block a rival"


def test_a_claim_past_the_stale_window_is_reclaimable(sessions_factory: Any) -> None:
    """Simulates a worker that crashed mid-job: back-date claimed_at past
    STALE_AFTER directly (no code path naturally waits 5 real minutes), then
    prove claim() treats it as abandoned — the actual behavior a crashed
    sibling's redelivered message depends on to eventually succeed instead
    of dead-lettering."""
    video_id = uuid.uuid4()
    insert_video_row(sessions_factory, video_id)

    with sync_session_scope(sessions_factory) as session:
        first = RenditionRepository(session).claim(OWNER, video_id, "360p")
    assert first is True

    stale_at = datetime.now(UTC) - RenditionRepository.STALE_AFTER - timedelta(seconds=5)
    with sync_session_scope(sessions_factory) as session:
        session.query(RenditionRow).filter(
            RenditionRow.video_id == video_id, RenditionRow.rendition == "360p"
        ).update({"claimed_at": stale_at})

    with sync_session_scope(sessions_factory) as session:
        second = RenditionRepository(session).claim(OWNER, video_id, "360p")
    assert second is True, "a claim older than STALE_AFTER must free for a new claimant"

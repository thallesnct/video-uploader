"""Data access for videos.

Every function takes `owner_id` as a **required positional argument**. That is
the point: a missing tenant filter then has to be written deliberately rather
than reached by forgetting a keyword (ADR-0016).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from pipeline.events import VideoState
from pipeline.models import RenditionRow, VideoRow

# States that occupy pipeline capacity, for quota accounting.
IN_FLIGHT_STATES = (
    VideoState.AWAITING_UPLOAD.value,
    VideoState.UPLOADED.value,
    VideoState.PROBED.value,
    VideoState.TRANSCODING.value,
    VideoState.PACKAGING.value,
)


class VideoRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        owner_id: str,
        video_id: UUID,
        *,
        filename: str,
        content_type: str,
        declared_size_bytes: int,
        object_key: str,
    ) -> VideoRow:
        row = VideoRow(
            id=video_id,
            owner_id=owner_id,
            filename=filename,
            content_type=content_type,
            declared_size_bytes=declared_size_bytes,
            object_key=object_key,
            status=VideoState.AWAITING_UPLOAD.value,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def get(self, owner_id: str, video_id: UUID) -> VideoRow | None:
        """Fetch one video. Another owner's video is indistinguishable from a
        missing one, which is the correct thing to tell a caller."""
        result = await self._session.execute(
            select(VideoRow).where(VideoRow.id == video_id, VideoRow.owner_id == owner_id)
        )
        return result.scalar_one_or_none()

    async def list_for_owner(
        self, owner_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[VideoRow]:
        result = await self._session.execute(
            select(VideoRow)
            .where(VideoRow.owner_id == owner_id)
            .order_by(VideoRow.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(result.scalars().all())

    async def count_in_flight(self, owner_id: str) -> int:
        """How much pipeline capacity this owner currently occupies (quotas)."""
        result = await self._session.execute(
            select(func.count())
            .select_from(VideoRow)
            .where(
                VideoRow.owner_id == owner_id,
                VideoRow.status.in_(IN_FLIGHT_STATES),
            )
        )
        return int(result.scalar_one())

    async def claim_upload_complete(self, owner_id: str, video_id: UUID) -> bool:
        """Move awaiting_upload -> uploaded exactly once.

        The claim pattern of ADR-0005, not a read-then-write: two concurrent
        calls to /complete must produce exactly one video.uploaded event, and
        only the caller that wins the UPDATE is allowed to publish. Returns
        False when someone else already completed it.
        """
        result = await self._session.execute(
            update(VideoRow)
            .where(
                VideoRow.id == video_id,
                VideoRow.owner_id == owner_id,
                VideoRow.status == VideoState.AWAITING_UPLOAD.value,
            )
            .values(status=VideoState.UPLOADED.value)
            .returning(VideoRow.id)
        )
        return result.scalar_one_or_none() is not None


class RenditionRepository:
    """The transcode worker's only sanctioned write path (ADR-0007, ADR-0005).

    Sync, not async: confluent-kafka's poll loop is blocking (ADR-0009), so a
    worker's DB access is too. This is the concrete enforcement of the
    column-ownership rule — its one write method touches only `attempt` and
    `claimed_at`. Nothing here can set `status`, `object_key`, or
    `failure_reason`; those belong to the projector once Phase 6 lands.
    """

    # A claim older than this is presumed abandoned (worker crashed mid-job)
    # and may be re-claimed. Set well above any realistic transcode duration —
    # a genuinely still-processing job is protected by max.poll.interval.ms and
    # the pause/resume loop (ADR-0004), not by this window.
    STALE_AFTER = timedelta(hours=2)

    def __init__(self, session: Session) -> None:
        self._session = session

    def claim(self, owner_id: str, video_id: UUID, rendition: str) -> bool:
        """Elect a single owner for this (video_id, rendition)'s work.

        Guards against a message being processed twice *concurrently* — the
        realistic case is a manual DLQ replay racing a live retry-tier message
        for the same rendition, not normal Kafka redelivery (which is
        sequential: the previous attempt has either committed or been evicted
        before a new one starts, per ADR-0004).

        Upsert rather than a plain UPDATE: no row exists yet for the first
        attempt at a rendition, since nothing creates one ahead of time.
        Returns False if another attempt already holds a live claim.
        """
        now = datetime.now(UTC)
        stale_before = now - self.STALE_AFTER
        statement = (
            insert(RenditionRow)
            .values(
                id=uuid.uuid4(),
                video_id=video_id,
                owner_id=owner_id,
                rendition=rendition,
                attempt=1,
                claimed_at=now,
            )
            .on_conflict_do_update(
                constraint="uq_renditions_video_rendition",
                set_={"attempt": RenditionRow.attempt + 1, "claimed_at": now},
                # Whether the rendition is already done is is_completed()'s
                # question, checked before claim() is ever called — this guard
                # exists only to keep two concurrent attempts from both winning.
                where=(RenditionRow.claimed_at.is_(None))
                | (RenditionRow.claimed_at < stale_before),
            )
            .returning(RenditionRow.id)
        )
        result = self._session.execute(statement)
        self._session.commit()
        return result.scalar_one_or_none() is not None

    def is_completed(self, owner_id: str, video_id: UUID, rendition: str) -> bool:
        """Read-only. Reading a projector-owned column is fine; writing it is not."""
        result = self._session.execute(
            select(RenditionRow.status).where(
                RenditionRow.video_id == video_id,
                RenditionRow.owner_id == owner_id,
                RenditionRow.rendition == rendition,
            )
        )
        return result.scalar_one_or_none() == VideoState.COMPLETED.value

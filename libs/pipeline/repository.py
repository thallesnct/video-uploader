"""Data access for videos.

Every function takes `owner_id` as a **required positional argument**. That is
the point: a missing tenant filter then has to be written deliberately rather
than reached by forgetting a keyword (ADR-0016).
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pipeline.events import VideoState
from pipeline.models import VideoRow

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

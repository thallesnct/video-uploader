"""Data access for videos.

Every function takes `owner_id` as a **required positional argument**. That is
the point: a missing tenant filter then has to be written deliberately rather
than reached by forgetting a keyword (ADR-0016).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from pipeline.events import Event, PipelineFailed, VideoState, VideoStatusChanged
from pipeline.models import EventRow, RenditionRow, VideoRow

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

    async def expire_stale_awaiting_uploads(self, owner_id: str, older_than: timedelta) -> int:
        """Fail an upload whose presigned PUT window has closed (ADR-0006
        follow-on).

        `older_than` is the presign's own expiry duration, not a separate
        guess: once it has passed, the URL is dead and the row can *never*
        be completed, so this is a fact, not a heuristic. Run opportunistically
        on the read/write paths that care (create, list) rather than by a
        background sweeper — no new service to deploy, and the check rides
        along with a query that was happening anyway. `awaiting_upload` never
        enters the Kafka pipeline (VideoUploaded is only published from
        `/complete`), so writing `failed` here directly does not collide with
        the projector's ownership of state columns post-upload (ADR-0007).
        """
        result = await self._session.execute(
            update(VideoRow)
            .where(
                VideoRow.owner_id == owner_id,
                VideoRow.status == VideoState.AWAITING_UPLOAD.value,
                VideoRow.created_at < datetime.now(UTC) - older_than,
            )
            .values(status=VideoState.FAILED.value, failure_reason="upload window expired")
            .returning(VideoRow.id)
        )
        return len(result.scalars().all())

    async def delete_awaiting_upload(self, owner_id: str, video_id: UUID) -> bool:
        """Cancel an upload that never completed (ADR-0006 follow-on) — the
        only state this is allowed to touch. A claim, not read-then-delete:
        a `/complete` racing this call must not leave an inconsistent result
        (ADR-0005). Safe as a hard delete: nothing downstream can reference
        this video_id yet, since VideoUploaded is never published before
        `/complete` succeeds, so no rendition or event row can exist for it.
        """
        result = await self._session.execute(
            delete(VideoRow)
            .where(
                VideoRow.id == video_id,
                VideoRow.owner_id == owner_id,
                VideoRow.status == VideoState.AWAITING_UPLOAD.value,
            )
            .returning(VideoRow.id)
        )
        return result.scalar_one_or_none() is not None

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


class ProjectorRepository:
    """The projector's write path — the sole owner of STATE columns (ADR-0007).

    Sync, not async: same reason as RenditionRepository (ADR-0009). Every write
    method is an upsert, so replaying a Kafka partition is safe by construction;
    the caller is expected to run one `apply()` per handler invocation inside a
    single transaction (`pipeline.db.sync_session_scope`) and commit only once,
    so the videos/renditions upsert and the events-log append can never land in
    two different transactions and diverge on a crash between them.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def apply(self, event: Event) -> None:
        if isinstance(event, VideoStatusChanged):
            self._apply_status(event)
        elif isinstance(event, PipelineFailed):
            self._apply_failure(event)
        else:
            raise TypeError(f"projector cannot apply event type {type(event).__name__}")
        self._append_event(event)

    def _apply_status(self, event: VideoStatusChanged) -> None:
        values: dict[str, object] = {"status": event.state.value}
        for field in ("duration_s", "width", "height", "expected_renditions"):
            value = getattr(event, field)
            if value is not None:
                values[field] = value
        self._session.execute(
            update(VideoRow)
            .where(VideoRow.id == event.video_id, VideoRow.owner_id == event.owner_id)
            .values(**values)
        )

        # rendition_object_key is only ever set once a rendition has actually
        # finished (worker_transcode's _announce); a rendition-scoped status
        # event with no object key yet has no STATE data worth writing.
        if event.rendition is not None and event.rendition_object_key is not None:
            now = datetime.now(UTC)
            statement = (
                insert(RenditionRow)
                .values(
                    id=uuid.uuid4(),
                    video_id=event.video_id,
                    owner_id=event.owner_id,
                    rendition=event.rendition,
                    status=VideoState.COMPLETED.value,
                    object_key=event.rendition_object_key,
                    completed_at=now,
                )
                .on_conflict_do_update(
                    constraint="uq_renditions_video_rendition",
                    set_={
                        "status": VideoState.COMPLETED.value,
                        "object_key": event.rendition_object_key,
                        "completed_at": now,
                    },
                )
            )
            self._session.execute(statement)

    def _apply_failure(self, event: PipelineFailed) -> None:
        """Every pipeline.failed this system emits today is terminal — it is
        only produced from the DLQ branch (ADR-0005 follow-on) — so its arrival
        is treated unconditionally as a terminal failure, not a retry signal."""
        self._session.execute(
            update(VideoRow)
            .where(VideoRow.id == event.video_id, VideoRow.owner_id == event.owner_id)
            .values(status=VideoState.FAILED.value, failure_reason=event.reason)
        )
        if event.rendition is not None:
            statement = (
                insert(RenditionRow)
                .values(
                    id=uuid.uuid4(),
                    video_id=event.video_id,
                    owner_id=event.owner_id,
                    rendition=event.rendition,
                    status=VideoState.FAILED.value,
                    failure_reason=event.reason,
                )
                .on_conflict_do_update(
                    constraint="uq_renditions_video_rendition",
                    set_={"status": VideoState.FAILED.value, "failure_reason": event.reason},
                )
            )
            self._session.execute(statement)

    def _append_event(self, event: Event) -> None:
        """De-duplicated by event_id, not by (video_id, offset): a replayed
        partition after a crash between the DB commit and the Kafka offset
        commit must insert nothing twice (ADR-0007, ADR-0008)."""
        statement = (
            insert(EventRow)
            .values(
                video_id=event.video_id,
                event_id=event.event_id,
                type=event.type,
                payload=json.loads(event.serialize()),
            )
            .on_conflict_do_nothing(index_elements=["event_id"])
        )
        self._session.execute(statement)


class SSERepository:
    """Read-only queries backing the SSE gateway (ADR-0008 follow-on).

    Async, unlike RenditionRepository/ProjectorRepository — this serves the
    API, not a confluent-kafka worker (ADR-0009). Both list_events_after and
    max_event_id are scoped by video_id only, since events has no owner_id
    column; the caller must have already checked video ownership via
    VideoRepository.get() before ever reaching here, exactly as to_response()
    already assumes elsewhere in the API.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_renditions(self, owner_id: str, video_id: UUID) -> list[RenditionRow]:
        result = await self._session.execute(
            select(RenditionRow)
            .where(RenditionRow.owner_id == owner_id, RenditionRow.video_id == video_id)
            .order_by(RenditionRow.rendition)
        )
        return list(result.scalars().all())

    async def list_events_after(self, video_id: UUID, after_id: int) -> list[EventRow]:
        result = await self._session.execute(
            select(EventRow)
            .where(EventRow.video_id == video_id, EventRow.id > after_id)
            .order_by(EventRow.id)
        )
        return list(result.scalars().all())

    async def max_event_id(self, video_id: UUID) -> int:
        result = await self._session.execute(
            select(func.max(EventRow.id)).where(EventRow.video_id == video_id)
        )
        return int(result.scalar_one() or 0)

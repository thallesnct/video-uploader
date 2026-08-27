"""The read-model tables (ADR-0007).

Written only by the projector once Phase 6 lands. The API creates the `videos`
row at upload time because that row exists before any event does — it is the
record of intent that the presigned URL is issued against.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class VideoRow(Base):
    __tablename__ = "videos"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    # Not null and indexed: every query filters on it, and a null owner would be
    # a row nobody can see and nobody can clean up (ADR-0016).
    owner_id: Mapped[str] = mapped_column(String(255), nullable=False)

    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    content_type: Mapped[str] = mapped_column(String(128), nullable=False)
    declared_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)

    # Filled by the probe stage (Phase 4); unknown until then.
    duration_s: Mapped[float | None] = mapped_column(Numeric(10, 3), nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # The ladder chosen for THIS source. The packaging join waits on exactly this
    # set (ADR-0013), so it is data, not a constant.
    expected_renditions: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)

    # Filled by the thumbnail stage (Phase 9); unknown until then.
    poster_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    sprite_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    vtt_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # Filled by the packager (Phase 9, ADR-0013). NULL means "not packaged
    # yet" — `status` already distinguishes that from "packaging in progress".
    master_playlist_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    failure_reason: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        # Listing is always "this owner's videos, newest first".
        Index("ix_videos_owner_created", "owner_id", created_at.desc()),
        # Counting in-flight work for quota checks (ADR-0016).
        Index("ix_videos_owner_status", "owner_id", "status"),
    )


class RenditionRow(Base):
    """Two column groups, two owners (ADR-0007's column-ownership rule):

    STATE  — status, object_key, failure_reason, completed_at: projector-owned
             once Phase 6 lands. Untouched by the Phase 5 transcode worker.
    CLAIM  — attempt, claimed_at: transcode-worker-owned, used only to elect a
             single owner for expensive work when a message could be in flight
             twice (e.g. a manual DLQ replay racing a live retry).

    `libs/pipeline/repository.py`'s `RenditionRepository` enforces this split
    in code: its only write method touches claim columns, never state ones.
    """

    __tablename__ = "renditions"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    video_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("videos.id"), nullable=False
    )
    owner_id: Mapped[str] = mapped_column(String(255), nullable=False)
    rendition: Mapped[str] = mapped_column(String(16), nullable=False)

    # --- STATE: projector-owned (Phase 6) ---
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    object_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # This rendition's own HLS playlist (Phase 9), distinct from object_key's
    # single MP4 — the master playlist the packager writes references these.
    playlist_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # --- CLAIM: transcode-worker-owned ---
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("video_id", "rendition", name="uq_renditions_video_rendition"),
        Index("ix_renditions_video", "video_id"),
        Index("ix_renditions_owner_status", "owner_id", "status"),
    )


class EventRow(Base):
    """Append-only log backing SSE `Last-Event-ID` replay (ADR-0007, ADR-0008).

    Written by the projector, in the same transaction as its videos/renditions
    upsert, before the Kafka offset is committed. `event_id` is unique so a
    replayed partition (crash between the DB commit and the offset commit)
    inserts nothing twice — the same idempotency shape as the upserts it sits
    next to, not a separate mechanism.
    """

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    video_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("videos.id"), nullable=False
    )
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, unique=True)
    type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        # The SSE resume query: "everything for this video after id N".
        Index("ix_events_video_id", "video_id", "id"),
    )

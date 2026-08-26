"""The read-model tables (ADR-0007).

Written only by the projector once Phase 6 lands. The API creates the `videos`
row at upload time because that row exists before any event does — it is the
record of intent that the presigned URL is issued against.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Index, Integer, Numeric, String, func
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

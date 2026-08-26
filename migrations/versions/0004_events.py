"""events table — append-only log backing SSE Last-Event-ID replay

Revision ID: 0004_events
Revises: 0003_renditions
Create Date: 2026-08-26

Written by the projector (Phase 6), in the same transaction as its
videos/renditions upsert. `event_id` is unique so replaying a partition after a
crash between the DB commit and the Kafka offset commit inserts nothing twice.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_events"
down_revision: str | None = "0003_renditions"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "video_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("videos.id"),
            nullable=False,
        ),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_events_video_id", "events", ["video_id", "id"])


def downgrade() -> None:
    op.drop_index("ix_events_video_id", table_name="events")
    op.drop_table("events")

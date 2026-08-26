"""videos table with owner scoping

Revision ID: 0002_videos
Revises: 0001_baseline
Create Date: 2026-08-26

owner_id is here from the first real table rather than added later: retrofitting
it would mean rewriting every object key and backfilling every row (ADR-0016).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_videos"
down_revision: str | None = "0001_baseline"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "videos",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_id", sa.String(255), nullable=False),
        sa.Column("filename", sa.String(512), nullable=False),
        sa.Column("content_type", sa.String(128), nullable=False),
        sa.Column("declared_size_bytes", sa.Integer(), nullable=False),
        sa.Column("object_key", sa.String(1024), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("duration_s", sa.Numeric(10, 3), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("expected_renditions", postgresql.JSONB(), nullable=True),
        sa.Column("failure_reason", sa.String(1024), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_videos_owner_created", "videos", ["owner_id", sa.text("created_at DESC")])
    op.create_index("ix_videos_owner_status", "videos", ["owner_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_videos_owner_status", table_name="videos")
    op.drop_index("ix_videos_owner_created", table_name="videos")
    op.drop_table("videos")

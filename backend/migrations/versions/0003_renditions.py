"""renditions table — the worker's claim, and the projector's future state

Revision ID: 0003_renditions
Revises: 0002_videos
Create Date: 2026-08-26

This is where ADR-0007's column-ownership rule becomes concrete rather than
theoretical. Two column groups, two owners, enforced by convention in code
(there is no DB-level enforcement for this — see the repository docstring):

- STATE (status, object_key, failure_reason, completed_at): the projector's,
  once Phase 6 lands. NULL/unset until then. Nothing in Phase 5 writes these.
- CLAIM (attempt, claimed_at): the transcode worker's, used to elect a single
  owner for expensive work when a message could be in flight twice (e.g. a
  manual DLQ replay racing a live retry). Never projected, never rendered.

UNIQUE (video_id, rendition) is the idempotency backstop ADR-0005 describes.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_renditions"
down_revision: str | None = "0002_videos"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "renditions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "video_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("videos.id"),
            nullable=False,
        ),
        # Identity, set once at claim time — same treatment as videos.owner_id.
        sa.Column("owner_id", sa.String(255), nullable=False),
        sa.Column("rendition", sa.String(16), nullable=False),
        # --- STATE: projector-owned (Phase 6). NULL until then. ---
        sa.Column("status", sa.String(32), nullable=True),
        sa.Column("object_key", sa.String(1024), nullable=True),
        sa.Column("failure_reason", sa.String(1024), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        # --- CLAIM: transcode-worker-owned. ---
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("video_id", "rendition", name="uq_renditions_video_rendition"),
    )
    op.create_index("ix_renditions_video", "renditions", ["video_id"])
    op.create_index("ix_renditions_owner_status", "renditions", ["owner_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_renditions_owner_status", table_name="renditions")
    op.drop_index("ix_renditions_video", table_name="renditions")
    op.drop_table("renditions")

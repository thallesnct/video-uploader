"""worker_package's claim columns (ADR-0013 follow-on)

Revision ID: 0006_packager_claim_columns
Revises: 0005_thumbnails_and_hls
Create Date: 2026-08-27

All three are CLAIM, worker_package-owned (ADR-0007) — never read or written
by the projector. They deliberately duplicate data the projector's own
columns already hold (`videos.expected_renditions`, `renditions.status`):
worker_package must never read those, because they are written from a
different topic (video.status) than the ones it consumes (video.probed,
rendition.completed), with no ordering guarantee between them. See the
ADR-0013 follow-on for the race this replaces.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006_packager_claim_columns"
down_revision: str | None = "0005_thumbnails_and_hls"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "videos", sa.Column("packager_expected_renditions", postgresql.JSONB(), nullable=True)
    )
    op.add_column(
        "videos", sa.Column("packaging_claimed_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("renditions", sa.Column("packager_playlist_key", sa.String(1024), nullable=True))


def downgrade() -> None:
    op.drop_column("renditions", "packager_playlist_key")
    op.drop_column("videos", "packaging_claimed_at")
    op.drop_column("videos", "packager_expected_renditions")

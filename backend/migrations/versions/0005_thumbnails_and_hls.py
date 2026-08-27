"""thumbnails, sprite/VTT, and HLS playlist columns (Phase 9)

Revision ID: 0005_thumbnails_and_hls
Revises: 0004_events
Create Date: 2026-08-27

All five columns are STATE (ADR-0007): projector-owned, written only from
`video.status` fields the thumbnail/transcode/package workers add in this same
phase. Nothing here is a worker's claim column — those, if `worker_package`
needs one for the completion join (ADR-0013), land in their own later
migration alongside that worker, not speculatively here.

`videos.master_playlist_key` NULL is exactly "not packaged yet"; no separate
flag is needed to tell that state apart from "packaging in progress" — the
video's own `status` column already carries that distinction.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0005_thumbnails_and_hls"
down_revision: str | None = "0004_events"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column("videos", sa.Column("poster_key", sa.String(1024), nullable=True))
    op.add_column("videos", sa.Column("sprite_key", sa.String(1024), nullable=True))
    op.add_column("videos", sa.Column("vtt_key", sa.String(1024), nullable=True))
    op.add_column("videos", sa.Column("master_playlist_key", sa.String(1024), nullable=True))
    op.add_column("renditions", sa.Column("playlist_key", sa.String(1024), nullable=True))


def downgrade() -> None:
    op.drop_column("renditions", "playlist_key")
    op.drop_column("videos", "master_playlist_key")
    op.drop_column("videos", "vtt_key")
    op.drop_column("videos", "sprite_key")
    op.drop_column("videos", "poster_key")

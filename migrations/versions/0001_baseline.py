"""baseline — empty schema

Revision ID: 0001_baseline
Revises:
Create Date: 2026-08-26

Deliberately empty. It establishes the alembic_version table so Phase 6's read
model (ADR-0007) lands as an ordinary revision on top of a known starting point.
"""
from __future__ import annotations

revision: str = "0001_baseline"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

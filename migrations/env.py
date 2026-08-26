"""Alembic environment.

The read model's tables arrive in Phase 6 (ADR-0007); this baseline exists so
migrations are wired from the start and the first schema change is a normal,
reviewable revision rather than a scramble.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Never commit a connection string; fail loudly if it is missing (ADR-0015).
database_url = os.environ.get("DATABASE_URL")
if not database_url:
    raise RuntimeError("DATABASE_URL is not set — see .env.example")
# Alembic runs synchronously; strip the async driver if one was configured.
config.set_main_option("sqlalchemy.url", database_url.replace("+asyncpg", "+psycopg"))

# Phase 6 points this at the models' MetaData to enable autogenerate.
target_metadata = None


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

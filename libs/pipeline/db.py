"""Async database access.

Engines are created explicitly rather than held as a module singleton: an engine
is bound to the event loop that created it, and a shared one across differently
scoped test loops fails in ways that take an afternoon to understand.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from pipeline.settings import database_settings


def async_url(url: str) -> str:
    """Force the async driver.

    DATABASE_URL is written for Alembic, which runs synchronously. The
    application needs asyncpg, so the driver is swapped here rather than
    maintaining two nearly identical environment variables.
    """
    for sync_driver in ("+psycopg2", "+psycopg", "+pg8000"):
        url = url.replace(sync_driver, "+asyncpg")
    if "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def create_engine(url: str | None = None, **kwargs: object) -> AsyncEngine:
    settings = database_settings()
    return create_async_engine(
        async_url(url or settings.url),
        pool_size=settings.pool_size,
        pool_pre_ping=True,  # a recycled connection after a failover is not fatal
        **kwargs,
    )


def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """One transaction per unit of work, rolled back on error."""
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

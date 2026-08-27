"""Database access — async for the API, sync for the workers (ADR-0009).

Engines are created explicitly rather than held as a module singleton: an async
engine is bound to the event loop that created it, and a shared one across
differently scoped test loops fails in ways that take an afternoon to
understand. Workers get the same treatment even though they have no loop to
bind to, so both paths are used identically.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager

from sqlalchemy import Engine
from sqlalchemy import create_engine as create_sync_engine_
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

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


def sync_url(url: str) -> str:
    """Force the sync driver — the counterpart to async_url, for workers."""
    for async_driver in ("+asyncpg",):
        url = url.replace(async_driver, "+psycopg")
    if "+psycopg" not in url and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def create_sync_engine(url: str | None = None, **kwargs: object) -> Engine:
    """For workers: confluent-kafka's poll loop is blocking, so its DB access
    is too (ADR-0009) — an async session here would need its own event loop for
    no benefit."""
    settings = database_settings()
    return create_sync_engine_(
        sync_url(url or settings.url), pool_size=settings.pool_size, pool_pre_ping=True, **kwargs
    )


def sync_sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


@contextmanager
def sync_session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    """One transaction per unit of work, rolled back on error."""
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

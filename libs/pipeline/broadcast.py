"""Fan-out wake-up signal for the SSE gateway (ADR-0008).

Each API replica runs one StatusBroadcaster, consuming video.status and
pipeline.failed with a unique, ephemeral group id starting at latest — this is
what makes every replica see every event (Kafka-native pub/sub), rather than a
shared group load-balancing partitions across replicas and delivering each
event to only one of them.

Deliberately does not parse the message payload: the id an SSE client needs is
the events table row id, assigned by the projector — a different,
unsynchronized consumer of the same topic. Parsing here and inventing an id
would not be the id a Last-Event-ID reconnect can resume from. Instead this
only reads the message key (already str(video_id).encode(), per Event.key) to
know which video changed, and treats that purely as a wake-up: the caller
always re-reads Postgres for content (ADR-0008 follow-on, 2026-08-27).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import Any
from uuid import UUID

from pipeline.settings import kafka_settings
from pipeline.topics import PIPELINE_FAILED, VIDEO_STATUS

log = logging.getLogger(__name__)


class StatusBroadcaster:
    def __init__(self, client: Any | None = None) -> None:
        self._client = client
        self._task: asyncio.Task[None] | None = None
        self._wakeups: dict[UUID, set[asyncio.Event]] = {}

    async def start(self) -> None:
        """Connect and consume until stop(). Blocks until ready to miss
        nothing published from this point on.

        aiokafka's start() awaits partition assignment internally when topics
        are passed to the constructor (verified against a real broker), but
        assignment is not the same milestone as "latest" being resolved to a
        concrete fetch position — that resolution is otherwise lazy, and a
        message published in the gap can be missed. Empirically reproduced
        with two consumer groups starting close together (deterministic, not
        rare, in that shape) and fixed by seek_to_end(), which forces the
        resolution eagerly instead of waiting for the first fetch.
        """
        if self._client is None:
            from aiokafka import AIOKafkaConsumer

            self._client = AIOKafkaConsumer(
                VIDEO_STATUS,
                PIPELINE_FAILED,
                bootstrap_servers=kafka_settings().bootstrap_servers,
                group_id=f"sse-gateway-{uuid.uuid4()}",
                auto_offset_reset="latest",
                # Never committed — an ephemeral group id already keeps
                # __consumer_offsets clean (ADR-0008's consequence note), and
                # nothing here is ever redelivered or resumed from an offset.
                enable_auto_commit=False,
            )
        await self._client.start()
        await self._client.seek_to_end()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._client is not None:
            await self._client.stop()

    async def _run(self) -> None:
        client = self._client  # a local, so the None check actually narrows
        if client is None:
            raise RuntimeError("StatusBroadcaster._run() called before start()")
        async for message in client:
            try:
                video_id = uuid.UUID(message.key.decode())
            except (AttributeError, ValueError):
                log.warning("video.status/pipeline.failed message with a non-video_id key")
                continue
            for wakeup in self._wakeups.get(video_id, ()):
                wakeup.set()

    def subscribe(self, video_id: UUID) -> asyncio.Event:
        wakeup = asyncio.Event()
        self._wakeups.setdefault(video_id, set()).add(wakeup)
        return wakeup

    def unsubscribe(self, video_id: UUID, wakeup: asyncio.Event) -> None:
        subscribers = self._wakeups.get(video_id)
        if subscribers is None:
            return
        subscribers.discard(wakeup)
        if not subscribers:
            del self._wakeups[video_id]

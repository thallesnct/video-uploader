"""The upload path and read API (Phase 3).

Bytes never pass through this service: the browser PUTs straight to the object
store with a presigned URL, and the API only issues that URL and records intent
(ADR-0001, ADR-0006). That is why it scales with request count rather than with
upload volume.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor  # noqa: E402
from pipeline import storage
from pipeline.auth import AuthError, Principal, TokenVerifier, bearer_token
from pipeline.broadcast import StatusBroadcaster
from pipeline.db import create_engine, session_scope, sessions
from pipeline.events import VideoState, VideoStatusChanged, VideoUploaded
from pipeline.obs import setup_tracing
from pipeline.producer import AsyncEventProducer
from pipeline.repository import VideoRepository
from pipeline.settings import observability_settings, quota_settings, sse_settings
from pipeline.topics import VIDEO_STATUS, VIDEO_UPLOADED
from sqlalchemy import text
from sse_starlette.sse import EventSourceResponse

from services.api.schemas import (
    ALLOWED_CONTENT_TYPES,
    CreateVideoRequest,
    CreateVideoResponse,
    VideoResponse,
)
from services.api.sse import sse_stream

SERVICE = "api"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own the producer and the engine for the life of the process.

    A producer that was never started fails on first publish, and one never
    stopped drops buffered messages on shutdown (ADR-0015 graceful shutdown).
    """
    app.state.engine = create_engine()
    app.state.sessions = sessions(app.state.engine)
    app.state.producer = AsyncEventProducer(service=SERVICE)
    app.state.verifier = TokenVerifier()
    app.state.store = storage.object_store()
    app.state.broadcaster = StatusBroadcaster()
    app.state.sse_active = 0
    await app.state.producer.start()
    await app.state.broadcaster.start()
    try:
        yield
    finally:
        await app.state.broadcaster.stop()
        await app.state.producer.stop()
        await app.state.engine.dispose()


app = FastAPI(title="video pipeline API", lifespan=lifespan)

# Instrumented at import, not inside the lifespan: Starlette builds its
# middleware stack before lifespan runs, so instrumenting there silently never
# takes effect — the app works, and every message quietly loses its traceparent.
#
# The API is where a video's trace begins (ADR-0010). Without an active server
# span there is nothing for the propagator to inject, and the trace is broken at
# the very first hop.
setup_tracing(SERVICE)
FastAPIInstrumentor.instrument_app(app)


# ------------------------------------------------------------------ dependencies


async def current_principal(
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    try:
        return app.state.verifier.verify(bearer_token(authorization))
    except AuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


Caller = Annotated[Principal, Depends(current_principal)]


# ----------------------------------------------------------------------- probes


@app.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness. Checks nothing external, on purpose (ADR-0015)."""
    return {"status": "alive"}


@app.get("/readyz", include_in_schema=False)
async def readyz(response: Response) -> dict[str, str]:
    """Readiness. Checks dependencies, and removes us from the LB if they fail."""
    detail: dict[str, str] = {}
    try:
        async with app.state.sessions() as session:
            await session.execute(text("select 1"))
        detail["postgres"] = "ok"
    except Exception as exc:
        detail["postgres"] = f"error: {exc}"

    try:
        await asyncio.to_thread(app.state.store.head, "readiness-probe")
        detail["object_store"] = "ok"
    except Exception as exc:
        detail["object_store"] = f"error: {exc}"

    ready = all(value == "ok" for value in detail.values())
    response.status_code = 200 if ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ready" if ready else "not ready", **detail}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ------------------------------------------------------------------ upload path


@app.post("/videos", status_code=status.HTTP_201_CREATED)
async def create_video(request: CreateVideoRequest, caller: Caller) -> CreateVideoResponse:
    """Record intent and hand back a presigned PUT.

    The URL is signed for one exact key under the caller's own prefix, so it
    cannot be redirected into another tenant's namespace (ADR-0016).
    """
    quotas = quota_settings()
    if request.size_bytes > quotas.max_upload_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"file exceeds the {quotas.max_upload_bytes} byte limit",
        )

    video_id = uuid.uuid4()
    extension = ALLOWED_CONTENT_TYPES[request.content_type]
    object_key = storage.source_key(caller.owner_id, video_id, extension)

    async with session_scope(app.state.sessions) as session:
        repository = VideoRepository(session)
        in_flight = await repository.count_in_flight(caller.owner_id)
        if in_flight >= quotas.max_videos_in_flight:
            # 429 rather than 403: this is a rate problem, not a permission one,
            # and the caller should retry once their earlier videos finish.
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"{in_flight} videos already in flight; "
                    f"the limit is {quotas.max_videos_in_flight}"
                ),
            )
        await repository.create(
            caller.owner_id,
            video_id,
            filename=request.filename,
            content_type=request.content_type,
            declared_size_bytes=request.size_bytes,
            object_key=object_key,
        )

    upload_url = await asyncio.to_thread(
        app.state.store.presign_put, object_key, request.content_type
    )
    return CreateVideoResponse(
        video_id=video_id,
        upload_url=upload_url,
        object_key=object_key,
        expires_in_s=app.state.store.presign_put_expiry_s,
    )


@app.post("/videos/{video_id}/complete")
async def complete_upload(video_id: uuid.UUID, caller: Caller) -> VideoResponse:
    """Verify the object landed, then publish video.uploaded exactly once."""
    async with session_scope(app.state.sessions) as session:
        repository = VideoRepository(session)
        row = await repository.get(caller.owner_id, video_id)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "video not found")

        head = await asyncio.to_thread(app.state.store.head, row.object_key)
        if head is None:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "no object at the presigned key — the upload did not complete",
            )
        actual_size = int(head.get("ContentLength", 0))

        # A presigned PUT cannot enforce a size ceiling by itself, so the real
        # check happens here, against what was actually stored (ADR-0006).
        if actual_size > quota_settings().max_upload_bytes:
            await asyncio.to_thread(
                app.state.store.client.delete_object,
                Bucket=app.state.store.bucket,
                Key=row.object_key,
            )
            raise HTTPException(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                "uploaded object exceeds the size limit and was discarded",
            )

        # The claim, not a read-then-write: two concurrent /complete calls must
        # produce exactly one video.uploaded event (ADR-0005).
        claimed = await repository.claim_upload_complete(caller.owner_id, video_id)
        refreshed = await repository.get(caller.owner_id, video_id)

    if refreshed is None:  # deleted between the claim and the re-read
        raise HTTPException(status.HTTP_404_NOT_FOUND, "video not found")

    if claimed:
        uploaded = VideoUploaded(
            video_id=video_id,
            owner_id=caller.owner_id,
            producer=SERVICE,
            object_key=refreshed.object_key,
            filename=refreshed.filename,
            size_bytes=actual_size,
            content_type=refreshed.content_type,
        )
        await app.state.producer.publish(VIDEO_UPLOADED, uploaded)
        await app.state.producer.publish(
            VIDEO_STATUS,
            VideoStatusChanged(
                video_id=video_id,
                owner_id=caller.owner_id,
                producer=SERVICE,
                state=VideoState.UPLOADED,
            ),
        )

    return to_response(refreshed)


@app.get("/videos")
async def list_videos(caller: Caller, limit: int = 50, offset: int = 0) -> list[VideoResponse]:
    async with session_scope(app.state.sessions) as session:
        rows = await VideoRepository(session).list_for_owner(
            caller.owner_id, limit=min(limit, 200), offset=offset
        )
    return [to_response(row) for row in rows]


@app.get("/videos/{video_id}")
async def get_video(video_id: uuid.UUID, caller: Caller) -> VideoResponse:
    async with session_scope(app.state.sessions) as session:
        row = await VideoRepository(session).get(caller.owner_id, video_id)
    if row is None:
        # Another tenant's video is reported as missing rather than forbidden:
        # a 403 would confirm the id exists.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "video not found")
    return to_response(row)


# --------------------------------------------------------------------- SSE


@app.get("/videos/{video_id}/events")
async def video_events(
    video_id: uuid.UUID,
    caller: Caller,
    last_event_id: Annotated[int | None, Header(alias="Last-Event-ID")] = None,
) -> EventSourceResponse:
    """Snapshot then live deltas (ADR-0008), or replay from Last-Event-ID.

    The concurrent-stream cap is checked here, advisory-only: two requests
    can both pass it before either increments (the actual counting happens
    once streaming starts, in counted_stream's try/finally, which is what
    guarantees the decrement runs even if the client never reads a byte). A
    small, bounded overshoot of a soft resource limit is an acceptable
    trade for not needing a lock on every connection attempt.
    """
    limits = sse_settings()
    if app.state.sse_active >= limits.max_concurrent_streams:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "too many concurrent event streams"
        )

    async with session_scope(app.state.sessions) as session:
        row = await VideoRepository(session).get(caller.owner_id, video_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "video not found")

    async def counted_stream() -> AsyncIterator[dict[str, str]]:
        app.state.sse_active += 1
        try:
            async for item in sse_stream(
                app.state.sessions,
                app.state.broadcaster,
                caller.owner_id,
                video_id,
                last_event_id,
            ):
                yield item
        finally:
            app.state.sse_active -= 1

    return EventSourceResponse(counted_stream(), ping=limits.ping_seconds)


def to_response(row: Any) -> VideoResponse:
    return VideoResponse(
        video_id=row.id,
        filename=row.filename,
        status=row.status,
        size_bytes=row.declared_size_bytes,
        duration_s=float(row.duration_s) if row.duration_s is not None else None,
        width=row.width,
        height=row.height,
        expected_renditions=row.expected_renditions,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
    )


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=observability_settings().metrics_port)  # noqa: S104

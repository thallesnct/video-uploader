"""The event contract every service shares (ADR-0003).

One definition, imported everywhere, so a field rename cannot break a consumer
at runtime on a message that will then be redelivered forever.

Two rules make deploys safe and are enforced by tests rather than by a registry:
adding an optional field is free, and consumers ignore fields they do not know,
so a new producer can ship before its consumers. Removing or retyping a field is
breaking: bump SCHEMA_VERSION, write an ADR, dual-read for a release.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

SCHEMA_VERSION = 1

# "360p", "1080p" — the ladder is chosen per video from the probe, so this is a
# shape constraint rather than a fixed set (ADR-0012).
Rendition = Annotated[str, StringConstraints(pattern=r"^\d{3,4}p$")]


class VideoState(StrEnum):
    """User-facing pipeline state. This is what the browser renders."""

    AWAITING_UPLOAD = "awaiting_upload"
    UPLOADED = "uploaded"
    PROBED = "probed"
    TRANSCODING = "transcoding"
    PACKAGING = "packaging"
    COMPLETED = "completed"
    FAILED = "failed"


def _now() -> datetime:
    return datetime.now(UTC)


class Event(BaseModel):
    """Fields every event carries, whatever its type."""

    model_config = ConfigDict(
        # Forward compatibility: an old consumer must tolerate a new producer's
        # extra field instead of crashing on it.
        extra="ignore",
        frozen=True,
    )

    type: str
    event_id: UUID = Field(default_factory=uuid4)
    # Also the partition key: all events for one video keep their order (ADR-0002).
    video_id: UUID
    # Carried on every message so a worker can build owner-scoped object keys and
    # authorize without a database round trip (ADR-0016 §6). Tenancy lives in the
    # data, never in the topology.
    owner_id: str
    occurred_at: datetime = Field(default_factory=_now)
    schema_version: int = SCHEMA_VERSION
    producer: str

    def serialize(self) -> bytes:
        return self.model_dump_json().encode()

    @property
    def key(self) -> bytes:
        """Kafka message key. Per-video ordering depends on this being video_id."""
        return str(self.video_id).encode()


class VideoUploaded(Event):
    type: Literal["video.uploaded"] = "video.uploaded"
    object_key: str
    filename: str
    size_bytes: int
    content_type: str


class VideoProbed(Event):
    type: Literal["video.probed"] = "video.probed"
    duration_s: float
    width: int
    height: int
    video_codec: str
    audio_codec: str | None = None
    # Chosen from the source, never a fixed ladder — the packaging join in
    # ADR-0013 waits on exactly this set.
    expected_renditions: list[Rendition]


class RenditionRequested(Event):
    type: Literal["rendition.requested"] = "rendition.requested"
    rendition: Rendition
    source_key: str
    target_key: str
    # Additive under ADR-0003. Without it the transcode worker cannot compute
    # transcode_realtime_ratio (obs.py) without re-probing or a DB round trip.
    duration_s: float


class RenditionCompleted(Event):
    type: Literal["rendition.completed"] = "rendition.completed"
    rendition: Rendition
    object_key: str
    size_bytes: int
    transcode_seconds: float


class ThumbnailCompleted(Event):
    type: Literal["thumbnail.completed"] = "thumbnail.completed"
    poster_key: str
    sprite_key: str
    vtt_key: str


class VideoCompleted(Event):
    type: Literal["video.completed"] = "video.completed"
    master_playlist_key: str
    renditions: list[Rendition]


class PipelineFailed(Event):
    type: Literal["pipeline.failed"] = "pipeline.failed"
    stage: str
    reason: str
    retry_count: int = 0
    # False means it will be retried; True means it reached the DLQ (ADR-0005).
    terminal: bool = False
    rendition: Rendition | None = None


class VideoStatusChanged(Event):
    """The client-facing progress event carried on the video.status topic.

    A deliberate seam (ADR-0002): internal stage topics can be reshaped without
    changing what the browser consumes over SSE.
    """

    type: Literal["video.status"] = "video.status"
    state: VideoState
    rendition: Rendition | None = None
    # Human/debug only — the frontend must never parse this (ADR-0007 follow-on).
    detail: str | None = None

    # --- Phase 6 additions: video.status is pinned to by both the projector
    # (ADR-0007) and the SSE gateway (ADR-0008), so it must carry everything
    # either needs rather than requiring them to also read internal stage
    # topics. All optional and additive — does not bump SCHEMA_VERSION. ---

    # Populated by the probe worker on a video-level status change.
    duration_s: float | None = None
    width: int | None = None
    height: int | None = None
    expected_renditions: list[Rendition] | None = None

    # Populated by the transcode worker; only ever set together with
    # `rendition`, never on a video-level status change. Named for the
    # rendition explicitly rather than a bare `object_key`/`size_bytes` so a
    # consumer cannot confuse which row they belong to.
    rendition_object_key: str | None = None
    rendition_size_bytes: int | None = None

    # --- Phase 9 additions: same additive rule (ADR-0003), no version bump. ---

    # Populated by the thumbnail worker on a video-level status change.
    poster_key: str | None = None
    sprite_key: str | None = None
    vtt_key: str | None = None

    # Populated by the transcode worker; only ever set together with
    # `rendition`, mirroring rendition_object_key — this rendition's own HLS
    # playlist, not the master the packager writes.
    rendition_playlist_key: str | None = None

    # Populated by the packager on the video-level transition to `completed`.
    master_playlist_key: str | None = None


EVENT_TYPES: dict[str, type[Event]] = {
    cls.model_fields["type"].default: cls
    for cls in (
        VideoUploaded,
        VideoProbed,
        RenditionRequested,
        RenditionCompleted,
        ThumbnailCompleted,
        VideoCompleted,
        PipelineFailed,
        VideoStatusChanged,
    )
}

E = TypeVar("E", bound=Event)


class UnknownEventType(ValueError):
    """Raised for a message whose type no consumer in this build understands."""


def parse(raw: bytes | str) -> Event:
    """Parse a message into its concrete event class.

    A message we cannot classify is poison, not a retry candidate: it will fail
    identically forever, so callers route it straight to the DLQ (ADR-0005).
    """
    payload: dict[str, Any] = json.loads(raw)
    event_type = payload.get("type")
    cls = EVENT_TYPES.get(str(event_type))
    if cls is None:
        raise UnknownEventType(f"no event class for type={event_type!r}")
    return cls.model_validate(payload)


def parse_as(raw: bytes | str, expected: type[E]) -> E:
    """Parse and assert the concrete type, for consumers of a single topic."""
    event = parse(raw)
    if not isinstance(event, expected):
        raise UnknownEventType(f"expected {expected.__name__}, got {type(event).__name__}")
    return event

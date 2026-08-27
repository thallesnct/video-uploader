"""Request and response bodies for the upload path."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

# Only containers the pipeline can actually probe and transcode. Anything else
# would fail in a worker minutes later, so it is rejected at the door.
ALLOWED_CONTENT_TYPES = {
    "video/mp4": "mp4",
    "video/quicktime": "mov",
    "video/x-matroska": "mkv",
    "video/webm": "webm",
    "video/x-msvideo": "avi",
}


class CreateVideoRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=512)
    content_type: str
    size_bytes: int = Field(gt=0)

    @field_validator("content_type")
    @classmethod
    def known_container(cls, value: str) -> str:
        if value not in ALLOWED_CONTENT_TYPES:
            raise ValueError(
                f"unsupported content type {value!r}; "
                f"expected one of {sorted(ALLOWED_CONTENT_TYPES)}"
            )
        return value

    @field_validator("filename")
    @classmethod
    def no_path_traversal(cls, value: str) -> str:
        """The filename is metadata only — it never becomes a path (ADR-0015).

        Rejected anyway: a caller sending one is either confused or probing.
        """
        if "/" in value or "\\" in value or value.startswith("."):
            raise ValueError("filename must not contain path separators")
        return value


class CreateVideoResponse(BaseModel):
    video_id: UUID
    upload_url: str
    object_key: str
    expires_in_s: int


class VideoResponse(BaseModel):
    video_id: UUID
    filename: str
    status: str
    size_bytes: int
    duration_s: float | None = None
    width: int | None = None
    height: int | None = None
    expected_renditions: list[str] | None = None
    failure_reason: str | None = None
    created_at: datetime


class RenditionSnapshot(BaseModel):
    rendition: str
    status: str | None = None
    object_key: str | None = None
    failure_reason: str | None = None
    completed_at: datetime | None = None


class VideoSnapshot(BaseModel):
    """The first SSE event on a fresh connect (ADR-0008): a client connecting
    after some renditions already finished must see them here, not wait for a
    live rendition.completed that will never come again for a finished one."""

    video: VideoResponse
    renditions: list[RenditionSnapshot]

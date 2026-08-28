"""sse_event_name's payload-shape dispatch (ADR-0008, extended Phase 11)."""

from __future__ import annotations

from pipeline.models import EventRow

from services.api.sse import sse_event_name


def _row(type_: str, **payload: object) -> EventRow:
    return EventRow(type=type_, payload=payload)


def test_a_rendition_scoped_failure_gets_its_own_event_name() -> None:
    row = _row("pipeline.failed", rendition="720p", reason="ffmpeg failed")
    assert sse_event_name(row) == "rendition.failed"


def test_a_video_level_failure_with_no_rendition_stays_plain_failed() -> None:
    """worker_probe rejecting a corrupt source fails before any
    rendition-specific work starts — genuinely no rendition to name."""
    row = _row("pipeline.failed", rendition=None, reason="unparseable input")
    assert sse_event_name(row) == "failed"


def test_rendition_completed_is_unaffected() -> None:
    row = _row("video.status", rendition="720p", rendition_object_key="videos/x/720p.mp4")
    assert sse_event_name(row) == "rendition.completed"

"""The contract tests that substitute for a schema registry (ADR-0003)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pipeline import events


def test_round_trip_preserves_every_field() -> None:
    original = events.RenditionCompleted(
        video_id=uuid4(),
        owner_id="user|test",
        producer="worker-transcode",
        rendition="720p",
        object_key="videos/x/renditions/720p.mp4",
        size_bytes=1234,
        transcode_seconds=42.5,
    )

    restored = events.parse(original.serialize())

    assert restored == original
    assert isinstance(restored, events.RenditionCompleted)


def test_consumer_ignores_fields_it_does_not_know() -> None:
    """A new producer must be deployable before its consumers (ADR-0003)."""
    payload = json.loads(
        events.VideoUploaded(
            video_id=uuid4(),
            owner_id="user|test",
            producer="api",
            object_key="videos/x/source.mp4",
            filename="holiday.mp4",
            size_bytes=99,
            content_type="video/mp4",
        ).serialize()
    )
    payload["hdr_metadata"] = {"added": "in a later release"}

    restored = events.parse(json.dumps(payload))

    assert isinstance(restored, events.VideoUploaded)
    assert not hasattr(restored, "hdr_metadata")


def test_unknown_type_is_poison_not_a_retry() -> None:
    with pytest.raises(events.UnknownEventType):
        events.parse(json.dumps({"type": "video.teleported", "video_id": str(uuid4())}))


def test_parse_as_rejects_the_wrong_event_on_a_topic() -> None:
    raw = events.VideoUploaded(
        video_id=uuid4(),
        owner_id="user|test",
        producer="api",
        object_key="k",
        filename="f.mp4",
        size_bytes=1,
        content_type="video/mp4",
    ).serialize()

    with pytest.raises(events.UnknownEventType):
        events.parse_as(raw, events.RenditionCompleted)


def test_key_is_video_id_so_ordering_holds() -> None:
    video_id = uuid4()
    event = events.VideoStatusChanged(
        video_id=video_id, owner_id="user|test", producer="api", state=events.VideoState.UPLOADED
    )
    assert event.key == str(video_id).encode()


def test_every_registered_type_matches_its_class_literal() -> None:
    for declared_type, cls in events.EVENT_TYPES.items():
        assert cls.model_fields["type"].default == declared_type


def test_defaults_are_populated() -> None:
    event = events.VideoStatusChanged(
        video_id=uuid4(), owner_id="user|test", producer="api", state=events.VideoState.PROBED
    )
    assert event.schema_version == events.SCHEMA_VERSION
    assert event.occurred_at.tzinfo is not None
    assert (datetime.now(UTC) - event.occurred_at).total_seconds() < 5


@pytest.mark.parametrize("bad", ["720", "p720", "", "72op"])
def test_rendition_shape_is_validated(bad: str) -> None:
    with pytest.raises(ValueError, match="rendition"):
        events.RenditionRequested(
            video_id=uuid4(),
            owner_id="user|test",
            producer="probe",
            rendition=bad,
            source_key="s",
            target_key="t",
        )


def test_events_are_immutable() -> None:
    event = events.VideoStatusChanged(
        video_id=uuid4(), owner_id="user|test", producer="api", state=events.VideoState.UPLOADED
    )
    with pytest.raises(ValueError, match="frozen"):
        event.state = events.VideoState.FAILED  # type: ignore[misc]

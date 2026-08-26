"""Keys are built in one place so a stray f-string cannot orphan an object."""

from __future__ import annotations

from uuid import UUID

from pipeline import storage

VIDEO = UUID("11111111-2222-3333-4444-555555555555")


def test_every_object_for_a_video_shares_its_prefix() -> None:
    prefix = storage.video_prefix(VIDEO)
    for key in (
        storage.source_key(VIDEO, "mp4"),
        storage.rendition_key(VIDEO, "720p"),
        storage.hls_playlist_key(VIDEO, "720p"),
        storage.hls_master_key(VIDEO),
        storage.poster_key(VIDEO),
        storage.sprite_key(VIDEO),
        storage.sprite_vtt_key(VIDEO),
    ):
        assert key.startswith(prefix), key


def test_scratch_lives_under_the_only_prefix_the_lifecycle_expires() -> None:
    """tmp/ is the one prefix the bucket rule deletes — see ADR-0001 and 0006."""
    key = storage.scratch_key(VIDEO, "720p.part")
    assert key.startswith("tmp/")
    assert not key.startswith(storage.video_prefix(VIDEO))


def test_source_extension_is_normalised() -> None:
    assert storage.source_key(VIDEO, ".mp4") == storage.source_key(VIDEO, "mp4")


def test_renditions_do_not_collide() -> None:
    keys = {storage.rendition_key(VIDEO, r) for r in ("360p", "480p", "720p", "1080p")}
    assert len(keys) == 4


def test_key_builders_need_no_credentials() -> None:
    """Importing storage for key building must not require an S3 client."""
    assert storage.rendition_key(VIDEO, "360p").endswith("renditions/360p.mp4")

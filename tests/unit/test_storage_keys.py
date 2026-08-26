"""Keys are built in one place so a stray f-string cannot orphan an object."""

from __future__ import annotations

from uuid import UUID

from pipeline import storage

VIDEO = UUID("11111111-2222-3333-4444-555555555555")
OWNER = "auth0|abc123"
OTHER = "auth0|intruder"


def every_key_for(owner: str) -> list[str]:
    return [
        storage.source_key(owner, VIDEO, "mp4"),
        storage.rendition_key(owner, VIDEO, "720p"),
        storage.hls_playlist_key(owner, VIDEO, "720p"),
        storage.hls_master_key(owner, VIDEO),
        storage.poster_key(owner, VIDEO),
        storage.sprite_key(owner, VIDEO),
        storage.sprite_vtt_key(owner, VIDEO),
    ]


def test_every_object_for_a_video_shares_its_prefix() -> None:
    prefix = storage.video_prefix(OWNER, VIDEO)
    for key in every_key_for(OWNER):
        assert key.startswith(prefix), key


def test_every_key_lives_under_its_owner():
    """The owner prefix is the tenancy boundary (ADR-0016)."""
    for key in every_key_for(OWNER):
        assert key.startswith(storage.owner_prefix(OWNER)), key
        assert storage.owns(OWNER, key)
        assert not storage.owns(OTHER, key)


def test_two_owners_never_share_a_key() -> None:
    assert set(every_key_for(OWNER)).isdisjoint(every_key_for(OTHER))


def test_a_prefix_is_not_a_substring_match() -> None:
    """users/alice must not own users/alice-evil/..."""
    assert not storage.owns("alice", storage.source_key("alice-evil", VIDEO, "mp4"))


def test_scratch_lives_under_the_only_prefix_the_lifecycle_expires() -> None:
    """tmp/ is the one prefix the bucket rule deletes — see ADR-0001 and 0006."""
    key = storage.scratch_key(OWNER, VIDEO, "720p.part")
    assert key.startswith("tmp/")
    assert not key.startswith(storage.video_prefix(OWNER, VIDEO))
    # Still owned, so the same authorization check works for scratch objects.
    assert storage.owns(OWNER, key)
    assert not storage.owns(OTHER, key)


def test_source_extension_is_normalised() -> None:
    assert storage.source_key(OWNER, VIDEO, ".mp4") == storage.source_key(OWNER, VIDEO, "mp4")


def test_renditions_do_not_collide() -> None:
    keys = {storage.rendition_key(OWNER, VIDEO, r) for r in ("360p", "480p", "720p", "1080p")}
    assert len(keys) == 4


def test_key_builders_need_no_credentials() -> None:
    """Importing storage for key building must not require an S3 client."""
    assert storage.rendition_key(OWNER, VIDEO, "360p").endswith("renditions/360p.mp4")

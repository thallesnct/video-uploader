"""The player asset proxy (`GET /videos/{id}/media/{path}`), Phase 9.

hls.js resolves every relative reference inside a playlist against the URL it
fetched that playlist from, so serving HLS content from presigned S3 URLs
doesn't work past the first request (the signature doesn't survive a relative
fetch). This route is the fix: one stable, authenticated origin for the whole
tree.
"""

from __future__ import annotations

import uuid
from typing import Any

from pipeline.storage import object_store

OWNER = "user|media"


def _create_video(client: Any, auth: Any, owner: str = OWNER) -> str:
    resp = client.post(
        "/videos",
        json={"filename": "clip.mp4", "content_type": "video/mp4", "size_bytes": 256},
        headers=auth(owner),
    )
    return str(resp.json()["video_id"])


def test_the_master_playlist_is_served_with_the_right_content_type(
    environment: None, client: Any, auth: Any
) -> None:
    video_id = _create_video(client, auth)
    store = object_store()
    key = f"users/{OWNER}/videos/{video_id}/hls/master.m3u8"
    store.client.put_object(Bucket=store.bucket, Key=key, Body=b"#EXTM3U\n#EXT-X-ENDLIST\n")

    resp = client.get(f"/videos/{video_id}/media/hls/master.m3u8", headers=auth(OWNER))

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert resp.content == b"#EXTM3U\n#EXT-X-ENDLIST\n"


def test_a_rendition_playlist_and_a_segment_are_both_served(
    environment: None, client: Any, auth: Any
) -> None:
    video_id = _create_video(client, auth)
    store = object_store()
    prefix = f"users/{OWNER}/videos/{video_id}/hls/360p"
    store.client.put_object(
        Bucket=store.bucket, Key=f"{prefix}/playlist.m3u8", Body=b"#EXTM3U\nseg000.ts\n"
    )
    store.client.put_object(Bucket=store.bucket, Key=f"{prefix}/seg000.ts", Body=b"\x00" * 32)

    playlist_resp = client.get(
        f"/videos/{video_id}/media/hls/360p/playlist.m3u8", headers=auth(OWNER)
    )
    segment_resp = client.get(f"/videos/{video_id}/media/hls/360p/seg000.ts", headers=auth(OWNER))

    assert playlist_resp.status_code == 200
    assert segment_resp.status_code == 200
    assert segment_resp.headers["content-type"].startswith("video/mp2t")
    assert segment_resp.content == b"\x00" * 32


def test_a_missing_asset_is_not_found(environment: None, client: Any, auth: Any) -> None:
    video_id = _create_video(client, auth)

    resp = client.get(f"/videos/{video_id}/media/hls/master.m3u8", headers=auth(OWNER))

    assert resp.status_code == 404


def test_another_tenants_video_media_is_not_found(
    environment: None, client: Any, auth: Any
) -> None:
    video_id = _create_video(client, auth)
    store = object_store()
    key = f"users/{OWNER}/videos/{video_id}/hls/master.m3u8"
    store.client.put_object(Bucket=store.bucket, Key=key, Body=b"#EXTM3U\n")

    resp = client.get(f"/videos/{video_id}/media/hls/master.m3u8", headers=auth("user|other"))

    assert resp.status_code == 404


def test_an_unknown_video_id_is_not_found(environment: None, client: Any, auth: Any) -> None:
    resp = client.get(f"/videos/{uuid.uuid4()}/media/hls/master.m3u8", headers=auth(OWNER))

    assert resp.status_code == 404


def test_a_path_traversal_attempt_is_not_found(environment: None, client: Any, auth: Any) -> None:
    """S3 keys have no directory-traversal semantics (storage.media_key's own
    docstring), so this can only ever miss — asserted directly anyway, since
    it's the one request shape worth being explicit about."""
    video_id = _create_video(client, auth)

    resp = client.get(f"/videos/{video_id}/media/../../../etc/passwd", headers=auth(OWNER))

    assert resp.status_code == 404

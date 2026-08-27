"""The upload path against real Kafka, Postgres and MinIO (Phase 3 gate)."""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest
import requests
from pipeline.topics import VIDEO_UPLOADED

ALICE = "user|alice"
BOB = "user|bob"

A_VIDEO = {"filename": "holiday.mp4", "content_type": "video/mp4", "size_bytes": 1024}


def drain(bootstrap: str, topic: str, seconds: float = 10.0) -> list[dict[str, Any]]:
    """Read everything currently on a topic from the beginning."""
    import json

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages: list[dict[str, Any]] = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        messages.append(
            {
                "key": message.key().decode() if message.key() else None,
                "value": json.loads(message.value()),
                "headers": dict(message.headers() or []),
            }
        )
    consumer.close()
    return messages


def upload(client: Any, headers: dict[str, str], body: dict[str, Any] | None = None) -> dict:
    created = client.post("/videos", json=body or A_VIDEO, headers=headers)
    assert created.status_code == 201, created.text
    payload = created.json()
    put = requests.put(
        payload["upload_url"],
        data=b"\x00" * 1024,
        headers={"Content-Type": (body or A_VIDEO)["content_type"]},
        timeout=30,
    )
    assert put.status_code == 200, put.text
    return dict(payload)


# ------------------------------------------------------------------ happy path


def test_upload_then_complete_emits_exactly_one_event(
    client: Any, auth: Any, kafka_bootstrap: str
) -> None:
    headers = auth(ALICE)
    created = upload(client, headers)
    video_id = created["video_id"]

    completed = client.post(f"/videos/{video_id}/complete", headers=headers)
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "uploaded"

    published = [
        message for message in drain(kafka_bootstrap, VIDEO_UPLOADED) if message["key"] == video_id
    ]
    assert len(published) == 1
    assert published[0]["value"]["object_key"] == created["object_key"]
    # Per-video ordering depends on the key being the video id (ADR-0002).
    assert published[0]["key"] == video_id
    # One trace across every stage depends on this header (ADR-0010).
    assert "traceparent" in published[0]["headers"]


def test_completing_twice_does_not_publish_twice(
    client: Any, auth: Any, kafka_bootstrap: str
) -> None:
    """The claim, not a read-then-write: a retried /complete is a no-op."""
    headers = auth(ALICE)
    video_id = upload(client, headers)["video_id"]

    first = client.post(f"/videos/{video_id}/complete", headers=headers)
    second = client.post(f"/videos/{video_id}/complete", headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200, "a repeated complete should be idempotent"

    published = [
        message for message in drain(kafka_bootstrap, VIDEO_UPLOADED) if message["key"] == video_id
    ]
    assert len(published) == 1, f"published {len(published)} events for one upload"


def test_complete_without_an_uploaded_object_is_rejected(client: Any, auth: Any) -> None:
    headers = auth(ALICE)
    created = client.post("/videos", json=A_VIDEO, headers=headers).json()

    response = client.post(f"/videos/{created['video_id']}/complete", headers=headers)

    assert response.status_code == 409


# ----------------------------------------------------------------- authentication


def test_a_token_signed_by_another_key_is_rejected(client: Any) -> None:
    """The authentication test, not an authorization one: this proves the JWKS
    signature check actually verifies rather than merely decodes."""
    import jwt
    from cryptography.hazmat.primitives.asymmetric import rsa

    attacker_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = jwt.encode(
        {
            "sub": ALICE,
            "iss": "http://devauth:8080",
            "aud": "video-pipeline",
            "iat": int(time.time()),
            "exp": int(time.time()) + 3600,
        },
        attacker_key,
        algorithm="RS256",
        headers={"kid": "dev-key-1"},
    )

    response = client.get("/videos", headers={"Authorization": f"Bearer {forged}"})

    assert response.status_code == 401


def test_a_token_for_another_audience_is_rejected(client: Any, mint: Any) -> None:
    token = mint(ALICE, audience="some-other-service")
    response = client.get("/videos", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


def test_an_expired_token_is_rejected(client: Any, mint: Any) -> None:
    token = mint(ALICE, expires_in=-60)
    response = client.get("/videos", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


@pytest.mark.parametrize("header", [None, "", "Basic abc", "Bearer", "not-a-token"])
def test_malformed_authorization_headers_are_rejected(client: Any, header: str | None) -> None:
    headers = {} if header is None else {"Authorization": header}
    assert client.get("/videos", headers=headers).status_code == 401


# ------------------------------------------------------------------- isolation


def test_object_keys_are_scoped_to_the_caller(client: Any, auth: Any) -> None:
    alice = client.post("/videos", json=A_VIDEO, headers=auth(ALICE)).json()
    bob = client.post("/videos", json=A_VIDEO, headers=auth(BOB)).json()

    assert alice["object_key"].startswith("users/user|alice/")
    assert bob["object_key"].startswith("users/user|bob/")


def test_one_tenant_cannot_read_anothers_video(client: Any, auth: Any) -> None:
    video_id = upload(client, auth(ALICE))["video_id"]

    response = client.get(f"/videos/{video_id}", headers=auth(BOB))

    # 404 rather than 403: a 403 would confirm the id exists.
    assert response.status_code == 404


def test_one_tenant_cannot_complete_anothers_upload(client: Any, auth: Any) -> None:
    video_id = upload(client, auth(ALICE))["video_id"]

    response = client.post(f"/videos/{video_id}/complete", headers=auth(BOB))

    assert response.status_code == 404


def test_listing_only_returns_your_own_videos(client: Any, auth: Any) -> None:
    carol, dave = f"user|{uuid.uuid4()}", f"user|{uuid.uuid4()}"
    client.post("/videos", json=A_VIDEO, headers=auth(carol))
    client.post("/videos", json=A_VIDEO, headers=auth(dave))

    listed = client.get("/videos", headers=auth(carol)).json()

    assert len(listed) == 1


def test_a_presigned_url_cannot_be_pointed_at_another_tenants_key(client: Any, auth: Any) -> None:
    """The signature pins the key, so rewriting the path invalidates it."""
    alice = client.post("/videos", json=A_VIDEO, headers=auth(ALICE)).json()
    tampered = alice["upload_url"].replace("users/user%7Calice", "users/user%7Cbob")

    response = requests.put(tampered, data=b"x", timeout=30)

    assert response.status_code in (400, 403)


# ---------------------------------------------------------------------- quotas


def test_an_oversized_declared_upload_is_refused(client: Any, auth: Any) -> None:
    response = client.post(
        "/videos",
        json={**A_VIDEO, "size_bytes": 50 * 1024 * 1024 * 1024},
        headers=auth(ALICE),
    )
    assert response.status_code == 413


def test_too_many_videos_in_flight_is_refused(client: Any, auth: Any) -> None:
    """One tenant must not be able to fill the transcode partitions (ADR-0016)."""
    from pipeline.settings import quota_settings

    subject = f"user|{uuid.uuid4()}"
    limit = quota_settings().max_videos_in_flight
    for _ in range(limit):
        assert client.post("/videos", json=A_VIDEO, headers=auth(subject)).status_code == 201

    refused = client.post("/videos", json=A_VIDEO, headers=auth(subject))

    assert refused.status_code == 429


# ------------------------------------------------------- expiry / cancellation


def _backdate_created_at(video_id: str, seconds_ago: float) -> None:
    """Force a row's created_at into the past — the presign window defaults
    to 6h, too long to just wait out in a test."""
    from pipeline.db import create_sync_engine
    from sqlalchemy import text

    engine = create_sync_engine()
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE videos SET created_at = now() - make_interval(secs => :s) "
                    "WHERE id = :id"
                ),
                {"s": seconds_ago, "id": video_id},
            )
    finally:
        engine.dispose()


def test_a_stale_awaiting_upload_is_expired_and_freed_from_quota(client: Any, auth: Any) -> None:
    """A presign window that has closed can never be completed — the row
    must not occupy quota forever (ADR-0006 follow-on)."""
    from pipeline.settings import quota_settings, s3_settings

    subject = f"user|{uuid.uuid4()}"
    limit = quota_settings().max_videos_in_flight
    created_ids = [
        client.post("/videos", json=A_VIDEO, headers=auth(subject)).json()["video_id"]
        for _ in range(limit)
    ]
    assert client.post("/videos", json=A_VIDEO, headers=auth(subject)).status_code == 429

    expiry = s3_settings().presign_put_expiry_s
    _backdate_created_at(created_ids[0], expiry + 60)

    # The same request that would have been refused now succeeds: the stale
    # row expires as part of this very call, before quota is even counted.
    response = client.post("/videos", json=A_VIDEO, headers=auth(subject))
    assert response.status_code == 201, response.text

    expired = client.get(f"/videos/{created_ids[0]}", headers=auth(subject)).json()
    assert expired["status"] == "failed"
    assert expired["failure_reason"] == "upload window expired"


def test_cancelling_an_awaiting_upload_video_removes_it(client: Any, auth: Any) -> None:
    headers = auth(ALICE)
    created = client.post("/videos", json=A_VIDEO, headers=headers).json()

    response = client.delete(f"/videos/{created['video_id']}", headers=headers)

    assert response.status_code == 204
    assert client.get(f"/videos/{created['video_id']}", headers=headers).status_code == 404


def test_cancelling_frees_the_quota_slot_immediately(client: Any, auth: Any) -> None:
    from pipeline.settings import quota_settings

    subject = f"user|{uuid.uuid4()}"
    limit = quota_settings().max_videos_in_flight
    ids = [
        client.post("/videos", json=A_VIDEO, headers=auth(subject)).json()["video_id"]
        for _ in range(limit)
    ]
    assert client.post("/videos", json=A_VIDEO, headers=auth(subject)).status_code == 429

    assert client.delete(f"/videos/{ids[0]}", headers=auth(subject)).status_code == 204

    assert client.post("/videos", json=A_VIDEO, headers=auth(subject)).status_code == 201


def test_cancelling_an_uploaded_video_is_rejected(client: Any, auth: Any) -> None:
    """Once /complete has run, a worker may already be acting on it — a
    DB-only cancel cannot stop that, so this scope is a hard boundary."""
    headers = auth(ALICE)
    created = upload(client, headers)
    completed = client.post(f"/videos/{created['video_id']}/complete", headers=headers)
    assert completed.status_code == 200

    response = client.delete(f"/videos/{created['video_id']}", headers=headers)

    assert response.status_code == 409
    assert client.get(f"/videos/{created['video_id']}", headers=headers).status_code == 200


def test_cancelling_someone_elses_video_is_not_found(client: Any, auth: Any) -> None:
    alices = client.post("/videos", json=A_VIDEO, headers=auth(ALICE)).json()

    response = client.delete(f"/videos/{alices['video_id']}", headers=auth(BOB))

    assert response.status_code == 404
    assert client.get(f"/videos/{alices['video_id']}", headers=auth(ALICE)).status_code == 200


def test_cancelling_an_unknown_video_is_not_found(client: Any, auth: Any) -> None:
    response = client.delete(f"/videos/{uuid.uuid4()}", headers=auth(ALICE))

    assert response.status_code == 404


def test_an_unsupported_container_is_refused_at_the_door(client: Any, auth: Any) -> None:
    response = client.post(
        "/videos",
        json={**A_VIDEO, "content_type": "application/zip"},
        headers=auth(ALICE),
    )
    assert response.status_code == 422


def test_a_filename_with_path_separators_is_refused(client: Any, auth: Any) -> None:
    response = client.post(
        "/videos", json={**A_VIDEO, "filename": "../../etc/passwd"}, headers=auth(ALICE)
    )
    assert response.status_code == 422

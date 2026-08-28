"""The webhook receiver test double (Phase 11) needs its own contract
verified — every other test that relies on it (worker_notify's integration
tests, the failure-UX e2e spec) trusts "POST stores it, GET lists it, POST
/reset clears it" without re-checking, so it has to actually be true."""

from __future__ import annotations

from fastapi.testclient import TestClient

from services.webhook_sink.main import app

client = TestClient(app)


def test_a_posted_webhook_is_recorded_and_listed() -> None:
    client.post("/reset")

    resp = client.post("/webhook", json={"event_id": "abc", "type": "video.completed"})

    assert resp.status_code == 200
    received = client.get("/received").json()
    assert received == [{"event_id": "abc", "type": "video.completed"}]


def test_multiple_webhooks_are_recorded_in_arrival_order() -> None:
    client.post("/reset")

    client.post("/webhook", json={"event_id": "1"})
    client.post("/webhook", json={"event_id": "2"})

    received = client.get("/received").json()
    assert [r["event_id"] for r in received] == ["1", "2"]


def test_reset_clears_prior_webhooks() -> None:
    client.post("/webhook", json={"event_id": "stale"})

    client.post("/reset")

    assert client.get("/received").json() == []

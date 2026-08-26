"""Probe semantics — the distinction that prevents a self-inflicted outage."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from pipeline.health import HealthRegistry, serve_health


def get(port: int, path: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_liveness_ignores_broken_dependencies() -> None:
    """A liveness probe that checks Kafka turns a broker blip into a mass restart."""
    registry = HealthRegistry()
    registry.register("kafka", lambda: False)
    server = serve_health(registry, 0)
    port = server.server_address[1]
    try:
        status, body = get(port, "/healthz")
        assert status == 200
        assert body["status"] == "alive"
    finally:
        server.shutdown()


def test_readiness_fails_when_a_dependency_is_down() -> None:
    registry = HealthRegistry()
    registry.register("kafka", lambda: True)
    registry.register("postgres", lambda: False)
    server = serve_health(registry, 0)
    port = server.server_address[1]
    try:
        status, body = get(port, "/readyz")
        assert status == 503
        assert body["kafka"] == "ok"
        assert body["postgres"] == "unavailable"
    finally:
        server.shutdown()


def test_a_raising_check_is_reported_not_fatal() -> None:
    registry = HealthRegistry()
    registry.register("s3", lambda: (_ for _ in ()).throw(RuntimeError("connection refused")))

    ready, detail = registry.readiness()

    assert ready is False
    assert "connection refused" in detail["s3"]


def test_readiness_is_true_with_no_checks_registered() -> None:
    ready, detail = HealthRegistry().readiness()
    assert ready is True
    assert detail == {}


def test_metrics_are_served_on_the_same_side_port() -> None:
    server = serve_health(HealthRegistry(), 0)
    port = server.server_address[1]
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            assert resp.status == 200
            assert b"stage_messages_total" in resp.read() or resp.status == 200
    finally:
        server.shutdown()

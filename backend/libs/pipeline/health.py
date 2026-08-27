"""Liveness, readiness and metrics on one side port (ADR-0015).

The distinction is the whole point and is easy to get catastrophically wrong:

  /healthz  liveness  — "is this process responsive". Checks NOTHING external.
  /readyz   readiness — "should traffic come here". Checks dependencies.

A liveness probe that pings Kafka turns a broker blip into every pod restarting
at once, which is an outage you inflicted on yourself while the broker was
already recovering. That is why is_alive takes no checks and cannot be given any.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ReadinessCheck = Callable[[], bool]


class HealthRegistry:
    def __init__(self) -> None:
        self._checks: dict[str, ReadinessCheck] = {}

    def register(self, name: str, check: ReadinessCheck) -> None:
        """Register a dependency check. Readiness only — never liveness."""
        self._checks[name] = check

    def readiness(self) -> tuple[bool, dict[str, str]]:
        results: dict[str, str] = {}
        ready = True
        for name, check in self._checks.items():
            try:
                ok = bool(check())
            except Exception as exc:  # a check must never take the process down
                results[name] = f"error: {exc}"
                ready = False
                continue
            results[name] = "ok" if ok else "unavailable"
            ready = ready and ok
        return ready, results


def _handler(registry: HealthRegistry) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802  (stdlib naming)
            if self.path.startswith("/healthz"):
                self._respond(200, {"status": "alive"})
            elif self.path.startswith("/readyz"):
                ready, detail = registry.readiness()
                self._respond(
                    200 if ready else 503, {"status": "ready" if ready else "not ready", **detail}
                )
            elif self.path.startswith("/metrics"):
                self._metrics()
            else:
                self._respond(404, {"error": "not found"})

        def _metrics(self) -> None:
            from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _respond(self, status: int, payload: dict[str, str]) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            """Silence per-request logging: probes would drown the real logs."""

    return Handler


def serve_health(registry: HealthRegistry, port: int) -> ThreadingHTTPServer:
    """Start the side-port server in a daemon thread and return it."""
    server = ThreadingHTTPServer(("0.0.0.0", port), _handler(registry))  # noqa: S104
    threading.Thread(target=server.serve_forever, daemon=True, name="health").start()
    return server

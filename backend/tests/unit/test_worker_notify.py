"""worker_notify's webhook classification: a rejected payload (4xx) will be
rejected identically forever, so it must dead-letter; the receiver's own
outage (5xx) or unreachability probably resolves, so it must retry
(ADR-0005's transient/terminal split, applied to an HTTP call instead of
ffmpeg)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from pipeline.retry import TerminalError, TransientError

from services.worker_notify.main import _post_webhook


@contextmanager
def _server_responding(status: int) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(length)
            self.send_response(status)
            self.end_headers()

        def log_message(self, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/webhook"
    finally:
        server.shutdown()
        thread.join()


def test_a_2xx_response_is_success() -> None:
    with _server_responding(200) as url:
        _post_webhook(url, {"event_id": "1"}, timeout_s=2)


def test_a_5xx_response_is_transient() -> None:
    with _server_responding(503) as url, pytest.raises(TransientError):
        _post_webhook(url, {"event_id": "1"}, timeout_s=2)


def test_a_4xx_response_is_terminal() -> None:
    with _server_responding(422) as url, pytest.raises(TerminalError):
        _post_webhook(url, {"event_id": "1"}, timeout_s=2)


def test_an_unreachable_target_is_transient() -> None:
    # Nothing listens here — connection refused, not a timeout, but the
    # same "probably resolves" classification applies.
    with pytest.raises(TransientError):
        _post_webhook("http://127.0.0.1:1/webhook", {"event_id": "1"}, timeout_s=2)

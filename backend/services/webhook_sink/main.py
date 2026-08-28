"""A webhook receiver for tests only (Phase 11).

worker_notify's whole job is firing an HTTP POST at an external system this
project has no control over — "the webhook fires" is not something a unit
test can observe on its own. This is that external system, standing in: it
remembers every payload it was ever POSTed to, so a test (or a human) can ask
"did the webhook fire, and with what" after the fact.

NOT for production, same framing as devauth: dev/test-only, never in a
hardened/prod compose profile.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

app = FastAPI(title="webhook sink (test double)", docs_url="/docs")

# In-memory, one process, no persistence across a restart — this is a test
# double's whole state, not a system of record. Order is arrival order,
# which is what a test polling "did my webhook show up yet" actually wants.
_received: list[dict[str, Any]] = []


@app.post("/webhook")
async def webhook(payload: dict[str, Any]) -> dict[str, str]:
    _received.append(payload)
    return {"status": "received"}


@app.get("/received")
async def received() -> list[dict[str, Any]]:
    return _received


@app.post("/reset")
async def reset() -> dict[str, str]:
    """Test isolation: each test that cares about "did my webhook fire"
    needs a clean slate, not every webhook every other test ever sent."""
    _received.clear()
    return {"status": "reset"}


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "alive"}

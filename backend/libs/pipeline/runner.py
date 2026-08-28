"""Wires a StageWorker's SIGTERM path (ADR-0015 §4) — the one piece every
worker's main() was still missing.

StageWorker.stop() (consumer.py) has existed since Phase 2, its own docstring
already says "SIGTERM path" — but nothing ever called signal.signal() to
actually connect a real SIGTERM to it. Every worker today only stops via
`restart: unless-stopped` killing the process outright: no offset flush, no
clean abort, no closed producer — safe only because every consumer is
idempotent (ADR-0005), not because shutdown is actually graceful the way
ADR-0015 specifies (stop consuming, finish or cleanly abort in-flight work,
commit offsets, flush the producer, close).

Every worker's main() already hand-rolled the same finally-block after
worker.run() (producer.flush(); consumer.close(); sometimes engine.dispose())
— this centralizes it so the signal wiring lands once, not copied into eight
services (AGENTS.md: "new cross-service logic goes here, not copied between
services").
"""

from __future__ import annotations

import logging
import signal
from typing import Any

from sqlalchemy.engine import Engine

from pipeline.consumer import StageWorker
from pipeline.producer import EventProducer

log = logging.getLogger(__name__)


def run_worker(
    worker: StageWorker,
    *,
    producer: EventProducer,
    consumer: Any,
    engine: Engine | None = None,
) -> None:
    """Call after worker.subscribe(). Installs a SIGTERM handler that calls
    worker.stop() (finish the in-flight message, exit the poll loop cleanly),
    runs the worker until stopped, then flushes/closes in the same order
    every service's main() already used.

    signal.signal() only works from the main thread — true for every
    caller here, each worker's main() is the container's entrypoint, nothing
    else competes for SIGTERM in that process.
    """

    def _on_sigterm(signum: int, _frame: object) -> None:
        log.info("received signal %s, finishing in-flight work and stopping", signum)
        worker.stop()

    signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        worker.run()
    finally:
        producer.flush()
        consumer.close()
        if engine is not None:
            engine.dispose()

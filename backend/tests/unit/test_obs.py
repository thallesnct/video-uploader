"""stage_in_flight_seconds used to be a dead metric — observe_stage's finally
block set it to 0 unconditionally on every exit, so it never reflected "age of
the oldest in-flight message" while one was actually running (the ADR-0004
early warning ADR-0010 names explicitly). Fixed with a scrape-time custom
collector instead of a plain Gauge; this proves the fix, not just that the
code runs."""

from __future__ import annotations

import pathlib
import time

import pytest
from pipeline.obs import observe_stage, render_metrics
from prometheus_client import REGISTRY


def _in_flight_seconds(stage: str) -> float | None:
    for family in REGISTRY.collect():
        if family.name != "stage_in_flight_seconds":
            continue
        for sample in family.samples:
            if sample.labels.get("stage") == stage:
                return sample.value
    return None


def test_stage_in_flight_seconds_reflects_real_elapsed_time_while_running() -> None:
    stage = "test-in-flight-stage"
    assert _in_flight_seconds(stage) is None

    with observe_stage(stage):
        time.sleep(0.05)
        value = _in_flight_seconds(stage)
        assert value is not None
        assert value >= 0.05

    # Gone entirely once the message finishes, not reset to 0 and left around
    # forever as a stale series for a stage that isn't running.
    assert _in_flight_seconds(stage) is None


def test_stage_in_flight_seconds_clears_even_on_a_failed_handler() -> None:
    stage = "test-in-flight-stage-error"
    with pytest.raises(RuntimeError), observe_stage(stage):
        raise RuntimeError("boom")

    assert _in_flight_seconds(stage) is None


# render_metrics() (Phase 12, ADR-0014's own noted gotcha): every service
# today runs one process, so the default-registry path is what's exercised
# in production right now — this proves it still works, and that the
# multiprocess branch (only reachable via PROMETHEUS_MULTIPROC_DIR, which
# nothing sets today) at least runs without error, ahead of ever actually
# needing it. The real end-to-end proof (two live uvicorn workers, one
# .db file per worker, sse_connections_active correctly summed to one
# series) was run by hand against the live stack, recorded in the closing
# commit — a unit test can't fork real uvicorn workers.
def test_render_metrics_default_registry_path() -> None:
    body = render_metrics()
    assert b"sse_connections_active" in body


def test_render_metrics_multiprocess_path_does_not_error(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    body = render_metrics()
    assert isinstance(body, bytes)

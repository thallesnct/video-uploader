"""stage_in_flight_seconds used to be a dead metric — observe_stage's finally
block set it to 0 unconditionally on every exit, so it never reflected "age of
the oldest in-flight message" while one was actually running (the ADR-0004
early warning ADR-0010 names explicitly). Fixed with a scrape-time custom
collector instead of a plain Gauge; this proves the fix, not just that the
code runs."""

from __future__ import annotations

import time

import pytest
from pipeline.obs import observe_stage
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

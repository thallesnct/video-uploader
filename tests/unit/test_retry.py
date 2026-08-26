"""Failure classification is cheap to get wrong in both directions (ADR-0005)."""

from __future__ import annotations

import pytest
from pipeline.events import UnknownEventType
from pipeline.retry import (
    FailureClass,
    RetryPolicy,
    TerminalError,
    TransientError,
    classify,
    source_topic_of,
    tier_delay_seconds,
)

POLICY = RetryPolicy(("10s", "1m", "10m"))


def test_unparseable_messages_are_poison() -> None:
    assert classify(UnknownEventType("nope")) is FailureClass.POISON


def test_explicit_classes_are_honoured() -> None:
    assert classify(TerminalError("corrupt")) is FailureClass.TERMINAL
    assert classify(TransientError("timeout")) is FailureClass.TRANSIENT


def test_unknown_exceptions_are_retried_not_dead_lettered() -> None:
    """A retried bug costs CPU; a dead-lettered network blip costs a video."""
    assert classify(RuntimeError("who knows")) is FailureClass.TRANSIENT


@pytest.mark.parametrize(("tier", "seconds"), [("10s", 10), ("1m", 60), ("10m", 600), ("2h", 7200)])
def test_tier_delays_parse(tier: str, seconds: int) -> None:
    assert tier_delay_seconds(tier) == seconds


@pytest.mark.parametrize("bad", ["10", "m1", "", "10x", "-1s"])
def test_malformed_tiers_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="malformed retry tier"):
        tier_delay_seconds(bad)


def test_tiers_must_increase() -> None:
    """A ladder that shortens its delay would hammer a struggling dependency."""
    with pytest.raises(ValueError, match="increase in delay"):
        RetryPolicy(("1m", "10s"))


def test_empty_policy_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one tier"):
        RetryPolicy(())


def test_transient_failures_walk_the_ladder_then_dead_letter() -> None:
    topic = "rendition.requested"
    assert POLICY.route(topic, FailureClass.TRANSIENT, 0) == f"{topic}.retry.10s"
    assert POLICY.route(topic, FailureClass.TRANSIENT, 1) == f"{topic}.retry.1m"
    assert POLICY.route(topic, FailureClass.TRANSIENT, 2) == f"{topic}.retry.10m"
    assert POLICY.route(topic, FailureClass.TRANSIENT, 3) == f"{topic}.dlq"


@pytest.mark.parametrize("failure", [FailureClass.TERMINAL, FailureClass.POISON])
def test_hopeless_failures_skip_the_ladder(failure: FailureClass) -> None:
    assert POLICY.route("video.uploaded", failure, 0) == "video.uploaded.dlq"


@pytest.mark.parametrize(
    ("topic", "expected"),
    [
        ("rendition.requested", "rendition.requested"),
        ("rendition.requested.retry.10s", "rendition.requested"),
        ("rendition.requested.retry.10m", "rendition.requested"),
        ("video.status", "video.status"),
    ],
)
def test_source_topic_is_recovered_from_a_retry_topic(topic: str, expected: str) -> None:
    assert source_topic_of(topic) == expected


def test_dlq_is_recognised() -> None:
    assert POLICY.is_dlq("video.uploaded.dlq")
    assert not POLICY.is_dlq("video.uploaded.retry.1m")

"""Failure classification and non-blocking retry routing (ADR-0005).

Retrying in place with sleep() would block the partition and re-trigger the
eviction loop of ADR-0004, so a delayed message moves to its own topic and a
pump republishes it when the delay has elapsed.

Classification is explicit and pure, which makes it the most testable logic in
the system — and getting it wrong is expensive in both directions: retrying a
corrupt file burns CPU forever, while dead-lettering a network blip loses a
video that would have succeeded on the next attempt.
"""

from __future__ import annotations

import re
from enum import StrEnum

from pipeline.events import UnknownEventType

_TIER_PATTERN = re.compile(r"^(\d+)(s|m|h)$")
_TIER_UNITS = {"s": 1, "m": 60, "h": 3600}


class FailureClass(StrEnum):
    TRANSIENT = "transient"  # will probably succeed later — retry
    TERMINAL = "terminal"  # will fail identically forever — dead-letter
    POISON = "poison"  # cannot even be parsed — dead-letter, never retry


class TransientError(Exception):
    """Raise when the operation is expected to succeed on a later attempt."""


class TerminalError(Exception):
    """Raise when retrying cannot possibly help (corrupt input, bad codec)."""


def classify(exc: BaseException) -> FailureClass:
    """Decide what to do with a failed message.

    Unknown exceptions are treated as transient. They are usually bugs, and a
    bug retried three times costs a little CPU; but a genuine network error
    dead-lettered on the first attempt costs a video. The retry ladder is finite,
    so a real bug still reaches the DLQ - just three attempts later.
    """
    if isinstance(exc, UnknownEventType):
        return FailureClass.POISON
    if isinstance(exc, TerminalError):
        return FailureClass.TERMINAL
    if isinstance(exc, TransientError):
        return FailureClass.TRANSIENT
    return FailureClass.TRANSIENT


def tier_delay_seconds(tier: str) -> int:
    match = _TIER_PATTERN.match(tier)
    if not match:
        raise ValueError(f"malformed retry tier {tier!r}; expected forms like '10s', '1m'")
    return int(match.group(1)) * _TIER_UNITS[match.group(2)]


class RetryPolicy:
    """Where a failed message goes next."""

    def __init__(self, tiers: tuple[str, ...]) -> None:
        if not tiers:
            raise ValueError("a retry policy needs at least one tier")
        delays = [tier_delay_seconds(tier) for tier in tiers]
        if delays != sorted(delays):
            raise ValueError(f"retry tiers must increase in delay, got {tiers}")
        self.tiers = tiers

    def route(
        self, source_topic: str, failure: FailureClass, retry_count: int, *, retryable: bool = True
    ) -> str | None:
        """The topic a failed message should be produced to, or None.

        source_topic is the ORIGINAL topic, not the retry topic the message was
        consumed from, so the ladder does not compound into names like
        'x.retry.10s.retry.1m'.

        retryable is False for topics like video.status/pipeline.failed, which
        have no timed retry ladder (ADR-0005 follow-on) because their consumers
        only ever perform a cheap idempotent upsert. A TRANSIENT failure there
        returns None: there is nowhere to produce it, and the caller must let
        the failure propagate and crash rather than commit past it — Kafka
        offsets are monotonic, so silently continuing would let a later
        commit skip this message forever. TERMINAL/POISON failures still
        dead-letter regardless — an unparseable message that crashed the
        worker forever would livelock the partition behind it.
        """
        if failure in (FailureClass.TERMINAL, FailureClass.POISON):
            return f"{source_topic}.dlq"
        if not retryable:
            return None
        if retry_count >= len(self.tiers):
            return f"{source_topic}.dlq"
        return f"{source_topic}.retry.{self.tiers[retry_count]}"

    def is_dlq(self, topic: str) -> bool:
        return topic.endswith(".dlq")


def source_topic_of(topic: str) -> str:
    """Strip a retry or DLQ suffix to recover the original topic name.

    Broadened from retry-suffix-only (Phase 5) to also strip `.dlq` (Phase
    11's replay CLI needs "give me the original topic" from a DLQ topic name
    the same way `_route_failure` already needs it from a retry-tier one) —
    safe for every existing caller, since a retry-topic string never also
    ends in `.dlq`.
    """
    topic = re.sub(r"\.retry\.\d+[smh]$", "", topic)
    return re.sub(r"\.dlq$", "", topic)


def tier_of(topic: str) -> str:
    """The tier suffix of a retry-tier topic name (`"10s"` from
    `"video.uploaded.retry.10s"`) — the retry pump's own use, to compute
    how long a given message has left to wait via `tier_delay_seconds`."""
    match = re.search(r"\.retry\.(\d+[smh])$", topic)
    if not match:
        raise ValueError(f"{topic!r} is not a retry-tier topic")
    return match.group(1)

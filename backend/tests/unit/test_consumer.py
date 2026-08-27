"""Tests for the loop that keeps a long transcode from evicting its consumer.

The failure this guards against (ADR-0004) has no error message: the group
rebalances, the job is redelivered, and the pipeline silently loops forever.
"""

from __future__ import annotations

import threading
import time
from uuid import uuid4

import pytest
from pipeline import events
from pipeline.consumer import ConsumerGroupStalled, StageWorker
from pipeline.retry import RetryPolicy, TerminalError, TransientError

from tests.unit.fakes import FakeConsumer, FakeMessage, FakeProducer

POLICY = RetryPolicy(("10s", "1m", "10m"))


def a_message(**overrides: object) -> FakeMessage:
    event = events.RenditionRequested(
        video_id=uuid4(),
        owner_id="user|test",
        producer="probe",
        rendition="720p",
        source_key="videos/x/source.mp4",
        target_key="videos/x/renditions/720p.mp4",
        duration_s=42.0,
    )
    return FakeMessage(value=event.serialize(), **overrides)  # type: ignore[arg-type]


def build(consumer: FakeConsumer, handler, producer: FakeProducer | None = None) -> StageWorker:
    return StageWorker(
        stage="transcode",
        source_topic="rendition.requested",
        consumer=consumer,
        producer=producer or FakeProducer(),  # type: ignore[arg-type]
        handler=handler,
        policy=POLICY,
        poll_timeout=0.01,
    )


def test_partitions_are_paused_before_the_handler_runs() -> None:
    consumer = FakeConsumer([a_message()])
    observed: list[bool] = []

    build(consumer, lambda event, view: observed.append(consumer.paused)).run(max_messages=1)

    assert observed == [True], "handler ran while partitions were still fetching"
    assert consumer.resume_calls, "partitions were never resumed"


def test_poll_keeps_being_called_while_the_handler_works() -> None:
    """This is the whole mechanism: polling during work is what prevents eviction."""
    consumer = FakeConsumer([a_message()])
    polls_at_start: list[int] = []

    def slow_handler(event: events.Event, view: object) -> None:
        polls_at_start.append(consumer.poll_calls)
        deadline = time.monotonic() + 2
        while consumer.poll_calls < polls_at_start[0] + 3 and time.monotonic() < deadline:
            time.sleep(0.005)

    build(consumer, slow_handler).run(max_messages=1)

    assert consumer.poll_calls >= polls_at_start[0] + 3, (
        "the loop stopped polling during the handler — the consumer would be evicted"
    )


def test_resume_uses_the_assignment_captured_at_pause_time() -> None:
    """A rebalance mid-handler changes assignment(); resuming the new set is wrong."""
    consumer = FakeConsumer([a_message()])

    def handler(event: events.Event, view: object) -> None:
        consumer.assignment_value = ["p9"]  # simulate a rebalance

    build(consumer, handler).run(max_messages=1)

    assert consumer.pause_calls[0] == ["p0", "p1"]
    assert consumer.resume_calls[0] == ["p0", "p1"]


def test_offset_is_committed_only_after_success() -> None:
    consumer = FakeConsumer([a_message()])
    build(consumer, lambda event, view: None).run(max_messages=1)

    assert len(consumer.commits) == 1
    assert consumer.events.index("commit") > consumer.events.index("pause")


def test_a_message_arriving_while_paused_is_not_dropped() -> None:
    """A partition assigned mid-handler can still deliver; stash, never discard."""
    consumer = FakeConsumer([a_message()])
    late = a_message()
    handled: list[str] = []

    def handler(event: events.Event, view: object) -> None:
        handled.append("x")
        if len(handled) == 1:
            consumer.queue.append(late)  # arrives during the heartbeat poll

    build(consumer, handler).run(max_messages=2)

    assert len(handled) == 2, "the message delivered during pause was dropped"


def test_transient_failure_goes_to_the_first_retry_tier() -> None:
    consumer = FakeConsumer([a_message()])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TransientError("s3 timed out")

    build(consumer, handler, producer).run(max_messages=1)

    topic, _key, _value, _headers = producer.published[0]
    assert topic == "rendition.requested.retry.10s"
    assert producer.header("retry_count") == "1"
    assert producer.header("original_topic") == "rendition.requested"


def test_terminal_failure_skips_the_ladder_and_dead_letters() -> None:
    consumer = FakeConsumer([a_message()])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TerminalError("unsupported codec")

    build(consumer, handler, producer).run(max_messages=1)

    assert producer.published[0][0] == "rendition.requested.dlq"
    assert producer.header("failure_class") == "terminal"


def test_dlq_emits_pipeline_failed_with_video_and_owner_context() -> None:
    """Phase 5's own checklist: 'emit pipeline.failed on DLQ'. Consumed by the
    projector for the read model's failure_reason and by notify (Phase 11)."""
    consumer = FakeConsumer([a_message()])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TerminalError("unsupported codec")

    build(consumer, handler, producer).run(max_messages=1)

    assert len(producer.typed_published) == 1
    topic, event = producer.typed_published[0]
    assert topic == "pipeline.failed"
    assert isinstance(event, events.PipelineFailed)
    assert event.terminal is True
    assert event.rendition == "720p"
    assert event.owner_id == "user|test"


def test_a_retried_not_yet_dlqd_failure_does_not_emit_pipeline_failed() -> None:
    """A message still walking the retry ladder is not a user-facing failure —
    emitting here would flash 'failed' in the UI for something that will
    probably still succeed."""
    consumer = FakeConsumer([a_message()])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TransientError("s3 timed out")

    build(consumer, handler, producer).run(max_messages=1)

    assert producer.typed_published == []


def test_pipeline_failed_emission_never_crashes_on_poison_messages() -> None:
    """An unparseable message cannot carry a video_id/owner_id, so there is
    nothing to attach to a status event — this must degrade, not raise."""
    consumer = FakeConsumer([FakeMessage(value=b'{"type":"video.teleported"}')])
    producer = FakeProducer()

    handled = build(consumer, lambda event, view: None, producer).run(max_messages=1)

    assert handled == 1
    assert producer.typed_published == []
    assert producer.published[0][0] == "rendition.requested.dlq"


def test_a_worker_that_consumes_pipeline_failed_does_not_re_emit_on_its_own_failure() -> None:
    """ADR-0005 follow-on: the projector subscribes to both video.status and
    pipeline.failed. If it dead-letters a pipeline.failed message and emits a
    fresh one, it will consume that too and fail identically — an unbounded
    self-amplifying loop with no stopping condition. It must dead-letter
    silently instead."""
    failure = events.PipelineFailed(
        video_id=uuid4(),
        owner_id="user|test",
        producer="probe",
        stage="probe",
        reason="original failure",
        retry_count=3,
        terminal=True,
    )
    consumer = FakeConsumer([FakeMessage(value=failure.serialize(), topic="pipeline.failed")])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TerminalError("video_id has no row in videos")

    worker = build(consumer, handler, producer)
    worker.subscribe(topics=["video.status", "pipeline.failed"])
    worker.run(max_messages=1)

    assert producer.published[0][0] == "pipeline.failed.dlq"
    assert producer.typed_published == [], (
        "re-emitting pipeline.failed here would loop forever: the same worker "
        "consumes that topic and would fail on it identically"
    )


def test_retry_ladder_does_not_compound_topic_names() -> None:
    """Consuming from x.retry.10s must route to x.retry.1m, not x.retry.10s.retry.1m."""
    consumer = FakeConsumer(
        [a_message(topic="rendition.requested.retry.10s", headers=[("retry_count", b"1")])]
    )
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TransientError("still failing")

    build(consumer, handler, producer).run(max_messages=1)

    assert producer.published[0][0] == "rendition.requested.retry.1m"


def test_exhausting_the_ladder_dead_letters() -> None:
    consumer = FakeConsumer(
        [a_message(topic="rendition.requested.retry.10m", headers=[("retry_count", b"3")])]
    )
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TransientError("hopeless")

    build(consumer, handler, producer).run(max_messages=1)

    assert producer.published[0][0] == "rendition.requested.dlq"


def test_failed_message_is_produced_and_flushed_before_the_commit() -> None:
    """Committing first would lose the message entirely if the produce failed."""
    ledger: list[str] = []
    consumer = FakeConsumer([a_message()])
    consumer.events = ledger
    producer = FakeProducer(ledger)

    def handler(event: events.Event, view: object) -> None:
        raise TransientError("boom")

    build(consumer, handler, producer).run(max_messages=1)

    assert ledger.index("produce") < ledger.index("commit")
    assert ledger.index("flush") < ledger.index("commit")


def test_unparseable_message_is_poison_and_never_retried() -> None:
    consumer = FakeConsumer([FakeMessage(value=b'{"type":"video.teleported"}')])
    producer = FakeProducer()

    build(consumer, lambda event, view: None, producer).run(max_messages=1)

    assert producer.published[0][0] == "rendition.requested.dlq"
    assert producer.header("failure_class") == "poison"


def test_commit_failure_does_not_crash_the_loop() -> None:
    """A rebalance can revoke the partition mid-work; redelivery is safe."""
    consumer = FakeConsumer([a_message()])
    consumer.commit_error = RuntimeError("no longer owns partition")

    handled = build(consumer, lambda event, view: None).run(max_messages=1)

    assert handled == 1


def test_stop_ends_the_loop_for_graceful_shutdown() -> None:
    consumer = FakeConsumer([])
    worker = build(consumer, lambda event, view: None)
    threading.Timer(0.05, worker.stop).start()

    assert worker.run() == 0


@pytest.mark.parametrize("bad_message", [FakeMessage(b"", error="broker error")])
def test_broker_errors_are_skipped_not_handled(bad_message: FakeMessage) -> None:
    consumer = FakeConsumer([bad_message])
    worker = build(consumer, lambda event, view: pytest.fail("handled an error frame"))
    threading.Timer(0.2, worker.stop).start()

    assert worker.run() == 0


def test_revocation_during_work_is_visible_to_the_handler() -> None:
    """A rebalance mid-transcode means nobody will accept the result."""
    consumer = FakeConsumer([a_message()])
    worker = build(consumer, lambda event, view: None)
    worker.subscribe()

    assert consumer.subscribed == ["rendition.requested"]
    assert not worker.revoked

    consumer.on_revoke(consumer, ["p0"])
    assert worker.revoked


def test_revocation_flag_resets_between_messages() -> None:
    consumer = FakeConsumer([a_message(), a_message()])
    seen: list[bool] = []
    worker = build(consumer, lambda event, view: seen.append(worker.revoked))
    worker.subscribe()
    consumer.on_revoke(consumer, ["p0"])

    worker.run(max_messages=2)

    assert seen == [False, False], "a stale revocation leaked into the next message"


def test_involuntarily_lost_partitions_raise_the_same_flag() -> None:
    """on_lost IS the ADR-0004 eviction: the group decided we were gone."""
    consumer = FakeConsumer([a_message()])
    worker = build(consumer, lambda event, view: None)
    worker.subscribe()

    consumer.on_lost(consumer, ["p0"])

    assert worker.revoked


def test_seconds_unassigned_is_none_before_any_revocation() -> None:
    worker = build(FakeConsumer([]), lambda event, view: None)
    worker.subscribe()

    assert worker.seconds_unassigned() is None


def test_a_reassignment_clears_the_unassigned_clock() -> None:
    consumer = FakeConsumer([])
    worker = build(consumer, lambda event, view: None)
    worker.subscribe()

    consumer.on_revoke(consumer, ["p0"])
    assert worker.seconds_unassigned() is not None

    consumer.on_assign(consumer, ["p0"])
    assert worker.seconds_unassigned() is None


def test_a_brief_unassigned_period_does_not_crash_the_worker() -> None:
    """An ordinary rebalance resolves in seconds — this must not be mistaken
    for the broker-unavailable case ConsumerGroupStalled exists for."""
    consumer = FakeConsumer([a_message()])
    worker = StageWorker(
        stage="transcode",
        source_topic="rendition.requested",
        consumer=consumer,
        producer=FakeProducer(),
        handler=lambda event, view: None,
        policy=POLICY,
        poll_timeout=0.01,
        stall_timeout_s=10.0,
    )
    worker.subscribe()
    consumer.on_revoke(consumer, ["p0"])
    consumer.on_assign(consumer, ["p0"])  # reassigned well within the window

    assert worker.run(max_messages=1) == 1


def test_a_prolonged_unassigned_period_crashes_the_worker() -> None:
    """A rebalance that never lands a new assignment is not a message to
    retry — it is a broker-side stall this process cannot fix by waiting
    longer (found running the real stack). Crashing is the only path to
    recovery, paired with restart: unless-stopped in docker-compose.yml."""
    consumer = FakeConsumer([])
    worker = StageWorker(
        stage="transcode",
        source_topic="rendition.requested",
        consumer=consumer,
        producer=FakeProducer(),
        handler=lambda event, view: None,
        policy=POLICY,
        poll_timeout=0.01,
        stall_timeout_s=0.05,
    )
    worker.subscribe()
    consumer.on_lost(consumer, ["p0"])
    time.sleep(0.1)

    with pytest.raises(ConsumerGroupStalled):
        worker.run(max_messages=1)


def test_both_producers_build_identical_headers() -> None:
    """The sync and async paths must not drift apart on what a message looks like."""
    from pipeline.producer import build_headers

    event = events.VideoUploaded(
        video_id=uuid4(),
        owner_id="user|test",
        producer="api",
        object_key="k",
        filename="f.mp4",
        size_bytes=1,
        content_type="video/mp4",
    )
    names = {key for key, _ in build_headers(event)}

    assert {"event_type", "schema_version"} <= names


async def test_async_producer_refuses_to_publish_before_start() -> None:
    """A producer that was never started must say so, not fail obscurely."""
    from pipeline.producer import AsyncEventProducer

    producer = AsyncEventProducer()
    event = events.VideoStatusChanged(
        video_id=uuid4(), owner_id="user|test", producer="api", state=events.VideoState.UPLOADED
    )
    with pytest.raises(RuntimeError, match="start"):
        await producer.publish("video.status", event)


def test_transient_failure_on_a_non_retryable_topic_crashes_instead_of_skipping() -> None:
    """video.status/pipeline.failed have no retry ladder (ADR-0005 follow-on):
    there is nowhere to produce this message. Kafka offsets are monotonic, so
    silently continuing to poll and later committing a *later* message would
    skip past this one forever. The worker must instead let the failure
    propagate and crash — a restart resumes from the last committed offset,
    which is still before this message, so it is redelivered correctly."""
    consumer = FakeConsumer([a_message(topic="video.status")])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TransientError("db unavailable")

    with pytest.raises(TransientError, match="db unavailable"):
        build(consumer, handler, producer).run(max_messages=1)

    assert producer.published == []
    assert consumer.commits == []


def test_terminal_failure_on_a_non_retryable_topic_still_reaches_a_dlq() -> None:
    """A DLQ always exists regardless of the retries flag — only the timed
    ladder is skipped for a non-retryable topic like video.status."""
    consumer = FakeConsumer([a_message(topic="video.status")])
    producer = FakeProducer()

    def handler(event: events.Event, view: object) -> None:
        raise TerminalError("corrupt status event")

    build(consumer, handler, producer).run(max_messages=1)

    assert producer.published[0][0] == "video.status.dlq"
    assert len(consumer.commits) == 1

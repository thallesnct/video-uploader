"""The consuming side, and the single most important loop in the system.

ADR-0004: a transcode that outlives max.poll.interval.ms (5 minutes by default)
gets its consumer evicted from the group. The offset was never committed, so the
job is redelivered to the next worker, which is evicted the same way. The video
never completes, CPU is pegged, and the only symptom is "the queue is stuck".

The fix implemented here: pause the assigned partitions, run the handler on a
worker thread, and keep calling poll() as a heartbeat. Progress is reported the
whole time, so the group never evicts us, and the offset is committed only after
the work has actually succeeded.

THREAD SAFETY: confluent-kafka's Consumer is not thread-safe. Only the polling
thread may touch it. Handlers receive a message and nothing else — the obvious
"let the handler commit when it finishes" refactor is a data race.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Protocol

from pipeline.events import Event, PipelineFailed, parse
from pipeline.obs import (
    DLQ_MESSAGES,
    RETRY_MESSAGES,
    extract_trace_context,
    observe_stage,
    tracer,
)
from pipeline.producer import EventProducer
from pipeline.retry import FailureClass, RetryPolicy, classify, source_topic_of
from pipeline.settings import kafka_settings
from pipeline.topics import PIPELINE_FAILED, REGISTRY

log = logging.getLogger(__name__)

Handler = Callable[[Event, "MessageView"], None]


class ConsumerGroupStalled(RuntimeError):
    """Raised when a rebalance never lands a new assignment within a generous
    window — see StageWorker._check_not_stalled for why this crashes rather
    than waits indefinitely."""


class ConsumerProtocol(Protocol):
    """The slice of confluent_kafka.Consumer this loop uses.

    Narrow on purpose: confluent-kafka's Consumer satisfies it structurally, and
    a fake satisfies it in unit tests — which is what lets the eviction logic be
    tested without a broker.
    """

    def poll(self, timeout: float) -> Any: ...
    def subscribe(self, topics: list[str], **callbacks: Any) -> None: ...
    def pause(self, partitions: list[Any]) -> None: ...
    def resume(self, partitions: list[Any]) -> None: ...
    def assignment(self) -> list[Any]: ...
    def commit(self, message: Any = None, asynchronous: bool = True) -> Any: ...
    def close(self) -> None: ...


class MessageView:
    """What a handler is allowed to see. Deliberately excludes the consumer."""

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    @property
    def topic(self) -> str:
        return str(self._raw.topic())

    @property
    def headers(self) -> list[tuple[str, bytes]]:
        return list(self._raw.headers() or [])

    @property
    def retry_count(self) -> int:
        for key, value in self.headers:
            if key == "retry_count":
                return int(value.decode())
        return 0

    @property
    def key(self) -> bytes:
        return bytes(self._raw.key() or b"")

    @property
    def value(self) -> bytes:
        return bytes(self._raw.value() or b"")


def consumer_config(group_id: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = kafka_settings()
    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id,
        # Offsets are committed by hand, only after the work succeeded (ADR-0005).
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": settings.auto_offset_reset,
        "max.poll.interval.ms": settings.max_poll_interval_ms,
        "session.timeout.ms": settings.session_timeout_ms,
        "max.poll.records": settings.max_poll_records,
        # Cooperative rebalancing: a scale event must not shuffle partitions that
        # are busy transcoding on unrelated workers.
        "partition.assignment.strategy": "cooperative-sticky",
    }
    config.update(extra or {})
    return config


class StageWorker:
    """Consume one topic, do slow work safely, commit only on success."""

    def __init__(
        self,
        *,
        stage: str,
        source_topic: str,
        consumer: ConsumerProtocol,
        producer: EventProducer,
        handler: Handler,
        policy: RetryPolicy,
        poll_timeout: float = 1.0,
        stall_timeout_s: float | None = None,
    ) -> None:
        self.stage = stage
        self.source_topic = source_topic
        self._consumer = consumer
        self._producer = producer
        self._handler = handler
        self._policy = policy
        self._poll_timeout = poll_timeout
        self._stall_timeout_s = (
            stall_timeout_s
            if stall_timeout_s is not None
            else kafka_settings().consumer_stall_timeout_s
        )
        self._stopped = threading.Event()
        # Set when the group revokes our partitions mid-handler. Long handlers
        # should check it and abandon work that nobody will accept the result of.
        self._revoked = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=stage)
        # Messages that arrived while partitions were paused (possible after a
        # rebalance assigns a new partition mid-handler). Never dropped.
        self._pending: list[Any] = []
        self._subscribed_topics: list[str] = [source_topic]
        # None whenever we currently hold an assignment (or haven't subscribed
        # yet); set to the moment we lost it otherwise. Distinct from
        # `_revoked`, which is a per-message flag a handler checks and the
        # next successful commit clears implicitly — this persists across
        # messages until a real reassignment happens, which is exactly what a
        # stall watchdog needs (ADR-0004 follow-on).
        self._unassigned_since: float | None = None

    def subscribe(self, topics: list[str] | None = None) -> None:
        """Subscribe, wiring the rebalance callbacks.

        A rebalance can still happen during long work — a scale event, or a new
        consumer joining — so handlers need a way to notice that the partition
        they are working for is gone.
        """
        self._subscribed_topics = topics or [self.source_topic]
        self._consumer.subscribe(
            self._subscribed_topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            # on_lost fires when partitions are taken involuntarily — the group
            # decided we were gone. That is the ADR-0004 eviction case itself, so
            # it must raise the same flag as an orderly revocation.
            on_lost=self._on_revoke,
        )

    def _on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        log.info("partitions assigned: %s", partitions)
        self._unassigned_since = None

    def _on_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        log.warning("partitions revoked during processing: %s", partitions)
        self._revoked.set()
        if self._unassigned_since is None:
            self._unassigned_since = time.monotonic()

    @property
    def revoked(self) -> bool:
        """True when our partitions were taken away since this message started."""
        return self._revoked.is_set()

    def seconds_unassigned(self) -> float | None:
        """None while we hold an assignment; otherwise how long we haven't.

        For `/readyz` visibility (ADR-0015: readiness checks dependencies) —
        not the crash decision itself, which only `_check_not_stalled` makes,
        from the poll loop. A momentary rebalance reporting "not ready" for a
        few seconds is normal and expected; this is a diagnostic signal, not
        a liveness check health.py's own docstring warns against building.
        """
        if self._unassigned_since is None:
            return None
        return time.monotonic() - self._unassigned_since

    def _check_not_stalled(self) -> None:
        """Crash if a rebalance has never landed a new assignment within a
        generous window, rather than sit idle forever (found running the real
        stack: a resource-starved single-node Kafka broker can leave every
        consumer group's rejoin failing for minutes, not the seconds a normal
        rebalance takes — and nothing in librdkafka's own retry loop gives up
        and surfaces that as an error; it just keeps trying silently).

        This is the same philosophy ADR-0005 already established for an
        unroutable transient failure: crashing is the only way to guarantee
        recovery here, because there is no in-process action left to take —
        `restart: unless-stopped` (docker-compose.yml) is what actually
        recovers it, by starting a fresh consumer that rejoins cleanly.
        """
        stalled_for = self.seconds_unassigned()
        if stalled_for is not None and stalled_for > self._stall_timeout_s:
            raise ConsumerGroupStalled(
                f"{self.stage}: no partition assignment for {stalled_for:.0f}s "
                f"(threshold {self._stall_timeout_s:.0f}s) — crashing for a clean restart"
            )

    def wait_for_assignment(self, timeout: float = 30.0) -> list[Any]:
        """Poll until the group gives us partitions.

        Useful at startup for an honest "ready" signal, and essential in tests:
        publishing before assignment against `auto.offset.reset=latest` means the
        message lands before anyone is listening and the worker waits forever.

        Anything delivered while waiting is stashed rather than dropped.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            assignment = list(self._consumer.assignment())
            if assignment:
                return assignment
            raw = self._consumer.poll(0.5)
            if raw is not None and not raw.error():
                self._pending.append(raw)
        raise TimeoutError(f"no partitions assigned within {timeout}s")

    def stop(self) -> None:
        """Ask the loop to finish the message in flight and exit (SIGTERM path)."""
        self._stopped.set()

    def run(self, max_messages: int | None = None) -> int:
        """Loop until stopped. max_messages bounds the loop for tests."""
        handled = 0
        try:
            while not self._stopped.is_set():
                self._check_not_stalled()
                raw = self._next_message()
                if raw is None:
                    continue
                self._process(raw)
                handled += 1
                if max_messages is not None and handled >= max_messages:
                    break
        finally:
            self._executor.shutdown(wait=True)
        return handled

    def _next_message(self) -> Any:
        if self._pending:
            return self._pending.pop(0)
        raw = self._consumer.poll(self._poll_timeout)
        if raw is None or raw.error():
            return None
        return raw

    def _process(self, raw: Any) -> None:
        self._revoked.clear()
        # Capture the assignment we pause, and resume exactly that set: a
        # rebalance during the handler can change what assignment() returns.
        paused = list(self._consumer.assignment())
        self._consumer.pause(paused)
        try:
            future = self._executor.submit(self._invoke_handler, raw)
            self._heartbeat_until(future)
            error = future.exception()
        finally:
            self._consumer.resume(paused)

        if error is not None:
            self._route_failure(raw, error)
        self._commit(raw)

    def _heartbeat_until(self, future: Future[None]) -> None:
        """Keep polling while the handler works — this is what prevents eviction.

        poll() is both the heartbeat and the wait, so there is no sleep here.
        Paused partitions yield nothing, but a partition assigned mid-handler
        can deliver a message; it is stashed rather than dropped.
        """
        while not future.done():
            extra = self._consumer.poll(self._poll_timeout)
            if extra is not None and not extra.error():
                self._pending.append(extra)

    def _invoke_handler(self, raw: Any) -> None:
        """Runs on the worker thread. Must not touch the consumer."""
        view = MessageView(raw)
        context = extract_trace_context(view.headers)
        with (
            tracer().start_as_current_span(f"{self.stage}.handle", context=context),
            observe_stage(self.stage),
        ):
            event = parse(view.value)
            self._handler(event, view)

    def _route_failure(self, raw: Any, error: BaseException) -> None:
        """Send the message to its next retry tier or the DLQ.

        Produced and flushed BEFORE the offset is committed: committing first
        would lose the message entirely if the produce failed.

        Raises the original error for a TRANSIENT failure on a topic with no
        retry ladder (ADR-0005 follow-on) instead of returning: Kafka offsets
        are monotonic, so silently continuing to poll and later committing a
        *later* message would move the committed offset past this one forever
        — there is no "come back to it" once that happens. Letting the
        exception crash the worker is the only way to guarantee redelivery:
        the process restarts and resumes from the last committed offset,
        which is still before this message.
        """
        view = MessageView(raw)
        failure = classify(error)
        origin = source_topic_of(view.topic)
        retry_count = view.retry_count
        retryable = REGISTRY[origin].retries if origin in REGISTRY else True
        destination = self._policy.route(origin, failure, retry_count, retryable=retryable)

        if destination is None:
            log.error(
                "unroutable transient failure on non-retryable topic %s; "
                "crashing so a restart redelivers from the last commit: %s",
                origin,
                error,
            )
            raise error

        headers = [(key, value) for key, value in view.headers if key != "retry_count"]
        headers += [
            ("retry_count", str(retry_count + 1).encode()),
            ("failure_reason", f"{type(error).__name__}: {error}".encode()[:512]),
            ("failure_class", str(failure.value).encode()),
            ("original_topic", origin.encode()),
            ("failed_in_stage", self.stage.encode()),
        ]
        self._producer.publish_raw(destination, view.key, view.value, headers)
        self._producer.flush()

        if self._policy.is_dlq(destination):
            DLQ_MESSAGES.labels(topic=destination, reason=failure.value).inc()
            log.error(
                "dead-lettered message from %s to %s after %d attempts: %s",
                view.topic,
                destination,
                retry_count,
                error,
            )
            self._emit_pipeline_failed(view, error, retry_count)
        else:
            RETRY_MESSAGES.labels(topic=origin, tier=destination.rsplit(".", 1)[-1]).inc()
            log.warning(
                "retrying message from %s via %s (attempt %d): %s",
                view.topic,
                destination,
                retry_count + 1,
                error,
            )

    def _emit_pipeline_failed(
        self, view: MessageView, error: BaseException, retry_count: int
    ) -> None:
        """Notify the rest of the system a message reached the DLQ.

        Consumed by the projector (for the read model's failure_reason) and by
        notify (Phase 11). Best-effort: the parse can fail if the payload itself
        was poison, and this must never crash the polling loop over a message
        that is already safely on the DLQ.

        Skipped when this worker itself consumes PIPELINE_FAILED (ADR-0005
        follow-on): the projector subscribes to both video.status and
        pipeline.failed, so a deterministic failure (e.g. a video_id with no
        row in `videos`) dead-lettering a pipeline.failed message would emit
        a fresh pipeline.failed, which this same worker then fails to process
        identically — an unbounded self-amplifying loop with no stopping
        condition. Dead-lettering silently is safe here: pipeline.failed is
        already the end of the failure-notification chain, nothing downstream
        depends on it re-emitting itself.
        """
        if PIPELINE_FAILED in self._subscribed_topics:
            log.warning(
                "not re-emitting pipeline.failed for a dead-lettered message on "
                "%s: this worker consumes pipeline.failed itself and re-emitting "
                "would loop",
                view.topic,
            )
            return
        try:
            event = parse(view.value)
        except Exception:
            log.warning(
                "could not attach video/owner context to pipeline.failed for "
                "an unparseable message on %s",
                view.topic,
            )
            return
        try:
            self._producer.publish(
                PIPELINE_FAILED,
                PipelineFailed(
                    video_id=event.video_id,
                    owner_id=event.owner_id,
                    producer=self.stage,
                    stage=self.stage,
                    reason=str(error)[:1024],
                    retry_count=retry_count,
                    terminal=True,
                    rendition=getattr(event, "rendition", None),
                ),
            )
            self._producer.flush()
        except Exception:
            log.warning("failed to publish pipeline.failed for %s", view.topic, exc_info=True)

    def _commit(self, raw: Any) -> None:
        """Commit synchronously, tolerating a partition we no longer own.

        A rebalance during long work can revoke the partition. That is not an
        error worth crashing over: the message will be redelivered, and every
        handler is idempotent by contract (ADR-0005).
        """
        try:
            self._consumer.commit(message=raw, asynchronous=False)
        except Exception as exc:
            log.warning(
                "commit failed (%s); message will be redelivered and handled idempotently",
                exc,
            )


__all__ = [
    "ConsumerGroupStalled",
    "ConsumerProtocol",
    "FailureClass",
    "Handler",
    "MessageView",
    "StageWorker",
    "consumer_config",
]

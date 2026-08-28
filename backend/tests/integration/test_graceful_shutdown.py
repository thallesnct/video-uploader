"""Graceful SIGTERM shutdown (Phase 12, ADR-0015 §4) against real Kafka.

StageWorker.stop() has existed since Phase 2 (its own docstring already says
"SIGTERM path"), but nothing ever wired a real SIGTERM to it — every worker
only stopped via `restart: unless-stopped` killing the process outright,
safe only because every consumer is idempotent (ADR-0005), not because
shutdown was actually graceful. libs/pipeline/runner.py's run_worker() is
the fix; this proves the underlying stop() semantics it relies on actually
hold: a stop() called mid-handler lets the in-flight message finish (no
work lost, no early abort) and its offset commits normally (no spurious
redelivery) — "graceful" as more than "eventually exits".

Calls worker.stop() directly rather than sending a real SIGTERM to this
test process: run_worker()'s own contribution is one line of signal-wiring
around an already-tested run() (test_consumer.py covers stop() at the unit
level with fakes); what only an integration test against real Kafka can
prove is the end-to-end claim — in-flight work finishes, the offset really
commits, a fresh consumer in the same group gets nothing.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest
from pipeline.consumer import StageWorker, consumer_config
from pipeline.events import VideoUploaded
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy
from pipeline.topics import REGISTRY

OWNER = "user|graceful-shutdown"
TOPIC = "video.uploaded"


def test_stop_mid_handler_finishes_the_in_flight_message_then_exits_cleanly(
    environment: None, kafka_bootstrap: str
) -> None:
    from confluent_kafka import Consumer

    video_id = uuid.uuid4()
    producer = EventProducer(service="test")
    event = VideoUploaded(
        video_id=video_id,
        owner_id=OWNER,
        producer="test",
        object_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        filename="clip.mp4",
        size_bytes=256,
        content_type="video/mp4",
    )
    producer.publish(TOPIC, event)
    producer.flush()

    finished = threading.Event()

    def slow_handler(evt: object, view: object) -> None:
        # Long enough that the test's stop() call below lands well before
        # this returns — proving stop() doesn't abort work already in flight.
        time.sleep(2)
        finished.set()

    group = f"graceful-shutdown-{uuid.uuid4()}"
    consumer = Consumer(consumer_config(group, {"auto.offset.reset": "earliest"}))
    worker = StageWorker(
        stage="graceful-shutdown-test",
        source_topic=TOPIC,
        consumer=consumer,
        producer=producer,
        handler=slow_handler,
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe()
    worker.wait_for_assignment()

    # Simulates run_worker()'s SIGTERM handler firing while the one message
    # is mid-handler: stop() only sets a flag checked between messages, so
    # this must not cut the sleeping handler short.
    threading.Timer(0.5, worker.stop).start()

    started = time.monotonic()
    handled = worker.run()
    elapsed = time.monotonic() - started
    consumer.close()

    assert handled == 1
    assert finished.is_set(), "stop() must not abort a handler already in flight"
    assert elapsed >= 2, "run() returned before the in-flight handler could have finished"

    # No spurious redelivery: the offset committed normally after the
    # handler succeeded, so a fresh consumer in the same group gets nothing.
    consumer2 = Consumer(consumer_config(group, {"auto.offset.reset": "earliest"}))
    worker2 = StageWorker(
        stage="graceful-shutdown-test",
        source_topic=TOPIC,
        consumer=consumer2,
        producer=producer,
        handler=slow_handler,
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker2.subscribe()
    worker2.wait_for_assignment()
    threading.Timer(2.0, worker2.stop).start()
    redelivered = worker2.run()
    consumer2.close()

    assert redelivered == 0, "the message must not redeliver after a graceful stop committed it"


@pytest.mark.slow
def test_run_worker_wires_a_real_sigterm_to_stop(environment: None, kafka_bootstrap: str) -> None:
    """The one line run_worker() actually adds beyond an already-tested
    run(): signal.signal(SIGTERM, ...). Proven with a real os.kill, not by
    calling stop() directly, since that's the only thing this test adds
    over the one above."""
    import os
    import signal
    import sys

    from confluent_kafka import Consumer

    video_id = uuid.uuid4()
    producer = EventProducer(service="test")
    event = VideoUploaded(
        video_id=video_id,
        owner_id=OWNER,
        producer="test",
        object_key=f"users/{OWNER}/videos/{video_id}/source.mp4",
        filename="clip.mp4",
        size_bytes=256,
        content_type="video/mp4",
    )
    producer.publish(TOPIC, event)
    producer.flush()

    if sys.platform == "win32":
        pytest.skip("SIGTERM semantics differ on Windows; every other CI/dev target is Linux")

    from pipeline.runner import run_worker

    def slow_handler(evt: object, view: object) -> None:
        time.sleep(2)

    group = f"graceful-shutdown-real-sigterm-{uuid.uuid4()}"
    consumer = Consumer(consumer_config(group, {"auto.offset.reset": "earliest"}))
    worker = StageWorker(
        stage="graceful-shutdown-test",
        source_topic=TOPIC,
        consumer=consumer,
        producer=producer,
        handler=slow_handler,
        policy=RetryPolicy(REGISTRY.retry_tiers),
        poll_timeout=0.5,
    )
    worker.subscribe()
    worker.wait_for_assignment()

    # run_worker() installs a process-wide SIGTERM handler and never restores
    # it — correct for a real worker process (SIGTERM should mean "shut
    # down" for the rest of its life), wrong to leave behind in a shared
    # pytest process running the rest of this suite afterward. Save/restore
    # explicitly so a later SIGTERM (e.g. a CI runner stopping the job) still
    # terminates this process normally rather than quietly calling
    # worker.stop() on an already-finished worker.
    previous_handler = signal.getsignal(signal.SIGTERM)
    try:
        threading.Timer(0.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()

        started = time.monotonic()
        run_worker(worker, producer=producer, consumer=consumer)
        elapsed = time.monotonic() - started
    finally:
        signal.signal(signal.SIGTERM, previous_handler)

    assert elapsed >= 2, "a real SIGTERM must not cut the in-flight handler short"

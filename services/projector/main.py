"""Projector — the sole writer of read-model STATE columns (ADR-0007).

Consumes video.status and pipeline.failed and upserts videos/renditions plus
an append-only events log for SSE Last-Event-ID replay (ADR-0008). Every write
is an upsert, so replaying a partition twice produces identical rows — this is
what lets the Kafka offset be committed only after the DB transaction, safely.

video.status and pipeline.failed are declared `retries: false` (ADR-0005
follow-on): a transient DB failure here leaves the message uncommitted for
ordinary redelivery rather than routing to a retry-tier topic, since an upsert
is cheap to retry and there is no ladder topic for either to route through.
"""

from __future__ import annotations

import logging

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.db import create_sync_engine, sync_session_scope, sync_sessions
from pipeline.events import Event, PipelineFailed, VideoStatusChanged
from pipeline.health import HealthRegistry, serve_health
from pipeline.obs import setup_tracing
from pipeline.producer import EventProducer
from pipeline.repository import ProjectorRepository
from pipeline.retry import RetryPolicy, TerminalError
from pipeline.settings import observability_settings
from pipeline.topics import PIPELINE_FAILED, REGISTRY, VIDEO_STATUS
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

SERVICE = "projector"
GROUP = "projector"

log = logging.getLogger(__name__)


def build_handler(sessions_factory: sessionmaker[Session]) -> Handler:
    def handle(event: Event, view: MessageView) -> None:
        if not isinstance(event, (VideoStatusChanged, PipelineFailed)):
            raise TerminalError(f"projector received unexpected event {event.type}")

        try:
            with sync_session_scope(sessions_factory) as session:
                ProjectorRepository(session).apply(event)
        except IntegrityError as exc:
            # video.status/pipeline.failed have no retry ladder (ADR-0005
            # follow-on): an unclassified exception here defaults to
            # TRANSIENT and crashes the worker forever on the same message.
            # A constraint violation — most realistically the events FK when
            # the referenced video row does not exist — cannot resolve on
            # retry, so it must dead-letter instead of crash-looping.
            raise TerminalError(f"projector upsert violated a constraint: {exc}") from exc

    return handle


def _database_reachable(sessions_factory: sessionmaker[Session]) -> bool:
    try:
        with sessions_factory() as session:
            session.execute(text("select 1"))
    except Exception:
        return False
    return True


def main() -> None:
    from confluent_kafka import Consumer

    logging.basicConfig(level=observability_settings().log_level)
    setup_tracing(SERVICE)

    engine = create_sync_engine()
    sessions_factory = sync_sessions(engine)
    producer = EventProducer(service=SERVICE)
    consumer = Consumer(consumer_config(GROUP))

    worker = StageWorker(
        stage=SERVICE,
        source_topic=VIDEO_STATUS,
        consumer=consumer,
        producer=producer,
        handler=build_handler(sessions_factory),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    health.register("dependencies", lambda: _database_reachable(sessions_factory))
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe(topics=[VIDEO_STATUS, PIPELINE_FAILED])
    try:
        worker.run()
    finally:
        producer.flush()
        consumer.close()
        engine.dispose()


if __name__ == "__main__":
    main()

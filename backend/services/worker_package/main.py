"""Packaging stage — the fan-in join (ADR-0013, and its follow-on).

Subscribes to *two* topics, `video.probed` and `rendition.completed`, and
persists its own fact before checking the join condition in either handler —
never reads the projector's `expected_renditions`/`status` columns, which are
written from `video.status`, a third topic with no ordering guarantee
relative to these two. See the ADR-0013 follow-on for the race this design
replaces and why it can't rely on Kafka redelivery (no retry pump yet).

No ffmpeg here: master.m3u8 is text, built from playlist keys already carried
on the events this worker consumes.
"""

from __future__ import annotations

import logging
import os
import tempfile
from uuid import UUID

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.db import create_sync_engine, sync_session_scope, sync_sessions
from pipeline.events import (
    Event,
    RenditionCompleted,
    VideoCompleted,
    VideoProbed,
    VideoState,
    VideoStatusChanged,
)
from pipeline.health import HealthRegistry, serve_health
from pipeline.hls import build_master_playlist
from pipeline.obs import setup_tracing
from pipeline.producer import EventProducer
from pipeline.repository import PackagerRepository
from pipeline.retry import RetryPolicy, TerminalError, TransientError
from pipeline.runner import run_worker
from pipeline.settings import observability_settings
from pipeline.storage import ObjectStore, hls_master_key, object_store, scratch_key
from pipeline.topics import (
    REGISTRY,
    RENDITION_COMPLETED,
    VIDEO_COMPLETED,
    VIDEO_PROBED,
    VIDEO_STATUS,
)
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

SERVICE = "package"
GROUP = "packager"

log = logging.getLogger(__name__)


def build_handler(
    store: ObjectStore, producer: EventProducer, sessions_factory: sessionmaker[Session]
) -> Handler:
    def handle(event: Event, view: MessageView) -> None:
        if isinstance(event, VideoProbed):
            with sync_session_scope(sessions_factory) as session:
                PackagerRepository(session).record_expected(
                    event.video_id, event.expected_renditions
                )
                playlists = PackagerRepository(session).ready_playlists(event.video_id)
        elif isinstance(event, RenditionCompleted):
            if event.playlist_key is None:
                # Only reachable from a rendition.completed published by a
                # pre-Phase-9 producer — a poison message, not a transient
                # condition (ADR-0005), same shape as VideoProbed.source_key.
                raise TerminalError("rendition.completed has no playlist_key to package with")
            with sync_session_scope(sessions_factory) as session:
                recorded = PackagerRepository(session).record_rendition(
                    event.video_id, event.rendition, event.playlist_key
                )
                if not recorded:
                    # worker_transcode's claim() always creates the renditions
                    # row before rendition.completed is ever published — a
                    # missing row here is a genuine invariant violation, not
                    # something a retry resolves.
                    raise TerminalError(f"no renditions row for {event.video_id}/{event.rendition}")
                playlists = PackagerRepository(session).ready_playlists(event.video_id)
        else:
            raise TransientError(f"packager received unexpected event {event.type}")

        if playlists is not None:
            _try_package(
                event.video_id, event.owner_id, playlists, store, sessions_factory, producer
            )

    return handle


def _try_package(
    video_id: UUID,
    owner_id: str,
    playlists: dict[str, str],
    store: ObjectStore,
    sessions_factory: sessionmaker[Session],
    producer: EventProducer,
) -> None:
    master_key = hls_master_key(owner_id, video_id)

    if store.exists(master_key):
        # Already packaged — this delivery is a redelivery of whichever
        # message last completed the join. Re-announce rather than re-derive:
        # nothing to recompute, the object already holds the answer.
        _announce(producer, video_id, owner_id, master_key, playlists)
        return

    with sync_session_scope(sessions_factory) as session:
        claimed = PackagerRepository(session).claim(video_id)
    if not claimed:
        log.info("packaging already claimed for video=%s — leaving it to that attempt", video_id)
        return

    try:
        playlist_text = build_master_playlist(sorted(playlists))
        scratch_dir = os.environ.get("SCRATCH_DIR", "/tmp")  # noqa: S108
        with tempfile.TemporaryDirectory(dir=scratch_dir) as scratch:
            local_master = os.path.join(scratch, "master.m3u8")
            with open(local_master, "w", encoding="utf-8") as fh:
                fh.write(playlist_text)
            remote_scratch = scratch_key(owner_id, video_id, "master.m3u8.part")
            store.upload(local_master, remote_scratch, content_type="application/vnd.apple.mpegurl")
            store.promote(remote_scratch, master_key)

        log.info("packaged video=%s with %d renditions", video_id, len(playlists))
        _announce(producer, video_id, owner_id, master_key, playlists)
    except Exception:
        # Guarantees a future retry can finish the job: this exception also
        # propagates out of the handler, so the triggering message's offset
        # is never committed and Kafka redelivers it (ADR-0004's commit-only-
        # after-success discipline). The claim would otherwise strand the
        # video in "claimed but never packaged" until STALE_AFTER passes.
        with sync_session_scope(sessions_factory) as session:
            PackagerRepository(session).release_claim(video_id)
        raise


def _announce(
    producer: EventProducer,
    video_id: UUID,
    owner_id: str,
    master_key: str,
    playlists: dict[str, str],
) -> None:
    renditions = sorted(playlists)
    producer.publish(
        VIDEO_COMPLETED,
        VideoCompleted(
            video_id=video_id,
            owner_id=owner_id,
            producer=SERVICE,
            master_playlist_key=master_key,
            renditions=renditions,
        ),
    )
    producer.publish(
        VIDEO_STATUS,
        VideoStatusChanged(
            video_id=video_id,
            owner_id=owner_id,
            producer=SERVICE,
            state=VideoState.COMPLETED,
            detail=f"packaged {len(renditions)} renditions",
            master_playlist_key=master_key,
        ),
    )
    producer.flush()


def _dependencies_reachable(store: ObjectStore, sessions_factory: sessionmaker[Session]) -> bool:
    try:
        store.client.head_bucket(Bucket=store.bucket)
        with sessions_factory() as session:
            session.execute(text("select 1"))
    except Exception:
        return False
    return True


def main() -> None:
    from confluent_kafka import Consumer

    logging.basicConfig(level=observability_settings().log_level)
    setup_tracing(SERVICE)

    store = object_store()
    producer = EventProducer(service=SERVICE)
    engine = create_sync_engine()
    sessions_factory = sync_sessions(engine)
    consumer = Consumer(consumer_config(GROUP))

    worker = StageWorker(
        stage=SERVICE,
        source_topic=RENDITION_COMPLETED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, sessions_factory),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    health.register("dependencies", lambda: _dependencies_reachable(store, sessions_factory))
    health.register("kafka_group", lambda: worker.seconds_unassigned() is None)
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe(topics=[RENDITION_COMPLETED, VIDEO_PROBED])
    run_worker(worker, producer=producer, consumer=consumer, engine=engine)


if __name__ == "__main__":
    main()

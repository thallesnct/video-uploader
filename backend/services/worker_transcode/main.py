"""Transcode stage — the highest-risk service in the pipeline (ADR-0004).

A rendition can take minutes to hours to encode. The consumer loop
(`pipeline.consumer.StageWorker`) is what keeps this worker from being evicted
from its group mid-transcode; this module is the handler that runs inside it.

Idempotency is two layers, cheapest first (ADR-0005):
1. Object existence — if the target key already has an object, a previous
   attempt succeeded and only its ack/commit was lost. Re-announce, don't
   re-encode. Widened for HLS (Phase 9): the MP4 and the HLS playlist are
   two separate promotes, so a skip requires *both* present — an attempt
   that died between them left the MP4 done but the playlist missing, and
   that gap is real, not hypothetical, since nothing makes the two promotes
   atomic together.
2. A DB claim — mutual exclusion against a message being processed twice
   *concurrently* (a manual DLQ replay racing a live retry-tier message for the
   same rendition), which is a real scenario Kafka redelivery alone cannot
   prevent. See `RenditionRepository.claim`'s docstring. Only guards the
   transcode itself; remuxing an already-finished MP4 into HLS needs no claim
   — segments are just re-derived and re-uploaded, safe to race.
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Callable

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.db import create_sync_engine, sync_session_scope, sync_sessions
from pipeline.events import (
    Event,
    RenditionCompleted,
    RenditionRequested,
    VideoState,
    VideoStatusChanged,
)
from pipeline.health import HealthRegistry, serve_health
from pipeline.hls import HlsResult, generate_hls
from pipeline.obs import TRANSCODE_REALTIME_RATIO, setup_tracing
from pipeline.producer import EventProducer
from pipeline.repository import RenditionRepository
from pipeline.retry import RetryPolicy, TransientError
from pipeline.runner import run_worker
from pipeline.settings import observability_settings
from pipeline.storage import (
    ObjectStore,
    hls_playlist_key,
    hls_segment_key,
    object_store,
    scratch_key,
)
from pipeline.topics import REGISTRY, RENDITION_COMPLETED, RENDITION_REQUESTED, VIDEO_STATUS
from pipeline.transcode import TranscodeResult, timeout_budget_s, transcode
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

SERVICE = "worker-transcode"
GROUP = "transcode"

log = logging.getLogger(__name__)

TranscodeFn = Callable[..., TranscodeResult]
HlsFn = Callable[..., HlsResult]


def build_handler(
    store: ObjectStore,
    producer: EventProducer,
    sessions_factory: sessionmaker[Session],
    transcode_fn: TranscodeFn = transcode,
    hls_fn: HlsFn = generate_hls,
) -> Handler:
    """The transcode handler.

    `transcode_fn`/`hls_fn` are injected so the Kafka round trip, the
    idempotency skip, and the claim can all be tested on a machine without
    ffmpeg (ADR-0011); the real ffmpeg path is covered separately by tests
    marked `ffmpeg`.
    """

    def handle(event: Event, view: MessageView) -> None:
        if not isinstance(event, RenditionRequested):
            raise TransientError(f"transcode received unexpected event {event.type}")

        playlist_final = hls_playlist_key(event.owner_id, event.video_id, event.rendition)
        existing_mp4 = store.head(event.target_key)
        playlist_ready = store.exists(playlist_final)

        if existing_mp4 is not None and playlist_ready:
            log.info(
                "rendition and HLS playlist already present for video=%s rendition=%s — "
                "re-announcing without redoing work",
                event.video_id,
                event.rendition,
            )
            _announce(
                producer, event, int(existing_mp4.get("ContentLength", 0)), 0.0, playlist_final
            )
            return

        need_hls = not playlist_ready
        scratch_dir = os.environ.get("SCRATCH_DIR", "/tmp")  # noqa: S108
        with tempfile.TemporaryDirectory(dir=scratch_dir) as scratch:
            local_output = os.path.join(scratch, f"{event.rendition}.mp4")
            elapsed = 0.0

            if existing_mp4 is None:
                with sync_session_scope(sessions_factory) as session:
                    claimed = RenditionRepository(session).claim(
                        event.owner_id, event.video_id, event.rendition
                    )
                if not claimed:
                    raise TransientError(
                        f"another attempt already holds the claim for "
                        f"{event.video_id}/{event.rendition}"
                    )

                local_source = os.path.join(scratch, "source")
                store.download(event.source_key, local_source)

                started = time.monotonic()
                transcode_fn(
                    local_source,
                    local_output,
                    event.rendition,
                    timeout_s=timeout_budget_s(event.duration_s),
                )
                elapsed = time.monotonic() - started

                remote_scratch = scratch_key(
                    event.owner_id, event.video_id, f"{event.rendition}.part"
                )
                store.upload(local_output, remote_scratch, content_type="video/mp4")
                store.promote(remote_scratch, event.target_key)
                # A freshly-produced MP4 always gets freshly-produced HLS —
                # never trust a playlist that might reference a prior attempt.
                need_hls = True

                if event.duration_s > 0:
                    TRANSCODE_REALTIME_RATIO.labels(rendition=event.rendition).observe(
                        elapsed / event.duration_s
                    )
                log.info(
                    "transcoded video=%s rendition=%s in %.1fs (%.2fx realtime)",
                    event.video_id,
                    event.rendition,
                    elapsed,
                    elapsed / event.duration_s if event.duration_s > 0 else 0.0,
                )
            else:
                log.info(
                    "rendition already present but HLS playlist missing for "
                    "video=%s rendition=%s — remuxing without re-encoding",
                    event.video_id,
                    event.rendition,
                )
                store.download(event.target_key, local_output)

            if need_hls:
                hls_dir = os.path.join(scratch, "hls")
                hls_result = hls_fn(local_output, hls_dir)
                for segment_path in hls_result.segment_paths:
                    filename = os.path.basename(segment_path)
                    store.upload(
                        segment_path,
                        hls_segment_key(event.owner_id, event.video_id, event.rendition, filename),
                        content_type="video/mp2t",
                    )
                remote_playlist_scratch = scratch_key(
                    event.owner_id, event.video_id, f"{event.rendition}.playlist.part"
                )
                store.upload(
                    hls_result.playlist_path,
                    remote_playlist_scratch,
                    content_type="application/vnd.apple.mpegurl",
                )
                store.promote(remote_playlist_scratch, playlist_final)

        result = store.head(event.target_key)
        size_bytes = int(result.get("ContentLength", 0)) if result else 0
        _announce(producer, event, size_bytes, elapsed, playlist_final)

    return handle


def _announce(
    producer: EventProducer,
    event: RenditionRequested,
    size_bytes: int,
    transcode_seconds: float,
    playlist_key: str,
) -> None:
    producer.publish(
        RENDITION_COMPLETED,
        RenditionCompleted(
            video_id=event.video_id,
            owner_id=event.owner_id,
            producer=SERVICE,
            rendition=event.rendition,
            object_key=event.target_key,
            size_bytes=size_bytes,
            transcode_seconds=transcode_seconds,
            playlist_key=playlist_key,
        ),
    )
    producer.publish(
        VIDEO_STATUS,
        VideoStatusChanged(
            video_id=event.video_id,
            owner_id=event.owner_id,
            producer=SERVICE,
            state=VideoState.TRANSCODING,
            rendition=event.rendition,
            detail=f"{event.rendition} ready",
            rendition_object_key=event.target_key,
            rendition_size_bytes=size_bytes,
            rendition_playlist_key=playlist_key,
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
        source_topic=RENDITION_REQUESTED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer, sessions_factory),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    health.register("dependencies", lambda: _dependencies_reachable(store, sessions_factory))
    # A stale-but-true value here is fine: readiness only affects routing, and
    # the actual recovery is `worker.run()` crashing on a real stall (see
    # StageWorker._check_not_stalled) plus `restart: unless-stopped`.
    health.register("kafka_group", lambda: worker.seconds_unassigned() is None)
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe()
    run_worker(worker, producer=producer, consumer=consumer, engine=engine)


if __name__ == "__main__":
    main()

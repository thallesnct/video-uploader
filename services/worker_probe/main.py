"""Probe stage: read what the source actually is, then plan the fan-out.

This is where the pipeline stops being fixed and becomes data-dependent
(ADR-0012). A 720p upload must never produce an upscaled 1080p rendition, so the
ladder is computed from the source rather than configured.

The single most important invariant here: the ladder is computed **once** and
both the `expected_renditions` list and the emitted `rendition.requested`
messages derive from that one list. If they ever disagree, the packaging join in
ADR-0013 waits forever for a rendition nobody was asked to produce, and the video
hangs in `transcoding` with nothing to alert on.
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.events import (
    Event,
    RenditionRequested,
    VideoProbed,
    VideoState,
    VideoStatusChanged,
    VideoUploaded,
)
from pipeline.health import HealthRegistry, serve_health
from pipeline.ladder import display_dimensions, select_ladder
from pipeline.media import MediaInfo, probe
from pipeline.obs import setup_tracing
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy, TransientError
from pipeline.settings import observability_settings
from pipeline.storage import ObjectStore, object_store, rendition_key
from pipeline.topics import (
    REGISTRY,
    RENDITION_REQUESTED,
    VIDEO_PROBED,
    VIDEO_STATUS,
    VIDEO_UPLOADED,
)

SERVICE = "worker-probe"
GROUP = "probe"

log = logging.getLogger(__name__)

Prober = Callable[[str], MediaInfo]


def build_handler(store: ObjectStore, producer: EventProducer, prober: Prober = probe) -> Handler:
    """The probe handler.

    `prober` is injected so the Kafka round trip can be tested on a machine
    without ffmpeg (ADR-0011); the real ffprobe path is covered separately by
    tests marked `ffmpeg` that run inside the worker image.
    """

    def handle(event: Event, view: MessageView) -> None:
        if not isinstance(event, VideoUploaded):
            raise TransientError(f"probe received unexpected event {event.type}")

        # Scratch space only — the container runs with a read-only root
        # filesystem, so this must be the mounted tmpfs (ADR-0015).
        with tempfile.TemporaryDirectory(dir=os.environ.get("SCRATCH_DIR", "/tmp")) as scratch:  # noqa: S108
            local = os.path.join(scratch, "source")
            store.download(event.object_key, local)
            info = prober(local)

        width, height = display_dimensions(info.width, info.height, info.rotation)
        # Computed once. Everything below derives from this list.
        ladder = select_ladder(info.width, info.height, info.rotation)

        log.info(
            "probed video=%s %dx%d %.2fs codec=%s ladder=%s",
            event.video_id,
            width,
            height,
            info.duration_s,
            info.video_codec,
            ladder,
        )

        producer.publish(
            VIDEO_PROBED,
            VideoProbed(
                video_id=event.video_id,
                owner_id=event.owner_id,
                producer=SERVICE,
                duration_s=info.duration_s,
                width=width,
                height=height,
                video_codec=info.video_codec,
                audio_codec=info.audio_codec,
                expected_renditions=ladder,
            ),
        )

        for rendition in ladder:
            producer.publish(
                RENDITION_REQUESTED,
                RenditionRequested(
                    video_id=event.video_id,
                    owner_id=event.owner_id,
                    producer=SERVICE,
                    rendition=rendition,
                    source_key=event.object_key,
                    target_key=rendition_key(event.owner_id, event.video_id, rendition),
                    duration_s=info.duration_s,
                ),
            )

        # The browser can render placeholders for the whole ladder as soon as
        # this lands, before any transcode finishes (ADR-0008).
        producer.publish(
            VIDEO_STATUS,
            VideoStatusChanged(
                video_id=event.video_id,
                owner_id=event.owner_id,
                producer=SERVICE,
                state=VideoState.PROBED,
                detail=f"{len(ladder)} renditions planned: {', '.join(ladder)}",
            ),
        )
        # Nothing is committed until these are actually delivered.
        producer.flush()

    return handle


def _bucket_reachable(store: ObjectStore) -> bool:
    """Readiness only — never wired to liveness (ADR-0015)."""
    try:
        store.client.head_bucket(Bucket=store.bucket)
    except Exception:
        return False
    return True


def main() -> None:
    from confluent_kafka import Consumer

    logging.basicConfig(level=observability_settings().log_level)
    setup_tracing(SERVICE)

    store = object_store()
    producer = EventProducer(service=SERVICE)
    consumer = Consumer(consumer_config(GROUP))

    worker = StageWorker(
        stage=SERVICE,
        source_topic=VIDEO_UPLOADED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    # Must actually be able to fail. `head(...) is None or True` is always True,
    # which looks like coverage while reporting ready through a total outage.
    health.register("object_store", lambda: _bucket_reachable(store))
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe()
    try:
        worker.run()
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()

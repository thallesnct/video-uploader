"""Thumbnail stage: poster frame, sprite sheet, and WebVTT cues (ADR-0012).

Runs off `video.probed`, in parallel with the per-rendition fan-out
`worker_transcode` consumes from `rendition.requested` — both are triggered by
the same probe event, and neither waits on the other (ADR-0013's completion
join only waits on renditions, not on thumbnails).

Idempotency is the object-existence check alone (ADR-0005's cheapest layer):
unlike a rendition, there is exactly one poster/sprite/vtt set per video, no
per-item DB row to contend over, so there is nothing for a claim to arbitrate.
A concurrent redelivery just re-derives and re-promotes the same bytes.

Publishes `state=VideoState.TRANSCODING`, never `PROBED` — by the time
`video.probed` exists, `rendition.requested` has already fanned out and the
pipeline is conceptually transcoding; thumbnail work is sibling work within
that same stage. `ProjectorRepository._apply_status` sets `status` from
whatever `state` a `video.status` event carries with no ordering guard, so
publishing `PROBED` here could regress a video already advanced to
`TRANSCODING` by a faster-finishing rendition — a real risk given thumbnail
and transcode run as genuinely concurrent, unordered stages.
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.events import Event, VideoProbed, VideoState, VideoStatusChanged
from pipeline.health import HealthRegistry, serve_health
from pipeline.obs import setup_tracing
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy, TerminalError, TransientError
from pipeline.runner import run_worker
from pipeline.settings import observability_settings
from pipeline.storage import ObjectStore, object_store, poster_key, scratch_key
from pipeline.storage import sprite_key as sprite_key_for
from pipeline.storage import sprite_vtt_key as vtt_key_for
from pipeline.thumbnail import build_vtt, generate_poster, generate_sprite, plan_sprite
from pipeline.topics import REGISTRY, VIDEO_PROBED, VIDEO_STATUS

SERVICE = "worker-thumbnail"
GROUP = "thumbnail"

log = logging.getLogger(__name__)

PosterFn = Callable[..., None]
SpriteFn = Callable[..., None]


def build_handler(
    store: ObjectStore,
    producer: EventProducer,
    poster_fn: PosterFn = generate_poster,
    sprite_fn: SpriteFn = generate_sprite,
) -> Handler:
    """`poster_fn`/`sprite_fn` are injected so the Kafka round trip and the
    idempotency skip can be tested without ffmpeg (ADR-0011); the real ffmpeg
    path is covered separately by tests marked `ffmpeg`."""

    def handle(event: Event, view: MessageView) -> None:
        if not isinstance(event, VideoProbed):
            raise TransientError(f"thumbnail received unexpected event {event.type}")
        if event.source_key is None:
            # Only reachable from a video.probed published by a pre-Phase-9
            # producer — a poison message for this consumer, not a transient
            # condition a retry could ever resolve (ADR-0005).
            raise TerminalError("video.probed has no source_key to download from")

        final_poster = poster_key(event.owner_id, event.video_id)
        final_sprite = sprite_key_for(event.owner_id, event.video_id)
        final_vtt = vtt_key_for(event.owner_id, event.video_id)

        if store.exists(final_poster) and store.exists(final_sprite) and store.exists(final_vtt):
            log.info(
                "thumbnails already present for video=%s — re-announcing without regenerating",
                event.video_id,
            )
            _announce(producer, event, final_poster, final_sprite, final_vtt)
            return

        scratch_dir = os.environ.get("SCRATCH_DIR", "/tmp")  # noqa: S108
        with tempfile.TemporaryDirectory(dir=scratch_dir) as scratch:
            local_source = os.path.join(scratch, "source")
            store.download(event.source_key, local_source)

            local_poster = os.path.join(scratch, "poster.jpg")
            poster_fn(local_source, local_poster, duration_s=event.duration_s)
            remote_poster = scratch_key(event.owner_id, event.video_id, "poster.jpg.part")
            store.upload(local_poster, remote_poster, content_type="image/jpeg")
            store.promote(remote_poster, final_poster)

            layout = plan_sprite(event.duration_s)
            local_sprite = os.path.join(scratch, "sprite.jpg")
            sprite_fn(local_source, local_sprite, layout)
            remote_sprite = scratch_key(event.owner_id, event.video_id, "sprite.jpg.part")
            store.upload(local_sprite, remote_sprite, content_type="image/jpeg")
            store.promote(remote_sprite, final_sprite)

            vtt_text = build_vtt(layout, final_sprite, event.duration_s)
            local_vtt = os.path.join(scratch, "sprite.vtt")
            with open(local_vtt, "w", encoding="utf-8") as fh:
                fh.write(vtt_text)
            remote_vtt = scratch_key(event.owner_id, event.video_id, "sprite.vtt.part")
            store.upload(local_vtt, remote_vtt, content_type="text/vtt")
            store.promote(remote_vtt, final_vtt)

        log.info("thumbnails ready for video=%s (%d sprite tiles)", event.video_id, layout.count)
        _announce(producer, event, final_poster, final_sprite, final_vtt)

    return handle


def _announce(
    producer: EventProducer,
    event: VideoProbed,
    poster: str,
    sprite: str,
    vtt: str,
) -> None:
    producer.publish(
        VIDEO_STATUS,
        VideoStatusChanged(
            video_id=event.video_id,
            owner_id=event.owner_id,
            producer=SERVICE,
            state=VideoState.TRANSCODING,
            detail="thumbnails ready",
            poster_key=poster,
            sprite_key=sprite,
            vtt_key=vtt,
        ),
    )
    producer.flush()


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
        source_topic=VIDEO_PROBED,
        consumer=consumer,
        producer=producer,
        handler=build_handler(store, producer),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    health.register("object_store", lambda: _bucket_reachable(store))
    health.register("kafka_group", lambda: worker.seconds_unassigned() is None)
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe()
    run_worker(worker, producer=producer, consumer=consumer)


if __name__ == "__main__":
    main()

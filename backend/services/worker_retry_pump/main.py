"""The retry-tier pump (ADR-0002, ADR-0005, ADR-0009 all name this component
by the same name, none of them scheduled building it — Phase 11).

Without this, a TRANSIENT failure has been equivalent to a silent, permanent
drop since Phase 5: `_route_failure` (consumer.py) correctly produces a
failed message to `<topic>.retry.<tier>`, but nothing ever consumed that
topic and republished to `<topic>` once the delay elapsed. The message just
sat there forever.

One process, subscribed to *every* retry-tier topic in the registry at
once — `REGISTRY.declared` × `REGISTRY.retry_tiers`, derived the same way
`infra/bootstrap_topics.py` derives which topics must exist, so this can
never fall out of step with what's actually provisioned. "Retry pump" is
named as a single component in every ADR that mentions it, not one per tier.

Per message: sleep until `occurred_at + tier_delay_seconds(tier)` (bounded —
600s at the outside, the "10m" tier), then republish the untouched
value/headers to the original topic. The sleep runs on `StageWorker`'s
handler thread while its existing pause/heartbeat loop keeps the consumer
group alive throughout — the exact mechanism `worker_transcode`'s
multi-minute ffmpeg runs already rely on safely exceeding
`max.poll.interval.ms`. This is the standard shape for a Kafka-backed delay
queue: block on the head-of-line message per partition, which is fine
because messages within one tier's topic arrive in roughly failure-order
and share that tier's fixed delay.

Headers are forwarded exactly as consumed — `retry_count` must carry
forward untouched, so a second failure on the republished message advances
to the *next* tier (`_route_failure` already handles this correctly with no
pump-specific code, since it recomputes the destination from
`source_topic_of(topic)` and the `retry_count` already in the headers).

Named, accepted limitation: one process handles one message at a time
(ADR-0004's whole design), so a burst in one tier serializes behind each
other's sleep. Same scaling lever as every other stage — more replicas via
Kafka's partition-based rebalancing (these topics already have multiple
partitions), not multi-threading within one process.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta

from pipeline.consumer import Handler, MessageView, StageWorker, consumer_config
from pipeline.events import Event
from pipeline.health import HealthRegistry, serve_health
from pipeline.obs import setup_tracing
from pipeline.producer import EventProducer
from pipeline.retry import RetryPolicy, source_topic_of, tier_delay_seconds, tier_of
from pipeline.runner import run_worker
from pipeline.settings import observability_settings
from pipeline.topics import REGISTRY

SERVICE = "worker-retry-pump"
GROUP = "retry-pump"

log = logging.getLogger(__name__)


def retry_tier_topics() -> list[str]:
    """Every `<topic>.retry.<tier>` this pump must drain — derived from the
    registry, same as `infra/bootstrap_topics.py` derives which ones must
    exist on the broker, so the two can never disagree about the set."""
    return [
        spec.retry_topic(tier)
        for spec in REGISTRY.declared
        if spec.retries
        for tier in REGISTRY.retry_tiers
    ]


def build_handler(producer: EventProducer) -> Handler:
    def handle(event: Event, view: MessageView) -> None:
        delay_s = tier_delay_seconds(tier_of(view.topic))
        due_at = event.occurred_at + timedelta(seconds=delay_s)
        remaining = (due_at - datetime.now(UTC)).total_seconds()
        if remaining > 0:
            time.sleep(remaining)

        source = source_topic_of(view.topic)
        producer.publish_raw(source, view.key, view.value, view.headers)
        producer.flush()

    return handle


def main() -> None:
    from confluent_kafka import Consumer

    logging.basicConfig(level=observability_settings().log_level)
    setup_tracing(SERVICE)

    producer = EventProducer(service=SERVICE)
    consumer = Consumer(consumer_config(GROUP))
    topics = retry_tier_topics()

    worker = StageWorker(
        stage=SERVICE,
        # Unused beyond StageWorker's own constructor default — subscribe()
        # below always passes the real, full topic list explicitly.
        source_topic=topics[0],
        consumer=consumer,
        producer=producer,
        handler=build_handler(producer),
        policy=RetryPolicy(REGISTRY.retry_tiers),
    )

    health = HealthRegistry()
    health.register("kafka_group", lambda: worker.seconds_unassigned() is None)
    serve_health(health, observability_settings().metrics_port)

    worker.subscribe(topics=topics)
    run_worker(worker, producer=producer, consumer=consumer)


if __name__ == "__main__":
    main()

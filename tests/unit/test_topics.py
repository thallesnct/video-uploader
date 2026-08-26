"""The registry is shared by infra scripts and services, so drift is a real risk."""
from __future__ import annotations

from pipeline import topics


def test_declared_topics_match_the_adr_table() -> None:
    registry = topics.REGISTRY
    expected = {
        topics.VIDEO_UPLOADED: 3,
        topics.VIDEO_PROBED: 3,
        topics.RENDITION_REQUESTED: 12,
        topics.RENDITION_COMPLETED: 6,
        topics.VIDEO_COMPLETED: 3,
        topics.VIDEO_STATUS: 6,
        topics.PIPELINE_FAILED: 3,
    }
    assert {spec.name for spec in registry.declared} == set(expected)
    for name, partitions in expected.items():
        assert registry[name].partitions["dev"] == partitions


def test_every_topic_with_retries_gets_a_full_ladder_and_a_dlq() -> None:
    registry = topics.REGISTRY
    planned = {plan.name for plan in registry.plan("dev")}

    for spec in registry.declared:
        assert spec.name in planned
        if not spec.retries:
            assert spec.dlq_topic not in planned
            continue
        assert spec.dlq_topic in planned
        for tier in registry.retry_tiers:
            assert spec.retry_topic(tier) in planned


def test_dlq_outlives_the_pipeline() -> None:
    """Replaying a DLQ is a human action; retention must survive the weekend."""
    plans = {plan.name: plan for plan in topics.REGISTRY.plan("dev")}
    source = topics.REGISTRY[topics.RENDITION_REQUESTED]

    dlq_retention = int(plans[source.dlq_topic].configs["retention.ms"])
    topic_retention = int(plans[source.name].configs["retention.ms"])

    assert dlq_retention == topics.DLQ_RETENTION_MS
    assert dlq_retention > topic_retention


def test_prod_profile_is_replicated_and_wider() -> None:
    dev = {p.name: p for p in topics.REGISTRY.plan("dev")}
    prod = {p.name: p for p in topics.REGISTRY.plan("prod")}

    assert set(dev) == set(prod)
    for name, plan in prod.items():
        assert plan.replication_factor == 3
        assert plan.configs["min.insync.replicas"] == "2"
        assert plan.partitions >= dev[name].partitions


def test_hot_path_has_the_most_partitions() -> None:
    """Partition count caps transcode parallelism (ADR-0002)."""
    registry = topics.REGISTRY
    hot = registry[topics.RENDITION_REQUESTED].partitions["dev"]
    assert all(
        spec.partitions["dev"] <= hot
        for spec in registry.declared
        if spec.name != topics.RENDITION_REQUESTED
    )

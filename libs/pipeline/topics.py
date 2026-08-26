"""Topic registry — the one place topic names, partitions and retention live.

Deliberately stdlib-only. infra/bootstrap_topics.py and infra/smoke.py import it
directly on a bare host with nothing installed, so nothing here may depend on
pydantic or any third-party package. Keeping the bootstrap and the application
on the same code is what stops broker reality from drifting away from ADR-0002.
"""
from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass
from typing import Literal

REGISTRY_PATH = pathlib.Path(__file__).with_name("topics.json")

Profile = Literal["dev", "prod"]

# DLQ messages outlive the pipeline by design: replaying them is a deliberate
# human action, and humans work on Mondays (ADR-0005).
DLQ_RETENTION_MS = 90 * 24 * 3600 * 1000


@dataclass(frozen=True)
class TopicSpec:
    """One declared topic, plus the retry and DLQ topics derived from it."""

    name: str
    partitions: dict[str, int]
    retention_ms: int
    produced_by: tuple[str, ...]
    consumed_by: tuple[str, ...]
    retries: bool

    def retry_topic(self, tier: str) -> str:
        return f"{self.name}.retry.{tier}"

    @property
    def dlq_topic(self) -> str:
        return f"{self.name}.dlq"


@dataclass(frozen=True)
class TopicPlan:
    """A topic as it must exist on the broker."""

    name: str
    partitions: int
    replication_factor: int
    configs: dict[str, str]


class TopicRegistry:
    def __init__(self, raw: dict) -> None:
        self._raw = raw
        self.retry_tiers: tuple[str, ...] = tuple(raw["retry_tiers"])
        self._specs = {
            spec["name"]: TopicSpec(
                name=spec["name"],
                partitions=spec["partitions"],
                retention_ms=spec["retention_ms"],
                produced_by=tuple(spec["produced_by"]),
                consumed_by=tuple(spec["consumed_by"]),
                retries=bool(spec.get("retries")),
            )
            for spec in raw["topics"]
        }

    @classmethod
    def load(cls, path: pathlib.Path | None = None) -> TopicRegistry:
        return cls(json.loads((path or REGISTRY_PATH).read_text()))

    def __getitem__(self, name: str) -> TopicSpec:
        return self._specs[name]

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    @property
    def declared(self) -> tuple[TopicSpec, ...]:
        return tuple(self._specs.values())

    def plan(self, profile: Profile = "dev") -> list[TopicPlan]:
        """Every topic that must exist, retry tiers and DLQs included.

        Derived rather than listed by hand so a new topic automatically gets its
        retry ladder, and the ladder can never fall out of step with ADR-0005.
        """
        settings = self._raw["profiles"][profile]
        rf = settings["replication_factor"]
        base_configs = {"min.insync.replicas": str(settings["min_insync_replicas"])}
        plans: list[TopicPlan] = []

        for spec in self._specs.values():
            partitions = spec.partitions[profile]
            plans.append(
                TopicPlan(spec.name, partitions, rf,
                          {**base_configs, "retention.ms": str(spec.retention_ms)})
            )
            if not spec.retries:
                continue
            # Retry tiers carry less traffic than the topic they serve, and a
            # DLQ should be receiving almost nothing at all.
            for tier in self.retry_tiers:
                plans.append(
                    TopicPlan(spec.retry_topic(tier), max(1, partitions // 2), rf,
                              {**base_configs, "retention.ms": str(spec.retention_ms)})
                )
            plans.append(
                TopicPlan(spec.dlq_topic, max(1, partitions // 4), rf,
                          {**base_configs, "retention.ms": str(DLQ_RETENTION_MS)})
            )
        return plans


REGISTRY = TopicRegistry.load()

# Ergonomic constants so services never type a topic name as a bare string.
VIDEO_UPLOADED = "video.uploaded"
VIDEO_PROBED = "video.probed"
RENDITION_REQUESTED = "rendition.requested"
RENDITION_COMPLETED = "rendition.completed"
VIDEO_COMPLETED = "video.completed"
VIDEO_STATUS = "video.status"
PIPELINE_FAILED = "pipeline.failed"

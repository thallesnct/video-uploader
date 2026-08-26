#!/usr/bin/env python3
"""Create and verify Kafka topics from infra/topics.json. Safe to re-run.

Auto-creation is disabled on the broker (ADR-0002), so this is the only way
topics come into existence. Retry and DLQ topics are derived from the registry
rather than listed by hand, so they can never fall out of step with ADR-0005.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "libs" / "pipeline" / "topics.json"
PROFILE = "dev"
BROKER = "localhost:9092"  # inside the kafka container


def kafka(*args: str, check: bool = True) -> str:
    """Run a kafka CLI tool inside the broker container."""
    cmd = ["docker", "compose", "exec", "-T", "kafka", *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if check and proc.returncode != 0:
        sys.exit(f"FAILED: {' '.join(args)}\n{proc.stderr.strip()}")
    return proc.stdout


def planned_topics(registry: dict) -> list[dict]:
    """Expand the registry into every topic that must exist, retries included."""
    rf = registry["profiles"][PROFILE]["replication_factor"]
    min_isr = registry["profiles"][PROFILE]["min_insync_replicas"]
    out: list[dict] = []
    for spec in registry["topics"]:
        out.append(
            {
                "name": spec["name"],
                "partitions": spec["partitions"][PROFILE],
                "replication_factor": rf,
                "configs": {
                    "retention.ms": str(spec["retention_ms"]),
                    "min.insync.replicas": str(min_isr),
                },
            }
        )
        if not spec.get("retries"):
            continue
        # Retry tiers are non-blocking (ADR-0005): a delayed message must never
        # hold up the partition it came from, so each tier is its own topic.
        for tier in registry["retry_tiers"]:
            out.append(
                {
                    "name": f"{spec['name']}.retry.{tier}",
                    "partitions": max(1, spec["partitions"][PROFILE] // 2),
                    "replication_factor": rf,
                    "configs": {
                        "retention.ms": str(spec["retention_ms"]),
                        "min.insync.replicas": str(min_isr),
                    },
                }
            )
        # DLQ keeps messages far longer than the pipeline: someone has to be able
        # to come back on Monday and replay them.
        out.append(
            {
                "name": f"{spec['name']}.dlq",
                "partitions": max(1, spec["partitions"][PROFILE] // 4),
                "replication_factor": rf,
                "configs": {
                    "retention.ms": str(90 * 24 * 3600 * 1000),
                    "min.insync.replicas": str(min_isr),
                },
            }
        )
    return out


def main() -> int:
    registry = json.loads(REGISTRY.read_text())
    planned = planned_topics(registry)

    existing = {
        line.strip()
        for line in kafka("kafka-topics", "--bootstrap-server", BROKER, "--list").splitlines()
        if line.strip() and not line.startswith("__")
    }

    created = 0
    for topic in planned:
        if topic["name"] in existing:
            continue
        args = [
            "kafka-topics",
            "--bootstrap-server",
            BROKER,
            "--create",
            "--if-not-exists",
            "--topic",
            topic["name"],
            "--partitions",
            str(topic["partitions"]),
            "--replication-factor",
            str(topic["replication_factor"]),
        ]
        for key, value in topic["configs"].items():
            args += ["--config", f"{key}={value}"]
        kafka(*args)
        created += 1
        print(
            f"  created {topic['name']} "
            f"(partitions={topic['partitions']}, rf={topic['replication_factor']})"
        )

    # Verify rather than trust: a topic created by an earlier run with the wrong
    # partition count is invisible until throughput silently caps.
    describe = kafka("kafka-topics", "--bootstrap-server", BROKER, "--describe")
    actual: dict[str, int] = {}
    for line in describe.splitlines():
        if line.startswith("Topic:") and "PartitionCount:" in line:
            parts = dict(
                p.split(":", 1) for p in (chunk.strip() for chunk in line.split("\t")) if ":" in p
            )
            actual[parts["Topic"].strip()] = int(parts["PartitionCount"].strip())

    drift = [
        (t["name"], t["partitions"], actual.get(t["name"]))
        for t in planned
        if actual.get(t["name"]) not in (None, t["partitions"])
    ]
    missing = [t["name"] for t in planned if t["name"] not in actual]

    print(
        f"topics: {len(planned)} declared, {created} created, "
        f"{len(planned) - created} already present"
    )
    if missing:
        print(f"MISSING after create: {missing}", file=sys.stderr)
    if drift:
        print("PARTITION DRIFT (declared vs actual):", file=sys.stderr)
        for name, want, got in drift:
            print(f"  {name}: want {want}, got {got}", file=sys.stderr)
        print(
            "Partitions can be increased but never decreased; to fix a topic "
            "that is too large, delete and recreate it.",
            file=sys.stderr,
        )
    return 1 if (missing or drift) else 0


if __name__ == "__main__":
    raise SystemExit(main())

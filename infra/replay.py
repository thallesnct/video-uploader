#!/usr/bin/env python3
"""Manual DLQ replay (Phase 11) — ADR-0005's own stated interface: "Replay
is a deliberate operator action (`make replay TOPIC=… VIDEO=…`), never
automatic."

Stdlib-only, matching every other infra/ script — bootstrap_topics.py
shells out to the broker's own `kafka-topics` CLI for the same reason
(AGENTS.md: infra/ is operator tooling, its own convention is worth keeping
unbroken even though replay, unlike bootstrap/smoke, runs against an
already-fully-running system, not a bare host).

Headers are NOT preserved on replay — deliberately, not an oversight: a
DLQ message's headers (retry_count, failure_reason, failure_class,
original_topic, failed_in_stage) all describe the failed attempt, none of
which should carry forward to a fresh one. retry_count implicitly resets
to 0 since MessageView.retry_count already treats a missing header as 0
(libs/pipeline/consumer.py) — the same fresh start a human's intervention
earns the message. Non-destructive: the DLQ keeps its own copy (Kafka's
own retention model, not something this script touches).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
BROKER = "localhost:9092"
# UUIDs (our keys) are hex digits and hyphens only — a tab can never collide.
KEY_SEP = "\t"


def compose(*args: str, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka", *args],
        capture_output=True,
        text=True,
        input=input_text,
        cwd=ROOT,
    )


def source_topic_of(topic: str) -> str:
    """Same logic as libs/pipeline/retry.py's function of the same name,
    duplicated rather than imported — infra/ stays dependency-free of the
    backend venv (AGENTS.md), the same reason bootstrap_topics.py reads
    topics.json directly instead of importing TopicRegistry."""
    topic = re.sub(r"\.retry\.\d+[smh]$", "", topic)
    return re.sub(r"\.dlq$", "", topic)


def consume_dlq(topic: str) -> list[tuple[str, str]]:
    """Every (key, value) currently on the DLQ topic. A bounded batch read
    (--timeout-ms), not a long-running subscription — kafka-console-consumer
    exits non-zero once the timeout fires with no new messages, which is the
    expected, successful end of this read, not an error; only stdout, already
    captured by then, is used."""
    proc = compose(
        "kafka-console-consumer",
        "--bootstrap-server",
        BROKER,
        "--topic",
        topic,
        "--from-beginning",
        "--timeout-ms",
        "5000",
        "--property",
        "print.key=true",
        "--property",
        f"key.separator={KEY_SEP}",
    )
    records = []
    for line in proc.stdout.splitlines():
        if KEY_SEP not in line:
            continue
        key, value = line.split(KEY_SEP, 1)
        records.append((key, value))
    return records


def replay(topic: str, video_id: str | None) -> int:
    source = source_topic_of(topic)
    records = consume_dlq(topic)
    matches = [(key, value) for key, value in records if video_id is None or key == video_id]

    if not matches:
        suffix = f" for video {video_id}" if video_id else ""
        print(f"no messages found on {topic}{suffix}")
        return 1

    lines = "\n".join(f"{key}{KEY_SEP}{value}" for key, value in matches) + "\n"
    proc = compose(
        "kafka-console-producer",
        "--bootstrap-server",
        BROKER,
        "--topic",
        source,
        "--property",
        "parse.key=true",
        "--property",
        f"key.separator={KEY_SEP}",
        input_text=lines,
    )
    if proc.returncode != 0:
        print(f"replay failed: {proc.stderr}", file=sys.stderr)
        return 1

    print(f"replayed {len(matches)} message(s) from {topic} to {source}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True, help="the DLQ topic, e.g. video.uploaded.dlq")
    parser.add_argument("--video-id", default=None, help="replay only this video's message(s)")
    args = parser.parse_args()

    if not args.topic.endswith(".dlq"):
        print(f"{args.topic!r} isn't a DLQ topic (expected a .dlq suffix)", file=sys.stderr)
        return 1

    return replay(args.topic, args.video_id)


if __name__ == "__main__":
    raise SystemExit(main())

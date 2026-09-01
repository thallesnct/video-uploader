#!/usr/bin/env python3
"""`make kafka-prod-verify` — Phase 12's Kafka prod-profile gate, rehearsed
once (confirmed with the user: no live deployment target). Brings up the
real 3-broker KRaft cluster (docker-compose.prod.yml's kafka-1/2/3), proves
an authenticated SASL_SSL client can create a topic that actually reports
RF=3/ISR=3 with min.insync.replicas=2 and unclean leader election off, and
proves an *unauthenticated* client is genuinely rejected — not just that
auth is configured, but that it does something. Tears the cluster down
unconditionally.

Runs under its own isolated `docker compose -p video-pipeline-kafka-prod`
project, matching infra/backup_verify.py's precedent: kafka-1/2/3 are new
service names (not overrides of the base file's single-node `kafka`), each
with its own container_name/volume, and none publish a host port at
all — every command below runs via `docker compose exec` against kafka-1,
so there's nothing to collide with the dev stack's published 29092 even if
it's running.

Requires infra/gen_kafka_certs.sh to have been run first (the `make
kafka-prod-verify` target runs it as a prerequisite) — the keystore/
truststore/JAAS config it generates gets bind-mounted into all three
brokers by docker-compose.prod.yml.

Stdlib-only (AGENTS.md) — no client library dependency; every check shells
out to the same `kafka-topics` CLI already bundled in the broker image.
"""

from __future__ import annotations

import pathlib
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAILURES: list[str] = []

PROJECT = "video-pipeline-kafka-prod"
BROKERS = ["kafka-1", "kafka-2", "kafka-3"]
CLIENT_CONFIG = "/etc/kafka/secrets/client.properties"
TOPIC = "kafka-prod-verify-topic"
CERTS_DIR = ROOT / "infra" / "kafka-certs"


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{f' — {detail}' if detail else ''}")
    if not ok:
        FAILURES.append(name)


def compose(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            PROJECT,
            "-f",
            "docker-compose.yml",
            "-f",
            "docker-compose.prod.yml",
            *args,
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=timeout,
    )


def kafka_topics(
    *args: str, authenticated: bool = True, timeout: int = 20
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "exec", "vp-kafka-1", "kafka-topics", "--bootstrap-server", "kafka-1:9094"]
    if authenticated:
        cmd += ["--command-config", CLIENT_CONFIG]
    cmd += list(args)
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            cmd, 1, exc.stdout or "", (exc.stderr or "") + "\n(timed out)"
        )


def main() -> int:
    if not (CERTS_DIR / "kafka.keystore.jks").exists():
        check(
            "certs generated (infra/gen_kafka_certs.sh)",
            False,
            f"missing {CERTS_DIR}/kafka.keystore.jks — run infra/gen_kafka_certs.sh first",
        )
        print(f"\nKAFKA-PROD-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1

    print(f"tearing down any stale {PROJECT!r} project from a previous failed run")
    compose("down", "-v")

    try:
        print("bringing up the 3-broker KRaft cluster (TLS+SASL on the client listener)")
        up = compose("up", "-d", *BROKERS, timeout=90)
        check("all three brokers start", up.returncode == 0, up.stderr.strip()[-500:])

        print("waiting for the quorum to settle and the internal listener to answer")
        api_versions = None
        for _ in range(30):
            api_versions = subprocess.run(
                [
                    "docker",
                    "exec",
                    "vp-kafka-1",
                    "kafka-broker-api-versions",
                    "--bootstrap-server",
                    "kafka-1:9092",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if api_versions.returncode == 0:
                break
            time.sleep(2)
        check(
            "internal (unauthenticated, inter-broker) listener answers",
            api_versions is not None and api_versions.returncode == 0,
            (api_versions.stderr.strip()[-300:] if api_versions else "never became ready"),
        )

        for name in BROKERS:
            status = subprocess.run(
                ["docker", "inspect", f"vp-{name}", "--format", "{{.State.Status}}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            check(
                f"{name} container is still running (not crash-looped)",
                status.stdout.strip() == "running",
            )

        print("creating a real topic through the authenticated SASL_SSL listener")
        created = kafka_topics(
            "--create", "--topic", TOPIC, "--partitions", "3", "--replication-factor", "3"
        )
        check(
            "topic created via an authenticated SASL_SSL client",
            created.returncode == 0,
            created.stderr.strip()[-300:] or created.stdout.strip()[-300:],
        )

        print("describing it back — this is the actual gate: RF=3, ISR=3, min.insync.replicas=2")
        described = kafka_topics("--describe", "--topic", TOPIC)
        output = described.stdout
        check("describe succeeds", described.returncode == 0, described.stderr.strip()[-300:])
        check("ReplicationFactor: 3", "ReplicationFactor: 3" in output, output[:200])
        check("min.insync.replicas=2", "min.insync.replicas=2" in output, output[:200])
        check(
            "unclean.leader.election.enable=false",
            "unclean.leader.election.enable=false" in output,
            output[:200],
        )
        isr_lines = [line for line in output.splitlines() if "Partition:" in line]
        all_isr_3 = bool(isr_lines) and all(
            len(line.split("Isr:")[-1].strip().split(",")) == 3 for line in isr_lines
        )
        check(
            "every partition's ISR has all 3 replicas in sync",
            all_isr_3,
            "; ".join(isr_lines) if isr_lines else "no partition lines found",
        )

        print("confirming an unauthenticated client is genuinely rejected, not just unconfigured")
        rejected = kafka_topics("--list", authenticated=False, timeout=10)
        check(
            "an unauthenticated client cannot list topics on the SASL_SSL listener",
            rejected.returncode != 0,
            "client succeeded without credentials — auth is not actually enforced"
            if rejected.returncode == 0
            else "rejected as expected",
        )
    finally:
        print("tearing down the isolated project unconditionally")
        compose("down", "-v")

    print()
    if FAILURES:
        print(f"KAFKA-PROD-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("KAFKA-PROD-VERIFY PASSED — a real 3-broker cluster, RF=3/ISR=3, auth genuinely enforced")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

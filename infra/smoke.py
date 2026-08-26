#!/usr/bin/env python3
"""Phase 1 gate: prove every dependency is actually usable, not merely running.

'Container is up' is not the same as 'a client can use it'. Each check here is
one that has already failed in practice or is called out in an ADR: the broker
bound to the wrong interface, a topic missing because auto-create is off, or
MinIO refusing the browser's preflight because CORS was never configured.

Deliberately dependency-free — it runs on a bare host with only Docker.
"""

from __future__ import annotations

import json
import pathlib
import socket
import subprocess
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAILURES: list[str] = []


def env() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{f' — {detail}' if detail else ''}")
    if not ok:
        FAILURES.append(name)


def port_open(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def compose(*args: str) -> tuple[int, str]:
    proc = subprocess.run(["docker", "compose", *args], capture_output=True, text=True, cwd=ROOT)
    return proc.returncode, proc.stdout + proc.stderr


def main() -> int:
    cfg = env()
    print("kafka")
    kafka_port = int(cfg.get("KAFKA_EXTERNAL_PORT", "29092"))
    # The external listener is what tests and host tooling dial; a broker bound
    # only to its container hostname passes a healthcheck and still fails here.
    check(
        f"external listener reachable on localhost:{kafka_port}", port_open("localhost", kafka_port)
    )

    registry = json.loads((ROOT / "libs" / "pipeline" / "topics.json").read_text())
    code, out = compose(
        "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--describe"
    )
    actual: dict[str, int] = {}
    for line in out.splitlines():
        if line.startswith("Topic:") and "PartitionCount:" in line:
            fields = dict(
                chunk.split(":", 1)
                for chunk in (c.strip() for c in line.split("\t"))
                if ":" in chunk
            )
            actual[fields["Topic"].strip()] = int(fields["PartitionCount"].strip())
    check("topic list readable", code == 0)

    wrong = []
    for spec in registry["topics"]:
        want = spec["partitions"]["dev"]
        got = actual.get(spec["name"])
        if got != want:
            wrong.append(f"{spec['name']} want={want} got={got}")
    check(
        f"{len(registry['topics'])} declared topics exist with the right partition counts",
        not wrong,
        "; ".join(wrong),
    )

    dlq_missing = [
        f"{s['name']}.dlq"
        for s in registry["topics"]
        if s.get("retries") and f"{s['name']}.dlq" not in actual
    ]
    check("retry and DLQ topics exist", not dlq_missing, ", ".join(dlq_missing))

    # ADR-0002: a typo must fail loudly rather than conjure a dead topic.
    code, out = compose(
        "exec",
        "-T",
        "kafka",
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--topic",
        "smoke.should.not.autocreate",
    )
    check("auto topic creation is disabled", code != 0)

    print("postgres")
    pg_port = int(cfg.get("POSTGRES_PORT", "5432"))
    check(f"accepting connections on localhost:{pg_port}", port_open("localhost", pg_port))
    code, out = compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        cfg["POSTGRES_USER"],
        "-d",
        cfg["POSTGRES_DB"],
        "-tAc",
        "select 1",
    )
    check("query executes", code == 0 and "1" in out, out.strip()[:80] if code else "")

    print("minio")
    api = cfg.get("S3_ENDPOINT", "http://localhost:9000")
    bucket = cfg.get("S3_BUCKET", "videos")
    try:
        with urllib.request.urlopen(f"{api}/minio/health/live", timeout=5) as resp:
            live = resp.status == 200
    except (urllib.error.URLError, TimeoutError):
        live = False
    check("health endpoint live", live)

    code, out = compose(
        "run",
        "--rm",
        "-T",
        "mc",
        f'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" '
        f'"$MINIO_ROOT_PASSWORD" >/dev/null && mc ls local/{bucket}',
    )
    check(f"bucket '{bucket}' exists", code == 0)

    # ADR-0006 calls missing CORS the classic first-run failure: the browser PUTs
    # straight to the object store, so a failed preflight breaks every upload
    # while every server-side test still passes.
    origin = cfg.get("MINIO_API_CORS_ALLOW_ORIGIN", "").split(",")[0].strip()
    allow = ""
    if origin:
        request = urllib.request.Request(
            f"{api}/{bucket}/smoke-preflight-probe",
            method="OPTIONS",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "content-type",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as resp:
                allow = resp.headers.get("Access-Control-Allow-Origin", "")
        except urllib.error.HTTPError as exc:
            allow = exc.headers.get("Access-Control-Allow-Origin", "")
        except (urllib.error.URLError, TimeoutError):
            allow = ""
    check(
        f"CORS preflight allows browser origin {origin}",
        allow in (origin, "*"),
        f"Access-Control-Allow-Origin: {allow or '(absent)'}",
    )

    print()
    if FAILURES:
        print(f"SMOKE FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("SMOKE PASSED — Kafka, Postgres and MinIO are usable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

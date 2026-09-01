#!/usr/bin/env python3
"""Phase 12's own gate: `make security-verify`. Images scan clean at the
agreed severity, containers run as non-root with a read-only rootfs, and an
egress attempt from a media worker to an arbitrary host fails.

Stdlib-only, matching every other infra/ script (AGENTS.md) — shells out to
`docker`/`docker compose`/`trivy` rather than adding a Python client for any
of them.

Trivy severity gate, decided before seeing any output (not adjusted after
the fact to make it green, per this repo's own discipline): HIGH/CRITICAL,
with --ignore-unfixed. A fixable HIGH/CRITICAL finding is real, actionable
signal; an unfixed one (no upstream patch exists yet) would fail this gate
forever regardless of anything this project does, which makes it noise, not
signal, for a gate meant to be re-run and trusted.
"""

from __future__ import annotations

import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAILURES: list[str] = []

# Only images this project builds and ships — a CVE in postgres:16-alpine or
# minio/minio:latest isn't something a Dockerfile change here can fix; that's
# a vendored dependency's own release cadence, not this gate's job to police.
OUR_IMAGES = [
    "video-pipeline-api",
    "video-pipeline-devauth",
    "video-pipeline-webhook-sink",
    "video-pipeline-worker-probe",
    "video-pipeline-worker-transcode",
    "video-pipeline-worker-thumbnail",
    "video-pipeline-worker-package",
    "video-pipeline-worker-notify",
    "video-pipeline-worker-retry-pump",
    "video-pipeline-projector",
]

# Known, accepted findings — allow-listed by exact reason, not by turning
# down the severity gate. devauth's signing key is a dev/test-only mock
# OIDC issuer's key (services/devauth/), never used for anything but
# minting local test tokens, its own filename already says "insecure", and
# it is already committed to git — this project's own established
# convention already treats it as a fixture, not a secret. Trivy is right
# to flag "a private key baked into an image" as a class of finding worth
# catching; this is the one place it's correct to do that and still be a
# false positive for what this key actually is.
ALLOWED_SECRET_PATHS = {"/app/services/devauth/dev-only-insecure-signing-key.pem"}

# Compose service names for OUR_IMAGES, in the same order — needed because
# `docker compose ps` (even scoped with --profile app) lists every running
# container in the whole project, not just app-profile ones, once anything
# else (kafka/postgres/minio with no profile, or the separate obs-stack
# file) is already running alongside. Caught running this for real: the
# first version of this check inspected vp-kafka/vp-postgres/vp-minio/
# vp-grafana/vp-tempo/vp-kafka-exporter/vp-otel too — third-party images
# this project doesn't build, same reasoning OUR_IMAGES already excludes
# them from the Trivy scan for. Explicit service names, not profile
# filtering, is the only way to scope this to what we actually ship.
OUR_SERVICES = [
    "api",
    "devauth",
    "webhook-sink",
    "worker-probe",
    "worker-transcode",
    "worker-thumbnail",
    "worker-package",
    "worker-notify",
    "worker-retry-pump",
    "projector",
]

MEDIA_WORKERS = ["worker-probe", "worker-transcode", "worker-thumbnail"]
# What these workers are still expected to reach — the point of the
# no-egress network isn't "reach nothing", it's "reach nothing but this".
ALLOWED_TARGETS = [("kafka", 9092), ("minio", 9000)]


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{f' — {detail}' if detail else ''}")
    if not ok:
        FAILURES.append(name)


def compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--profile", "app", *args],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )


def check_trivy() -> None:
    for image in OUR_IMAGES:
        proc = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                "/var/run/docker.sock:/var/run/docker.sock",
                "aquasec/trivy:latest",
                "image",
                "--severity",
                "HIGH,CRITICAL",
                "--ignore-unfixed",
                "--format",
                "json",
                "--timeout",
                "5m",
                image,
            ],
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        if proc.returncode != 0 and not proc.stdout.strip():
            check(f"trivy scans {image}", False, proc.stderr.strip()[:300] or "trivy itself failed")
            continue

        try:
            report = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            check(f"trivy scans {image}", False, f"unparseable trivy output: {exc}")
            continue

        findings: list[str] = []
        for result in report.get("Results", []):
            target = result.get("Target", "")
            for vuln in result.get("Vulnerabilities", []) or []:
                findings.append(f"{target}: {vuln.get('VulnerabilityID')} ({vuln.get('Severity')})")
            for secret in result.get("Secrets", []) or []:
                # For a "secret" result, Target *is* the file path inside the image.
                if target in ALLOWED_SECRET_PATHS:
                    continue
                findings.append(f"{target}: unallowed secret finding ({secret.get('Title')})")

        check(
            f"trivy: {image} has no unallowed HIGH/CRITICAL findings",
            not findings,
            "; ".join(findings)[:500],
        )


def check_non_root_and_readonly() -> None:
    proc = compose("ps", "-q", *OUR_SERVICES)
    container_ids = [line for line in proc.stdout.splitlines() if line.strip()]
    if len(container_ids) != len(OUR_SERVICES):
        check(
            f"all {len(OUR_SERVICES)} of our services are running to inspect",
            False,
            f"found {len(container_ids)}",
        )
        return

    fmt = "{{.Name}}|{{.Config.User}}|{{.HostConfig.ReadonlyRootfs}}"
    for cid in container_ids:
        inspect = subprocess.run(
            ["docker", "inspect", cid, "--format", fmt],
            capture_output=True,
            text=True,
        )
        if inspect.returncode != 0:
            check(f"inspect {cid}", False, inspect.stderr.strip()[:200])
            continue
        name, user, readonly = inspect.stdout.strip().split("|")
        name = name.lstrip("/")
        non_root = bool(user) and user not in ("root", "0")
        check(f"{name} runs as non-root", non_root, f"User={user!r}")
        check(f"{name} has a read-only rootfs", readonly == "true", f"ReadonlyRootfs={readonly}")


def check_egress_denied() -> None:
    probe_script = (
        "import urllib.request\n"
        "try:\n"
        "    urllib.request.urlopen('https://example.com', timeout=3)\n"
        "    print('REACHED')\n"
        "except Exception as exc:\n"
        "    print(f'BLOCKED:{type(exc).__name__}')\n"
    )
    for worker in MEDIA_WORKERS:
        proc = compose("exec", "-T", worker, "python3", "-c", probe_script)
        blocked = proc.returncode == 0 and proc.stdout.strip().startswith("BLOCKED")
        check(
            f"{worker} cannot reach an arbitrary external host",
            blocked,
            proc.stdout.strip() or proc.stderr.strip()[:200],
        )

        for host, port in ALLOWED_TARGETS:
            reach_script = (
                f"import socket\n"
                f"try:\n"
                f"    socket.create_connection(('{host}', {port}), timeout=3).close()\n"
                f"    print('REACHED')\n"
                f"except Exception as exc:\n"
                f"    print(f'FAILED:{{type(exc).__name__}}')\n"
            )
            proc = compose("exec", "-T", worker, "python3", "-c", reach_script)
            reached = proc.returncode == 0 and proc.stdout.strip() == "REACHED"
            check(f"{worker} can still reach {host}:{port}", reached, proc.stdout.strip())


def main() -> int:
    print("scanning our own images with trivy (HIGH/CRITICAL, --ignore-unfixed)")
    check_trivy()

    print("every running container: non-root, read-only rootfs")
    check_non_root_and_readonly()

    print("media workers: no egress beyond kafka/minio, real dependencies still reachable")
    check_egress_denied()

    print()
    if FAILURES:
        print(f"SECURITY-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("SECURITY-VERIFY PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

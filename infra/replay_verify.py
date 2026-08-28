#!/usr/bin/env python3
"""Phase 11 gate, third leg: prove `make replay` drives a real video from
the DLQ to completion, not just that it moves bytes between topics (that
narrower claim is already covered by the manual verification recorded in
replay.py's own commit message).

Why this can't be a corrupt-upload-and-replay-it test: worker_probe's
ffprobe failure on unparseable input is TerminalError — it fails
*identically* every time, so replaying the exact same bad bytes can never
reach "completed". The only scenario where a DLQ replay is meaningful is
the real-world one it exists for: a message that was dead-lettered for a
reason since resolved, whose underlying data was fine all along. This
script builds exactly that: a real, valid upload whose video.uploaded
never goes to the live topic at all — it's seeded straight onto the DLQ,
as if some now-fixed condition had dead-lettered it — then replayed for
real via the shipped CLI (a subprocess call to replay.py, not a
reimplementation of its logic).

Also why this isn't a Playwright spec (tests/e2e/failure-and-notify.spec.ts
covers the other two legs of this phase's gate): `make replay` is a host
CLI action by ADR-0005's own stated design, not an HTTP endpoint the
Playwright container could reach — this deliberately drives the real
Makefile-invoked script instead. Stdlib-only, matching every other infra/
script (AGENTS.md).
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAILURES: list[str] = []

API = "http://localhost:8000"
DEVAUTH = "http://localhost:8080"
BROKER = "localhost:9092"
KEY_SEP = "\t"  # same convention as replay.py — UUID keys can never contain a tab

FIXTURE = ROOT / "tests" / "e2e" / ".fixtures" / "testsrc-640x360.mp4"


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{f' — {detail}' if detail else ''}")
    if not ok:
        FAILURES.append(name)


def get_json(url: str, headers: dict[str, str] | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, {}
    except (urllib.error.URLError, TimeoutError):
        return 0, {}


def post_json(url: str, payload: dict, headers: dict[str, str] | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode()
    all_headers = {"Content-Type": "application/json", **(headers or {})}
    request = urllib.request.Request(url, method="POST", data=data, headers=all_headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as resp:
            body = resp.read()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        return exc.code, {}


def produce(topic: str, key: str, value: dict) -> bool:
    """Same shape as replay.py's own compose() helper, duplicated rather
    than imported for the same reason: infra/ stays free of the backend
    venv (AGENTS.md)."""
    line = f"{key}{KEY_SEP}{json.dumps(value)}\n"
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            BROKER,
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            f"key.separator={KEY_SEP}",
        ],
        input=line,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return proc.returncode == 0


def seed_dlq_message() -> tuple[str, str] | None:
    """Presign + PUT the real fixture (a genuinely valid file — this is the
    point), but never call /complete: instead of letting the API publish
    video.uploaded, seed the exact same event straight onto
    video.uploaded.dlq, as if a now-resolved condition had dead-lettered
    it. Also publishes the VideoStatusChanged the real /complete would
    have, so the video legitimately shows "uploaded" and stays there —
    nothing else is subscribed to make it move until the replay happens.
    Returns (video_id, owner_id), or None if any step failed."""
    status, body = post_json(f"{DEVAUTH}/token", {"sub": "replay-verify"})
    check("devauth issues a token", status == 200, f"status={status}")
    if status != 200:
        return None
    token = body["access_token"]
    owner_id = "replay-verify"  # Principal.owner_id is the bare JWT sub, no transform
    auth = {"Authorization": f"Bearer {token}"}

    status, body = post_json(
        f"{API}/videos",
        {
            "filename": "replay-verify.mp4",
            "content_type": "video/mp4",
            "size_bytes": FIXTURE.stat().st_size,
        },
        auth,
    )
    check("POST /videos", status == 201, f"status={status}")
    if status != 201:
        return None
    video_id = body["video_id"]
    object_key = body["object_key"]
    upload_url = body["upload_url"]

    put = urllib.request.Request(
        upload_url, method="PUT", data=FIXTURE.read_bytes(), headers={"Content-Type": "video/mp4"}
    )
    try:
        with urllib.request.urlopen(put, timeout=15) as resp:
            put_ok = resp.status == 200
    except (urllib.error.HTTPError, urllib.error.URLError):
        put_ok = False
    check("PUT the real fixture to the presigned URL", put_ok)
    if not put_ok:
        return None

    now = datetime.now(UTC).isoformat()
    uploaded_event = {
        "type": "video.uploaded",
        "event_id": str(uuid.uuid4()),
        "video_id": video_id,
        "owner_id": owner_id,
        "occurred_at": now,
        "schema_version": 1,
        "producer": "replay-verify",
        "object_key": object_key,
        "filename": "replay-verify.mp4",
        "size_bytes": FIXTURE.stat().st_size,
        "content_type": "video/mp4",
    }
    dlq_ok = produce("video.uploaded.dlq", video_id, uploaded_event)
    check("seed video.uploaded.dlq with a valid-file message", dlq_ok)
    if not dlq_ok:
        return None

    status_event = {
        "type": "video.status",
        "event_id": str(uuid.uuid4()),
        "video_id": video_id,
        "owner_id": owner_id,
        "occurred_at": now,
        "schema_version": 1,
        "producer": "replay-verify",
        "state": "uploaded",
    }
    status_ok = produce("video.status", video_id, status_event)
    check("publish video.status=uploaded (what /complete would have)", status_ok)
    if not status_ok:
        return None

    return video_id, owner_id


def check_stuck_before_replay(video_id: str, auth: dict[str, str]) -> None:
    """Nothing consumes video.uploaded.dlq on its own — confirm the video
    is genuinely stuck, not already progressing some other way, before
    crediting the replay for anything.

    Polls rather than checking once: the video.status=uploaded event just
    seeded still has to reach the projector's consumer, which can lag a
    couple of seconds behind the produce call, especially right after the
    stack itself just restarted (observed for real running this against a
    freshly-restarted compose stack — a single-shot check read the row
    before the projector had caught up and failed on a status that was
    correct, just not yet applied)."""
    status = None
    for _ in range(10):
        code, body = get_json(f"{API}/videos/{video_id}", auth)
        if code == 200:
            status = body.get("status")
            if status == "uploaded":
                break
        time.sleep(1)
    check(
        "video is stuck at uploaded before replay (DLQ isn't auto-drained)",
        status == "uploaded",
        f"status={status!r}",
    )


def run_replay(video_id: str) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "infra" / "replay.py"),
            "--topic",
            "video.uploaded.dlq",
            "--video-id",
            video_id,
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    check(
        "make replay's own CLI (infra/replay.py) exits 0",
        proc.returncode == 0,
        proc.stdout.strip() or proc.stderr.strip(),
    )


def wait_for_completion(video_id: str, auth: dict[str, str]) -> None:
    final_status = None
    for _ in range(60):
        code, body = get_json(f"{API}/videos/{video_id}", auth)
        if code == 200:
            final_status = body.get("status")
            if final_status in ("completed", "failed"):
                break
        time.sleep(2)
    check(
        "replayed video reaches completed (not failed, not stuck)",
        final_status == "completed",
        f"got {final_status!r}",
    )


def main() -> int:
    if not FIXTURE.exists():
        check(f"fixture present at {FIXTURE}", False, "run `make e2e` once first to extract it")
        return 1

    print("seeding a real, valid upload straight onto video.uploaded.dlq")
    seeded = seed_dlq_message()
    if seeded is None:
        print()
        print(f"REPLAY-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    video_id, owner_id = seeded

    status, body = post_json(f"{DEVAUTH}/token", {"sub": owner_id})
    auth = {"Authorization": f"Bearer {body.get('access_token', '')}"}

    print("confirming it's genuinely stuck, not progressing on its own")
    check_stuck_before_replay(video_id, auth)

    print("running the real `make replay` CLI")
    run_replay(video_id)

    print("polling for completion")
    wait_for_completion(video_id, auth)

    print()
    if FAILURES:
        print(f"REPLAY-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("REPLAY-VERIFY PASSED — a DLQ'd valid-payload message reached completed via replay")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

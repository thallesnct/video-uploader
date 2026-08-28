#!/usr/bin/env python3
"""Phase 10 gate: prove observability actually works, not just that the
containers are up. `make obs-verify`.

Drives one real upload through the running stack, then checks the three
things ADR-0010 and this phase's gate actually promise:

1. Dashboards provision with no manual setup (Grafana's own search API).
2. One uploaded video yields a single trace spanning every stage (Tempo's
   search API, by the video_id span attribute — not assumed, queried).
3. The lag panel has real data (Prometheus's query API).

Deliberately dependency-free, same as infra/smoke.py — runs on a bare host
with only Docker and the standard library.
"""

from __future__ import annotations

import json
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAILURES: list[str] = []

API = "http://localhost:8000"
DEVAUTH = "http://localhost:8080"
PROMETHEUS = "http://localhost:9090"
TEMPO = "http://localhost:3200"
GRAFANA = "http://localhost:3000"

FIXTURE = ROOT / "tests" / "e2e" / ".fixtures" / "testsrc-640x360.mp4"

EXPECTED_DASHBOARDS = {"Pipeline Overview", "Stage Detail", "Kafka Health", "Infrastructure"}
# The trace this phase's gate cares about: "API -> probe -> transcode ->
# package" (PROGRESS.md's exact wording). worker-thumbnail also appears in
# practice (a sibling branch off video.probed) but isn't part of the gate's
# named chain, so it isn't required here. worker_package's own SERVICE
# constant is "package", not "worker-package" — verified against a real
# trace this session, not assumed.
REQUIRED_SERVICES = {"api", "worker-probe", "worker-transcode", "package"}


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


def drive_one_upload() -> str | None:
    """Mint a token, upload the e2e fixture, wait for completion. Returns
    the video_id, or None if any step failed (already checked/reported)."""
    status, body = post_json(f"{DEVAUTH}/token", {"sub": "obs-verify"})
    check("devauth issues a token", status == 200, f"status={status}")
    if status != 200:
        return None
    token = body["access_token"]
    auth = {"Authorization": f"Bearer {token}"}

    status, body = post_json(
        f"{API}/videos",
        {
            "filename": "obs-verify.mp4",
            "content_type": "video/mp4",
            "size_bytes": FIXTURE.stat().st_size,
        },
        auth,
    )
    check("POST /videos", status == 201, f"status={status}")
    if status != 201:
        return None
    video_id = body["video_id"]
    upload_url = body["upload_url"]

    put = urllib.request.Request(
        upload_url, method="PUT", data=FIXTURE.read_bytes(), headers={"Content-Type": "video/mp4"}
    )
    try:
        with urllib.request.urlopen(put, timeout=15) as resp:
            put_ok = resp.status == 200
    except (urllib.error.HTTPError, urllib.error.URLError):
        put_ok = False
    check("PUT fixture bytes to the presigned URL", put_ok)
    if not put_ok:
        return None

    status, _ = post_json(f"{API}/videos/{video_id}/complete", {}, auth)
    check("POST /videos/{id}/complete", status == 200, f"status={status}")
    if status != 200:
        return None

    final_status = None
    for _ in range(60):
        code, body = get_json(f"{API}/videos/{video_id}", auth)
        if code == 200:
            final_status = body.get("status")
            if final_status in ("completed", "failed"):
                break
        time.sleep(2)
    check("video reaches completed", final_status == "completed", f"got {final_status!r}")
    return video_id if final_status == "completed" else None


def check_trace(video_id: str) -> None:
    query = urllib.parse.urlencode({"tags": f"video_id={video_id}"})
    code, body = get_json(f"{TEMPO}/api/search?{query}")
    traces = body.get("traces", [])
    check(f"Tempo has at least one trace tagged video_id={video_id}", code == 200 and bool(traces))
    if not traces:
        return

    # Multiple traces carry this tag — every request the video ever
    # touched (status polls, the initial POST /videos) gets one, alongside
    # the trace that actually covers the pipeline work. Don't guess by
    # reported duration (it doesn't reliably reflect when the last async,
    # Kafka-consumer-side span finished) — fetch every candidate and use
    # whichever one actually covers every required service.
    best_trace_id = traces[0]["traceID"]
    best_services: set[str] = set()
    for candidate in traces:
        trace_id = candidate["traceID"]
        _, full = get_json(f"{TEMPO}/api/traces/{trace_id}")
        services: set[str] = set()
        for batch in full.get("batches", []):
            for attr in batch.get("resource", {}).get("attributes", []):
                if attr["key"] == "service.name":
                    services.add(attr["value"].get("stringValue"))
        if services.issuperset(REQUIRED_SERVICES):
            best_trace_id, best_services = trace_id, services
            break
        if len(services) > len(best_services):
            best_trace_id, best_services = trace_id, services

    missing = REQUIRED_SERVICES - best_services
    check(
        f"trace {best_trace_id} spans every required stage",
        not missing,
        f"missing={sorted(missing)}, present={sorted(best_services)}" if missing else "",
    )


def check_lag_panel() -> None:
    code, body = get_json(f"{PROMETHEUS}/api/v1/query?query=kafka_consumergroup_lag_sum")
    result = body.get("data", {}).get("result", [])
    check("kafka_consumergroup_lag_sum returns a non-empty series", code == 200 and bool(result))


def check_dashboards() -> None:
    code, body = get_json(f"{GRAFANA}/api/search?type=dash-db")
    titles = {d["title"] for d in body} if isinstance(body, list) else set()
    missing = EXPECTED_DASHBOARDS - titles
    check(
        "all four dashboards provisioned with no manual setup",
        code == 200 and not missing,
        f"missing={sorted(missing)}" if missing else "",
    )


def main() -> int:
    if not FIXTURE.exists():
        check(f"fixture present at {FIXTURE}", False, "run via `make obs-verify`, not directly")
        return 1

    print("driving one real upload through the stack")
    video_id = drive_one_upload()

    print("tracing (ADR-0010): one video, one trace, every stage")
    if video_id:
        check_trace(video_id)
    else:
        check("trace check skipped", False, "no completed video to trace")

    print("metrics: the lag panel has real data")
    check_lag_panel()

    print("dashboards: provisioned with no manual setup")
    check_dashboards()

    print()
    if FAILURES:
        print(f"OBS-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("OBS-VERIFY PASSED — dashboards provisioned, one trace per video, lag panel is live")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

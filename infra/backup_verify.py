#!/usr/bin/env python3
"""`make backup-verify` — Phase 12's Postgres PITR gate: prove a real
point-in-time recovery works, not just that `pg_basebackup` runs without
error. Writes two rows, notes the exact server timestamp between them,
destroys the live data directory, restores from a base backup, replays WAL
up to that timestamp, and asserts the row before it survived while the row
after it didn't — the only way to actually prove "recovers to a specific
point" rather than "a backup exists".

Runs against an **isolated** `docker compose -p video-pipeline-pitr`
project (docker-compose.yml + docker-compose.prod.yml's postgres override),
never the default project this session's dev stack uses — this script
destroys a data directory on purpose, so it must be structurally
impossible for that to be the shared dev `pg-data` volume. `container_name`
is overridden to `vp-postgres-pitr` (distinct from the dev stack's
`vp-postgres`) and `POSTGRES_PORT` is overridden in this script's own
subprocess environment (docker-compose.prod.yml's own comment explains why
an override *inside* that file can't remove the base file's port mapping —
Compose concatenates list fields like `ports` across files rather than
replacing them) so this can never collide with the dev stack's published
5432 whether or not that's currently running. Torn down unconditionally in
`finally`, matching every other `*_verify.py` in this family.

Stdlib-only (AGENTS.md) except for shelling out to the `migrate` compose
service for schema setup — reusing the exact one-shot service `make
migrate` already runs, not a hand-rolled alembic invocation.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAILURES: list[str] = []

PROJECT = "video-pipeline-pitr"
CONTAINER = "vp-postgres-pitr"
ISOLATED_PORT = "55432"  # never actually connected to; see module docstring


def env() -> dict[str, str]:
    """Same shape as smoke.py's own env() — duplicated rather than shared
    for the same reason infra/ stays free of the backend venv (AGENTS.md)."""
    values: dict[str, str] = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


ENV = env()
PG_USER = ENV.get("POSTGRES_USER", "videos")
PG_DB = ENV.get("POSTGRES_DB", "videos")


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
        env={**os.environ, "POSTGRES_PORT": ISOLATED_PORT},
        timeout=timeout,
    )


def psql(sql: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "docker",
            "exec",
            "-u",
            "postgres",
            CONTAINER,
            "psql",
            "-U",
            PG_USER,
            "-d",
            PG_DB,
            "-t",
            "-A",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )


def docker_exec(
    *args: str, user: str = "postgres", timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "exec", "-u", user, CONTAINER, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def insert_video(filename: str, object_key: str) -> bool:
    # Both arguments are this script's own two hardcoded literals
    # ("keep.mp4"/"drop.mp4" below), never external input — a real
    # injection vector needs untrusted data, which this script never
    # touches.
    query = f"insert into videos (id, owner_id, filename, content_type, declared_size_bytes, object_key, status) values (gen_random_uuid(), 'pitr-verify', '{filename}', 'video/mp4', 100, '{object_key}', 'uploaded');"  # noqa: S608, E501
    result = psql(query)
    return result.returncode == 0


def rows_present() -> list[str]:
    result = psql("select filename from videos order by created_at;")
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def main() -> int:
    print(f"tearing down any stale {PROJECT!r} project from a previous failed run")
    compose("down", "-v")

    print("bringing up an isolated Postgres with archive_mode on")
    up = compose("up", "-d", "--wait", "postgres")
    check("isolated postgres starts healthy", up.returncode == 0, up.stderr.strip()[-500:])
    if up.returncode != 0:
        compose("down", "-v")
        print(f"\nBACKUP-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1

    try:
        archive_mode = psql("select current_setting('archive_mode');")
        check(
            "archive_mode is genuinely on (not just configured)",
            archive_mode.stdout.strip() == "on",
            archive_mode.stdout.strip() or archive_mode.stderr.strip(),
        )

        # Named volumes start root-owned; archive_command runs as the
        # postgres user and would otherwise fail silently on every attempt
        # (found rehearsing this by hand — pg_switch_wal "succeeded" but
        # /archive stayed empty). This is provisioning the rehearsal
        # environment, not something a real deployment's image/volume setup
        # would need — a real one would bake the ownership in.
        docker_exec("chown", "postgres:postgres", "/archive", user="root")

        print("running the real migrate service against it (schema, no data)")
        migrated = compose("run", "--rm", "migrate", timeout=180)
        check(
            "schema migrated onto the isolated instance",
            migrated.returncode == 0,
            migrated.stdout[-500:],
        )

        print("taking a base backup (no data yet — the recovery floor)")
        docker_exec("psql", "-U", PG_USER, "-d", PG_DB, "-c", "select pg_switch_wal();")
        backup = docker_exec(
            "pg_basebackup",
            "-U",
            PG_USER,
            "-D",
            "/archive/basebackup",
            "-Fp",
            "-Xnone",
            timeout=120,
        )
        check("pg_basebackup succeeds", backup.returncode == 0, backup.stderr.strip()[-300:])

        print("writing the row that must survive recovery")
        check("insert 'keep' row", insert_video("keep.mp4", "owners/pitr-verify/keep"))
        target_time_result = psql("select clock_timestamp();")
        target_time = target_time_result.stdout.strip()
        check("captured a recovery_target_time from the server's own clock", bool(target_time))

        print("writing the row that must NOT survive recovery")
        check("insert 'drop' row", insert_video("drop.mp4", "owners/pitr-verify/drop"))
        docker_exec("psql", "-U", PG_USER, "-d", PG_DB, "-c", "select pg_switch_wal();")
        time.sleep(1)  # let the archiver's cp actually run before we stop the server

        before = rows_present()
        check(
            "both rows present before restore (sanity — this isn't the proof)",
            before == ["keep.mp4", "drop.mp4"],
            f"got {before!r}",
        )

        print("stopping postgres and destroying its live data directory")
        stopped = compose("stop", "postgres")
        check("postgres stops cleanly", stopped.returncode == 0)

        restore_script = f"""set -e
rm -rf /var/lib/postgresql/data/*
cp -a /archive/basebackup/. /var/lib/postgresql/data/
rm -f /var/lib/postgresql/data/postmaster.pid
touch /var/lib/postgresql/data/recovery.signal
cat >> /var/lib/postgresql/data/postgresql.auto.conf <<CONF
restore_command = 'cp /archive/%f %p'
recovery_target_time = '{target_time}'
recovery_target_action = 'promote'
CONF
chown -R postgres:postgres /var/lib/postgresql/data
chmod 700 /var/lib/postgresql/data
"""
        restored = compose(
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            "postgres",
            "-c",
            restore_script,
            timeout=60,
        )
        check(
            "data directory replaced with the base backup + recovery config",
            restored.returncode == 0,
            restored.stderr.strip()[-300:],
        )

        print("starting postgres — it should replay WAL up to the target time and promote")
        restart = compose("up", "-d", "--wait", "postgres", timeout=120)
        check(
            "postgres comes back healthy after recovery",
            restart.returncode == 0,
            restart.stderr.strip()[-500:],
        )

        after = rows_present()
        check(
            "'keep' row survived — recovery reached the target time",
            "keep.mp4" in after,
            f"got {after!r}",
        )
        check(
            "'drop' row did NOT survive — recovery stopped before it, "
            'not just "restored something"',
            "drop.mp4" not in after,
            f"got {after!r}",
        )
    finally:
        print("tearing down the isolated project unconditionally")
        compose("down", "-v")

    print()
    if FAILURES:
        print(f"BACKUP-VERIFY FAILED — {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("BACKUP-VERIFY PASSED — a real point-in-time recovery reached exactly the target time")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

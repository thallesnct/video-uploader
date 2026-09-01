# ADR-0015: Production readiness — security, operations and deployment

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The target is a system that could actually run in production, not a compose demo.
Production readiness is not a final phase — several of its requirements (health
check semantics, shutdown behaviour, the ffmpeg sandbox) are impossible to bolt
on afterwards because they constrain how each service is written.

One risk deserves naming up front: **ffmpeg parses attacker-controlled binary
input.** Media demuxers are a long-standing source of memory-safety CVEs. In this
architecture an uploader hands arbitrary bytes to a process that runs ffmpeg
against them. That process must be treated as compromisable.

## Decision

### 1. Untrusted-input containment (highest priority)

- Transcode/probe/thumbnail workers run as **non-root**, with a **read-only root
  filesystem** and a writable `tmpfs` scratch dir only.
- **No egress** from media workers except the object store, Kafka and Postgres
  (NetworkPolicy / firewall). If ffmpeg is exploited, it cannot phone home.
- `--cap-drop ALL`, `no-new-privileges`, a seccomp profile, and memory/CPU limits;
  ffmpeg is additionally bounded by `-threads`, a wall-clock timeout and `ulimit`s.
- Never pass user-controlled strings into a shell — `subprocess` with an argv
  list, no `shell=True`, filenames derived from `video_id` and never from the
  uploaded name (which is stored as metadata only).
- Validate container/codec via ffprobe before transcoding, and reject formats
  outside an allow-list; enable only the demuxers we need where feasible.

### 2. Authentication and authorization

- **OIDC bearer tokens** verified in the API (`pyjwt` + JWKS, or `authlib` for a
  full client flow). No home-grown auth.
- Every object key is prefixed by owner; presigned URLs are minted only for keys
  the caller owns (ADR-0006's trust boundary).
- The SSE endpoint authorizes per video on connect **and** the token's expiry is
  enforced on the long-lived stream — a connection opened at hour 0 must not
  stream past the token's life.
- Per-user upload quotas and rate limits at the edge (Traefik/nginx or an API
  middleware), plus a max upload size enforced in the presign policy itself.

### 3. Configuration and secrets

- 12-factor: all config from the environment via `pydantic-settings`, validated at
  startup, **fail fast** on anything missing or malformed.
- No secrets in images or git. Docker/Kubernetes secrets, or SOPS-encrypted files
  for the compose deployment; `.env.example` documents every variable with a
  non-secret placeholder.
- One config module per service; nothing reads `os.environ` at the call site.

### 4. Lifecycle: health, shutdown, restarts

- **`/healthz` (liveness) must not check dependencies.** A liveness probe that
  pings Kafka turns a broker blip into a mass restart of every pod — a
  self-inflicted outage. It answers "is this process responsive".
- **`/readyz` (readiness)** does check Kafka, Postgres and the object store, and
  removes the instance from the load balancer while a dependency is down.
- Workers expose the same two endpoints on a side port so probes work without a
  web framework.
- **Graceful shutdown on SIGTERM**: stop consuming, let the in-flight rendition
  finish (or abort it cleanly), commit offsets, flush the producer, close.
  Termination grace period is set above the p99 rendition time; anything killed
  mid-flight is safe anyway because of idempotency (ADR-0005).
- Kubernetes `preStop` sleep so the LB deregisters before SIGTERM reaches the API.

### 5. Kafka production settings

`replication.factor=3`, `min.insync.replicas=2`, `acks=all`,
`enable.idempotence=true`, `unclean.leader.election.enable=false`, auto topic
creation **off**, retention set per topic (short for stage topics, long for
`video.status` and DLQs), TLS + SASL between clients and brokers, quotas per
client id. Brokers spread across availability zones with rack awareness.

### 6. Scaling

- **KEDA scaling workers on Kafka consumer lag** is the right primitive here —
  CPU-based autoscaling reacts to the wrong signal for queue workloads. Replica
  ceiling equals the partition count (ADR-0002), which is why partitions are
  over-provisioned up front.
- The API scales on connections and CPU; SSE makes connection count the real
  capacity metric (ADR-0008).
- Cost control: transcode is the expensive stage; a separate node pool (spot
  instances are safe because the work is idempotent and replayable) is the
  intended production shape.

### 7. Data durability

- Postgres: managed service or streaming replication, PITR via WAL archiving,
  restore rehearsed — a backup that has never been restored is not a backup.
- Object store: versioning on, lifecycle rules for `tmp/` and abandoned uploads,
  cross-region replication for sources if the business requires it.
- The read model is rebuildable from Kafka (ADR-0007), so its backup requirement
  is weaker than the object store's — sources and renditions are the irreplaceable
  data.

### 8. Supply chain and CI/CD

- Pinned lockfile, Renovate/Dependabot PRs, **Trivy** image scanning and an SBOM
  in CI, `pip-audit` on the lockfile.
- Multi-stage images, non-root, minimal base, pinned digests.
- Pipeline: lint → unit → integration → build+scan → e2e → deploy. Migrations run
  as a pre-deploy job, and must be backward compatible for one release so a
  rollback does not meet a schema it cannot read.

### 9. SLOs and alerting

Defined targets, each attached to the symptom alert that watches it
(`ops/prometheus/rules/pipeline.yml`) rather than a cause — ADR-0010's own
rule, restated concretely here (Phase 12):

| SLO | Target | Watched by |
|---|---|---|
| Time to first rendition | p95 < 60 s for a 5-minute source | **No direct alert** — see below |
| Pipeline completes without manual intervention | ≥ 99% of videos | `DeadLetterQueueNonEmpty` — a DLQ arrival *is* the manual-intervention event (`make replay`), so its count is this SLO's error budget spend, not a proxy for it |
| DLQ depth | 0, always | `DeadLetterQueueNonEmpty` (same alert — the instantaneous-symptom framing of the row above) |
| Stage handling time | p99 < 300 s per stage | `StageP99Regression` |
| Consumer backlog | bounded, recovers | `ConsumerLagHigh` |
| Rebalance-eviction risk | no message runs past 480s in-flight | `StageApproachingPollIntervalEviction` — the ADR-0004 early warning: 80% of `max.poll.interval.ms`, so a message that would eventually get evicted is caught before the eviction, not after |
| SSE capacity | concurrent streams stay under the configured cap | `SSEConnectionsAtCap` |

Time to first rendition has no alert because it has no metric: it spans
probe → queue wait → transcode, and nothing today emits a single "upload to
first rendition" duration (`ops/grafana/dashboards/pipeline-overview.json`'s
own Pipeline Overview panel already says as much — "the closest thing to
end-to-end latency... without a dedicated upload-to-complete metric," a
Phase 10 finding, not a new one). It's *observable* per video today, through
the trace each upload produces (ADR-0010: "per-video questions need
traces") — spanning probe through the first `rendition.completed`, verified
live in `obs-verify`. What it doesn't have is an aggregate p95 or a
threshold to page on. Manufacturing one now means guessing a number this
repo's own convention explicitly rejects elsewhere (`pipeline.yml`'s own
header: "Thresholds are placeholders until Phase 10 measures real numbers")
— Phase 14 exists specifically to replace a guess with a measured baseline
(load-testing scenario → real p95 → this row's target and its alert both
get filled in from data, not invented here).

Every other threshold above is *itself* still a placeholder in the same
sense, not yet a measured baseline — Phase 14's own checklist item
("Record the numbers as SLO baselines in ADR-0015, replacing its
placeholders") is what turns this table's targets into verified numbers; this
section defines what's being targeted and how each target's burn is
observed, which is the part that doesn't wait on load-test data to be true.

## Alternatives considered

- **"Harden it later, in a final phase."** Rejected: probe semantics, shutdown
  behaviour and the ffmpeg sandbox all change how services are written. Later
  means rewriting.
- **Run ffmpeg in-process (PyAV) for speed.** Rejected — see ADR-0014; the
  isolation is the point, and the speed difference is noise next to encoding time.
- **Firecracker/gVisor per transcode job.** Stronger isolation and a real option
  at scale. Rejected for now as disproportionate; the container hardening above
  plus no-egress covers the realistic threat, and this ADR names the upgrade path.
- **Session cookies and app-managed users.** Rejected: OIDC delegates the part of
  security most easily got wrong.
- **CPU-based autoscaling for workers.** Rejected: lag is the signal that
  actually reflects backlog; CPU lags behind it and oscillates.

## Consequences

- Local development must not require the full hardening: compose runs a relaxed
  profile, and the hardened settings live in the deployment manifests. The gap
  between them is itself a risk, so `make ci` runs e2e against the hardened image.
- Non-root + read-only rootfs forces every temp path through an explicit scratch
  mount — worker code must never write next to the binary or into the image.
- OIDC means a local identity provider (Keycloak or a dev-mode static token) in
  compose, which is extra moving parts in dev.
- Some of this (KEDA, NetworkPolicies, PITR) only materializes on a real
  deployment target; the open question in PROGRESS.md about Kubernetes vs compose
  as the end state decides how much gets built.

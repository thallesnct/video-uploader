# ADR-0016: Per-user isolation, identity, and how load testing constrains it

- **Status:** Accepted
- **Date:** 2026-08-26

## Context

The system is intended for real use, not only as a study project, and will be
load tested. That makes multi-tenancy a design input rather than a later
retrofit: `owner_id` has to be in the object keys and the schema from the first
migration, because adding it afterwards means rewriting every key and backfilling
every row.

"Isolation" is a vague word, so this ADR fixes what it means here.

## Decision

**Isolation lives in data and authorization, never in the Kafka topology.**

### 1. Identity

OIDC bearer tokens, verified against the issuer's JWKS (cached, refreshed on
unknown `kid`). The `sub` claim is `owner_id`. No home-grown authentication, no
session cookies, no user table of our own beyond a projection of the `sub`.

### 2. Object keys carry the owner

```
users/{owner_id}/videos/{video_id}/source.{ext}
users/{owner_id}/videos/{video_id}/renditions/{height}p.mp4
users/{owner_id}/videos/{video_id}/hls/…    thumbs/…
tmp/{owner_id}/{video_id}/…
```

This is what makes presigned upload safe (ADR-0006). The API mints a URL only
for a key under the caller's own prefix, and the signature pins the exact key,
so a tampered request cannot write into another tenant's namespace. Bucket
policy denies any key outside `users/${sub}/` as defence in depth.

### 3. Every row is owned

`videos.owner_id` is not null, indexed, and part of every query's WHERE clause.
Renditions and events inherit ownership through `video_id`. A query without an
owner filter is a bug, and repository functions take `owner_id` as a required
argument so it cannot be omitted by forgetting rather than by choosing.

### 4. Authorization on the stream, not just the endpoints

`GET /videos/{id}/events` checks ownership on connect. This one is easy to miss
because the endpoint works perfectly without the check — it simply also streams
other people's progress to anyone holding a UUID. The token's expiry is enforced
for the life of the connection too: a stream opened at hour 0 must not still be
running on an expired token at hour 9 (ADR-0008).

### 5. Quotas are part of isolation

Per-user limits on upload size, concurrent uploads, and videos in flight. One
tenant must not be able to fill the `rendition.requested` partitions and starve
everyone else, and a transcode pool is precisely the shared resource where that
happens.

### 6. Kafka topology stays tenant-agnostic

Messages carry `owner_id` for tracing and authorization, but there are no
per-tenant topics or partitions. Partition count is the parallelism budget
(ADR-0002); spending it on tenancy would cap throughput and explode topic count.
Fair scheduling between tenants, if ever needed, is a quota and consumer-side
concern, not a topology one.

### 7. Identity provider: standards in the app, something light in dev

The application verifies a signature against a JWKS URL and nothing more, so the
issuer is swappable. Dev, CI and load tests run a **minimal local issuer** that
mints signed tokens instantly from a fixed key pair. Keycloak (or any hosted
OIDC provider) is a drop-in for a real deployment — only the issuer URL changes.

The reason is load testing: driving thousands of synthetic users through a real
provider's interactive flows is slow and fiddly, and a load test that spends its
time in the auth server measures the wrong system.

## Alternatives considered

- **No isolation; single-user local tool.** Rejected by the project's intent.
  Retrofitting `owner_id` later means rewriting every object key and backfilling
  every row — the most expensive change in this list.
- **Isolation by separate buckets per tenant.** Rejected: buckets are a limited,
  slow-to-provision resource on real S3, and per-prefix policy achieves the same
  boundary. Revisit only if a compliance requirement demands physical separation.
- **Per-tenant topics or partitions.** Rejected — see §6. It converts a data
  concern into a topology concern and caps parallelism.
- **A row-level-security (RLS) policy in Postgres instead of query filters.**
  Genuinely attractive, and stronger: the database refuses to return another
  tenant's row even if application code forgets. Deferred rather than rejected —
  it requires every connection to set a session variable, which interacts poorly
  with connection pooling and with the projector writing on behalf of all
  tenants. Documented as the upgrade path if isolation ever becomes a compliance
  requirement rather than a correctness one.
- **Keycloak from the start.** Rejected as the default for the load-testing
  reason in §7, not because it is wrong. It stays the documented production path.

## Consequences

- Every API handler needs the caller's `owner_id`; it is a required argument to
  repository functions rather than an optional filter.
- The load-test harness needs a token minter. That is a deliverable, not a
  detail, and it lands with the dev issuer.
- Quota enforcement must be measurable, or the noisy-neighbour behaviour the
  load test is meant to reveal will not be observable.
- Objects gain a path segment. Key builders already centralise this
  (ADR-0006), so the change is contained to `libs/pipeline/storage.py` and its
  tests — which is exactly why they were centralised.
- A future move to RLS is possible without changing the object layout, since the
  ownership boundary is expressed identically in both places.

# ADR-0006: Object store — S3-compatible (MinIO in dev) with presigned direct upload

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

ADR-0001 makes the object store load-bearing: it holds sources, renditions, HLS
segments and thumbnails, and every worker reads and writes it. Two questions:
which store, and how do bytes get in.

Uploads can be multi-GB. If they stream through FastAPI, one upload occupies a
worker process for its whole duration, memory or disk is consumed twice, request
timeouts and proxy body limits become the constraint, and API replicas must scale
with upload volume rather than with request volume.

## Decision

**S3-compatible object storage**: MinIO in dev/CI (same API, runs in compose and
in testcontainers), AWS S3 or any S3-compatible service in production, behind one
`libs/pipeline/storage.py` abstraction.

**Presigned `PUT` direct from the browser to the store.** The API never sees the
bytes:

1. `POST /videos` → create row (`awaiting_upload`), return a presigned URL scoped
   to one exact key, with a content-type condition, a max size, and a short expiry.
2. Browser `PUT`s directly to the store, showing native upload progress.
3. `POST /videos/{id}/complete` → API HEADs the object to verify it exists and
   matches the declared size, then emits `video.uploaded` (ADR-0001).

Files above ~100 MB use S3 multipart via presigned part URLs; the client library
handles resumption.

Key layout:

```
videos/{video_id}/source.{ext}
videos/{video_id}/renditions/{height}p.mp4
videos/{video_id}/hls/{height}p/{segment}.ts, playlist.m3u8
videos/{video_id}/hls/master.m3u8
videos/{video_id}/thumbs/poster.jpg, sprite.jpg, sprite.vtt
tmp/{video_id}/{rendition}.part          # promoted on success only
```

Downloads and playback also use presigned GETs, so the API is never a media proxy.

## Alternatives considered

- **Store blobs in Postgres (bytea / large objects).** Rejected: bloats WAL and
  backups, no presigning so all traffic crosses the app, and the DB becomes the
  scaling bottleneck for a workload it is not designed for.
- **Shared filesystem / NFS volume.** Rejected: works in compose, fails on any
  multi-node deployment; no presigned URLs, no lifecycle policies, and locking
  semantics that transcode workers would have to reason about.
- **GCS / Azure Blob directly.** Both are fine services, and the abstraction keeps
  a port open. Rejected as the default because the S3 API is the one every
  library, tool and local emulator speaks — it is the portable choice.
- **Upload through the API, then forward to the store.** Rejected for the reasons
  in Context. Kept only as a documented fallback for clients that cannot reach the
  store directly.

## Consequences

- CORS must be configured on the bucket for browser PUTs — a classic first-run
  failure; `make smoke` checks it.
- Presign expiry must exceed realistic upload duration on a slow connection
  (start at 6 h for multipart, 15 min for simple PUT).
- An upload that never calls `/complete` leaves an orphan; a lifecycle rule
  expires `tmp/` and unclaimed sources, coordinated with retry windows (ADR-0001).
- **MinIO and S3 differ on aborting incomplete multipart uploads.** S3 expects an
  `AbortIncompleteMultipartUpload` lifecycle rule; MinIO rejects that rule and
  purges stale uploads server-side via `api stale_uploads_expiry` (24 h default).
  The dev bootstrap relies on the MinIO behaviour, so the production deployment
  on real S3 must add the lifecycle rule explicitly or abandoned multipart parts
  accumulate and are billed indefinitely.
- **Promoting a scratch object with `copy_object` caps at 5 GB on real S3.**
  A single-part server-side copy cannot exceed that, so renditions above it need
  multipart copy. MinIO is more permissive, which means this will pass every
  local test and fail in production on exactly the large files the pipeline
  exists to handle. **Resolved in Phase 5** — `storage.promote()` uses boto3's
  transfer-managed `client.copy()` rather than the low-level `copy_object`;
  `copy()` chooses single- or multi-part transparently above its transfer
  threshold, so no manual size check or hand-rolled multipart logic is needed.
- Because the client writes directly, **the object key is the trust boundary**:
  the presign must pin the exact key, and `/complete` must verify rather than
  trust the client's claim.

## Follow-on decision: presigning against a different host than the internal client (2026-08-27)

Discovered building Phase 8's upload page — the first thing to actually run a
browser against the containerized stack. `ObjectStore` used one `S3Settings`
endpoint for both the boto3 client's own calls (`head`, `promote`, `download`
— all made from inside a container, where `http://minio:9000` resolves) and
for signing presigned URLs. A URL's signature covers its host, so a browser
handed a presign signed against `http://minio:9000` cannot use it at all — that
name resolves nowhere outside the compose network. This was invisible until
now because every prior test ran on the host, where `.env`'s `S3_ENDPOINT=
http://localhost:9000` happens to be correct for both purposes at once.

Fixed with a second, optional setting: `S3Settings.public_endpoint`, defaulting
to `endpoint` (preserving today's host-based behavior unchanged). `ObjectStore`
gets a second lazily-constructed boto3 client — same credentials, region, and
signature version, only the `endpoint_url` differs — used exclusively by
`presign_put`/`presign_get`. The compose `api` service sets
`S3_PUBLIC_ENDPOINT=http://localhost:${MINIO_API_PORT}` alongside its internal
`S3_ENDPOINT=http://minio:9000`.

### Consequences

- Any future deployment target needs to know its own internal-vs-public S3
  endpoint split; on real S3 both settings are typically the same public URL,
  so this only matters for a container-networked MinIO.
- A test asserting a presigned URL's host must check against `public_endpoint`,
  not `endpoint`, or it will pass for the wrong reason.

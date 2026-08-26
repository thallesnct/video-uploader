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
- Because the client writes directly, **the object key is the trust boundary**:
  the presign must pin the exact key, and `/complete` must verify rather than
  trust the client's claim.

"""Object keys and S3 access (ADR-0006).

Every key in the system is built here. Call sites never format their own: a
stray f-string is how a worker writes a rendition somewhere the packager will
never look for it, and that failure is silent.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pipeline.settings import s3_settings

if TYPE_CHECKING:  # boto3 is heavy; unit tests import this module for key builders
    pass

# --------------------------------------------------------------------------- keys


def owner_prefix(owner_id: str) -> str:
    """Everything a tenant owns lives under this prefix (ADR-0016).

    The presigned URL the browser gets is signed for one exact key, and the API
    only ever signs keys under the caller's own prefix — so this string is the
    tenancy boundary, enforced by a bucket policy as well as by our code.
    """
    return f"users/{owner_id}"


def video_prefix(owner_id: str, video_id: UUID | str) -> str:
    return f"{owner_prefix(owner_id)}/videos/{video_id}"


def source_key(owner_id: str, video_id: UUID | str, extension: str) -> str:
    return f"{video_prefix(owner_id, video_id)}/source.{extension.lstrip('.')}"


def rendition_key(owner_id: str, video_id: UUID | str, rendition: str) -> str:
    return f"{video_prefix(owner_id, video_id)}/renditions/{rendition}.mp4"


def hls_playlist_key(owner_id: str, video_id: UUID | str, rendition: str) -> str:
    return f"{video_prefix(owner_id, video_id)}/hls/{rendition}/playlist.m3u8"


def hls_master_key(owner_id: str, video_id: UUID | str) -> str:
    return f"{video_prefix(owner_id, video_id)}/hls/master.m3u8"


def poster_key(owner_id: str, video_id: UUID | str) -> str:
    return f"{video_prefix(owner_id, video_id)}/thumbs/poster.jpg"


def sprite_key(owner_id: str, video_id: UUID | str) -> str:
    return f"{video_prefix(owner_id, video_id)}/thumbs/sprite.jpg"


def sprite_vtt_key(owner_id: str, video_id: UUID | str) -> str:
    return f"{video_prefix(owner_id, video_id)}/thumbs/sprite.vtt"


def owns(owner_id: str, key: str) -> bool:
    """Whether a key belongs to this owner.

    Used before signing or serving anything: a key that arrived from a client is
    a claim, never a fact.
    """
    return key.startswith(f"{owner_prefix(owner_id)}/") or key.startswith(f"tmp/{owner_id}/")


def scratch_key(owner_id: str, video_id: UUID | str, name: str) -> str:
    """Temporary output, promoted to its final key only once complete.

    Writing straight to the final key would let a half-written file be mistaken
    for a finished rendition by the idempotency check in ADR-0005. tmp/ is also
    the only prefix the bucket lifecycle expires.
    """
    return f"tmp/{owner_id}/{video_id}/{name}"


# -------------------------------------------------------------------------- store


class ObjectStore:
    """Thin S3 wrapper. Kept small so swapping MinIO for S3 stays a config change."""

    def __init__(self, client: Any | None = None, presign_client: Any | None = None) -> None:
        self._settings = s3_settings()
        self._client = client
        self._presign_client = presign_client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = self._new_client(self._settings.endpoint)
        return self._client

    @property
    def presign_client(self) -> Any:
        """Only used to sign URLs handed to the browser (ADR-0006 follow-on).

        A presigned URL's signature covers its host, so it must be built
        against whatever host the browser can actually reach — which differs
        from `client`'s endpoint when the API runs in a container (it dials
        MinIO at http://minio:9000; a browser can only reach
        http://localhost:9000). Reuses `client` outright when no public
        endpoint is configured, which is correct for host-based dev/CI where
        both are the same URL — no reason to hold two boto3 clients then.
        """
        if self._settings.public_endpoint is None:
            return self.client
        if self._presign_client is None:
            self._presign_client = self._new_client(self._settings.public_endpoint)
        return self._presign_client

    def _new_client(self, endpoint: str) -> Any:
        import boto3  # imported lazily: key builders must work without creds
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self._settings.access_key,
            aws_secret_access_key=self._settings.secret_key,
            region_name=self._settings.region,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    @property
    def bucket(self) -> str:
        return self._settings.bucket

    @property
    def presign_put_expiry_s(self) -> int:
        return self._settings.presign_put_expiry_s

    def presign_put(self, key: str, content_type: str, max_bytes: int | None = None) -> str:
        """URL the browser PUTs to directly — the API never sees the bytes.

        The key is pinned by the signature, so a client cannot redirect its
        upload somewhere else: that signature is the trust boundary (ADR-0006).
        """
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": key, "ContentType": content_type}
        if max_bytes is not None:
            params["ContentLength"] = max_bytes
        return str(
            self.presign_client.generate_presigned_url(
                "put_object",
                Params=params,
                ExpiresIn=self._settings.presign_put_expiry_s,
            )
        )

    def presign_get(self, key: str) -> str:
        return str(
            self.presign_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=self._settings.presign_get_expiry_s,
            )
        )

    def head(self, key: str) -> dict[str, Any] | None:
        """Object metadata, or None when it does not exist."""
        from botocore.exceptions import ClientError

        try:
            return dict(self.client.head_object(Bucket=self.bucket, Key=key))
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def download(self, key: str, destination: str) -> None:
        self.client.download_file(self.bucket, key, destination)

    def upload(self, source: str, key: str, content_type: str | None = None) -> None:
        extra = {"ContentType": content_type} if content_type else None
        self.client.upload_file(source, self.bucket, key, ExtraArgs=extra)

    def promote(self, scratch: str, final: str) -> None:
        """Move a completed scratch object to its real key, then delete the temp.

        Copy-then-delete rather than write-in-place so a crash mid-transcode can
        never leave a truncated file sitting at the key the packager reads.

        Uses the transfer-managed `copy()`, not the low-level `copy_object` API.
        `copy_object` performs a single-request server-side copy that real S3
        caps at 5 GB (ADR-0006) — MinIO is more permissive, so that cap passes
        every local test and only fails in production, on exactly the large
        renditions this pipeline exists to handle. `copy()` transparently uses
        multipart copy above boto3's transfer threshold, verified against a
        real boto3 client rather than assumed.
        """
        self.client.copy(
            CopySource={"Bucket": self.bucket, "Key": scratch},
            Bucket=self.bucket,
            Key=final,
        )
        self.client.delete_object(Bucket=self.bucket, Key=scratch)


@functools.lru_cache(maxsize=1)
def object_store() -> ObjectStore:
    return ObjectStore()

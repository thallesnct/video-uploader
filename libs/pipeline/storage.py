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


def video_prefix(video_id: UUID | str) -> str:
    return f"videos/{video_id}"


def source_key(video_id: UUID | str, extension: str) -> str:
    return f"{video_prefix(video_id)}/source.{extension.lstrip('.')}"


def rendition_key(video_id: UUID | str, rendition: str) -> str:
    return f"{video_prefix(video_id)}/renditions/{rendition}.mp4"


def hls_playlist_key(video_id: UUID | str, rendition: str) -> str:
    return f"{video_prefix(video_id)}/hls/{rendition}/playlist.m3u8"


def hls_master_key(video_id: UUID | str) -> str:
    return f"{video_prefix(video_id)}/hls/master.m3u8"


def poster_key(video_id: UUID | str) -> str:
    return f"{video_prefix(video_id)}/thumbs/poster.jpg"


def sprite_key(video_id: UUID | str) -> str:
    return f"{video_prefix(video_id)}/thumbs/sprite.jpg"


def sprite_vtt_key(video_id: UUID | str) -> str:
    return f"{video_prefix(video_id)}/thumbs/sprite.vtt"


def scratch_key(video_id: UUID | str, name: str) -> str:
    """Temporary output, promoted to its final key only once complete.

    Writing straight to the final key would let a half-written file be mistaken
    for a finished rendition by the idempotency check in ADR-0005. tmp/ is also
    the only prefix the bucket lifecycle expires.
    """
    return f"tmp/{video_id}/{name}"


# -------------------------------------------------------------------------- store


class ObjectStore:
    """Thin S3 wrapper. Kept small so swapping MinIO for S3 stays a config change."""

    def __init__(self, client: Any | None = None) -> None:
        self._settings = s3_settings()
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3  # imported lazily: key builders must work without creds
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=self._settings.endpoint,
                aws_access_key_id=self._settings.access_key,
                aws_secret_access_key=self._settings.secret_key,
                region_name=self._settings.region,
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 5, "mode": "standard"},
                ),
            )
        return self._client

    @property
    def bucket(self) -> str:
        return self._settings.bucket

    def presign_put(self, key: str, content_type: str, max_bytes: int | None = None) -> str:
        """URL the browser PUTs to directly — the API never sees the bytes.

        The key is pinned by the signature, so a client cannot redirect its
        upload somewhere else: that signature is the trust boundary (ADR-0006).
        """
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": key, "ContentType": content_type}
        if max_bytes is not None:
            params["ContentLength"] = max_bytes
        return str(
            self.client.generate_presigned_url(
                "put_object",
                Params=params,
                ExpiresIn=self._settings.presign_put_expiry_s,
            )
        )

    def presign_get(self, key: str) -> str:
        return str(
            self.client.generate_presigned_url(
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
        """
        self.client.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": scratch},
            Key=final,
        )
        self.client.delete_object(Bucket=self.bucket, Key=scratch)


@functools.lru_cache(maxsize=1)
def object_store() -> ObjectStore:
    return ObjectStore()

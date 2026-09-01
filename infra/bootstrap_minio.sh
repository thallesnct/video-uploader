#!/usr/bin/env bash
# Create the bucket and its lifecycle rules. Safe to re-run.
#
# Only tmp/ scratch expires. Sources and renditions never do: ADR-0001 warns that
# deleting a source before its retry window closes turns a retryable failure into
# an unreproducible one.
#
# Incomplete multipart uploads are NOT handled here. On AWS S3 that is an
# AbortIncompleteMultipartUpload lifecycle rule, but MinIO rejects that rule and
# instead purges stale uploads server-side via `api stale_uploads_expiry`
# (24h by default). The production deployment on real S3 must add the lifecycle
# rule explicitly — see ADR-0006.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose run --rm -T mc '
set -e
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

if mc ls "local/$S3_BUCKET" >/dev/null 2>&1; then
  echo "  bucket $S3_BUCKET already present"
else
  mc mb "local/$S3_BUCKET"
fi

# Sources and renditions are the irreplaceable data (ADR-0015 backups
# section) — versioning means an overwrite or an accidental `mc rm` is
# recoverable, the object-store equivalent of Postgres PITR above. tmp/
# scratch still expires via the lifecycle rule below regardless of
# versioning; MinIO expires every version of an object once its lifecycle
# rule matches, not just the current one.
mc version enable "local/$S3_BUCKET"
echo -n "  versioning: "
mc version info "local/$S3_BUCKET"

cat <<JSON | mc ilm import "local/$S3_BUCKET"
{
  "Rules": [
    {
      "ID": "expire-scratch",
      "Status": "Enabled",
      "Filter": { "Prefix": "tmp/" },
      "Expiration": { "Days": 1 }
    }
  ]
}
JSON
echo "  lifecycle: tmp/ expires after 1 day"
echo -n "  stale multipart uploads: "
mc admin config get local api stale_uploads_expiry 2>/dev/null || echo "server default (24h)"
'

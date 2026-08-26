#!/usr/bin/env bash
# Create the bucket and its lifecycle rules. Safe to re-run.
#
# Lifecycle matters more here than it looks: ADR-0001 warns that a rule deleting
# a source before its retry window closes produces unreproducible failures, so
# only tmp/ and abandoned multipart uploads expire — never sources or renditions.
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

cat <<JSON | mc ilm import "local/$S3_BUCKET"
{
  "Rules": [
    {
      "ID": "expire-scratch",
      "Status": "Enabled",
      "Filter": { "Prefix": "tmp/" },
      "Expiration": { "Days": 1 }
    },
    {
      "ID": "abort-incomplete-multipart",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
JSON
echo "  lifecycle rules applied to $S3_BUCKET"
'

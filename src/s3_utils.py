from __future__ import annotations

import argparse
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from src.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    DATA_DIR,
    PROCESSED_DIR,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_PREFIX,
    SYNTHETIC_DIR,
)


def get_s3_client():
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def ensure_bucket(bucket: str | None = None, create: bool = False, client=None) -> str:
    bucket = bucket or S3_BUCKET
    client = client or get_s3_client()
    try:
        client.head_bucket(Bucket=bucket)
        return bucket
    except ClientError:
        if not create:
            raise
        kwargs = {"Bucket": bucket}
        if AWS_REGION and AWS_REGION != "us-east-1" and not S3_ENDPOINT_URL:
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": AWS_REGION}
        client.create_bucket(**kwargs)
        return bucket


def upload_file(local_path: Path | str, key: str, bucket: str | None = None, client=None) -> int:
    local_path = Path(local_path)
    bucket = bucket or S3_BUCKET
    client = client or get_s3_client()
    client.upload_file(str(local_path), bucket, key)
    return local_path.stat().st_size


def upload_directory(
    local_dir: Path | str,
    prefix: str,
    bucket: str | None = None,
    client=None,
) -> dict:
    local_dir = Path(local_dir)
    bucket = bucket or S3_BUCKET
    client = client or get_s3_client()
    uploaded = 0
    total_bytes = 0
    if not local_dir.exists():
        return {"files": 0, "bytes": 0, "prefix": prefix}
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(local_dir).as_posix()
        key = f"{prefix.rstrip('/')}/{relative}"
        total_bytes += upload_file(path, key, bucket, client)
        uploaded += 1
    return {"files": uploaded, "bytes": total_bytes, "prefix": prefix}


def upload_all(create_bucket: bool = False) -> dict:
    client = get_s3_client()
    bucket = ensure_bucket(create=create_bucket, client=client)
    prefix = S3_PREFIX.rstrip("/")
    results = {
        "bucket": bucket,
        "raw_synthetic": upload_directory(SYNTHETIC_DIR, f"{prefix}/raw/synthetic", bucket, client),
        "processed": upload_directory(PROCESSED_DIR, f"{prefix}/processed", bucket, client),
    }
    reference = {}
    for filename in ("taxi_zone_lookup.csv", "zone_centroids.csv"):
        path = DATA_DIR / filename
        if path.exists():
            reference[filename] = upload_file(path, f"{prefix}/reference/{filename}", bucket, client)
    results["reference"] = reference
    return results


def list_objects(limit: int = 50) -> list[str]:
    client = get_s3_client()
    response = client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX, MaxKeys=limit)
    return [item["Key"] for item in response.get("Contents", [])]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Subir datos a S3 o MinIO")
    parser.add_argument("--upload", action="store_true", help="Subir data/synthetic y data/processed")
    parser.add_argument("--create-bucket", action="store_true", help="Crear el bucket si no existe")
    parser.add_argument("--list", action="store_true", help="Listar objetos del prefijo")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.create_bucket:
        ensure_bucket(create=True)
    if args.upload:
        print(json.dumps(upload_all(create_bucket=args.create_bucket), indent=2, ensure_ascii=False))
    if args.list:
        for key in list_objects():
            print(key)
    if not (args.upload or args.list or args.create_bucket):
        build_parser().print_help()


if __name__ == "__main__":
    main()

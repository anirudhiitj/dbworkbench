"""S3 utilities for snapshot upload/download/management."""

import boto3
from botocore.exceptions import ClientError
from fastapi_backend.app.config import S3_BUCKET, S3_REGION


def _get_client():
    return boto3.client("s3", region_name=S3_REGION)


def upload_snapshot(local_path: str, s3_key: str):
    """Upload a local file to S3."""
    try:
        _get_client().upload_file(local_path, S3_BUCKET, s3_key)
    except ClientError as e:
        raise RuntimeError(f"Failed to upload snapshot to S3 (key='{s3_key}', path='{local_path}')") from e


def download_snapshot(s3_key: str, local_path: str):
    """Download a snapshot from S3 to a local file."""
    try:
        _get_client().download_file(S3_BUCKET, s3_key, local_path)
    except ClientError as e:
        raise RuntimeError(f"Failed to download snapshot from S3 (key='{s3_key}', path='{local_path}')") from e


def list_snapshots() -> list[dict]:
    """List all snapshot objects in the S3 bucket."""
    client = _get_client()
    try:
        paginator = client.get_paginator("list_objects_v2")
        all_objects: list[dict] = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="snapshots/"):
            contents = page.get("Contents", [])
            all_objects.extend(contents)
    except ClientError as e:
        raise RuntimeError("Failed to list snapshots from S3") from e

    return [
        {"key": obj["Key"], "size": obj["Size"], "last_modified": obj["LastModified"]}
        for obj in all_objects
    ]


def delete_snapshot(s3_key: str):
    """Delete a single snapshot object from S3."""
    try:
        _get_client().delete_object(Bucket=S3_BUCKET, Key=s3_key)
    except ClientError as e:
        raise RuntimeError(f"Failed to delete snapshot from S3 (key='{s3_key}')") from e


def delete_old_snapshots(keep_latest: int = 5):
    """Keep only the latest N snapshots in S3, delete the rest."""
    objects = list_snapshots()
    # Sort newest first
    objects.sort(key=lambda o: o["last_modified"], reverse=True)
    for obj in objects[keep_latest:]:
        delete_snapshot(obj["key"])

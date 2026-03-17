"""S3 utilities for snapshot upload/download/management."""

import boto3
from botocore.exceptions import ClientError
from fastapi_backend.app.config import S3_BUCKET, S3_REGION


def _get_client():
    return boto3.client("s3", region_name=S3_REGION)


def upload_snapshot(local_path: str, s3_key: str):
    """Upload a local file to S3."""
    _get_client().upload_file(local_path, S3_BUCKET, s3_key)


def download_snapshot(s3_key: str, local_path: str):
    """Download a snapshot from S3 to a local file."""
    _get_client().download_file(S3_BUCKET, s3_key, local_path)


def list_snapshots() -> list[dict]:
    """List all snapshot objects in the S3 bucket."""
    client = _get_client()
    response = client.list_objects_v2(Bucket=S3_BUCKET, Prefix="snapshots/")
    contents = response.get("Contents", [])
    return [{"key": obj["Key"], "size": obj["Size"], "last_modified": obj["LastModified"]} for obj in contents]


def delete_snapshot(s3_key: str):
    """Delete a single snapshot object from S3."""
    _get_client().delete_object(Bucket=S3_BUCKET, Key=s3_key)


def delete_old_snapshots(keep_latest: int = 5):
    """Keep only the latest N snapshots in S3, delete the rest."""
    objects = list_snapshots()
    # Sort newest first
    objects.sort(key=lambda o: o["last_modified"], reverse=True)
    for obj in objects[keep_latest:]:
        delete_snapshot(obj["key"])

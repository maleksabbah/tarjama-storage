# app/Repositories/S3Client.py
"""
Boto3 wrapper for MinIO/S3.

Two clients:
- self.client      -> signs/operates against the INTERNAL endpoint (minio:9000).
                      Used for the storage container's own ops (head, delete, list).
- self.presign     -> signs against the PUBLIC endpoint (S3_PUBLIC_ENDPOINT).
                      Used ONLY to generate presigned URLs, so the signature is
                      computed for the same host the uploader/downloader will hit.
                      No host string-rewriting (that breaks SigV4 signatures).
"""
import os
from typing import List

import boto3
from botocore.client import Config as BotoConfig


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "asr-bucket")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_PUBLIC_ENDPOINT = os.getenv("S3_PUBLIC_ENDPOINT", S3_ENDPOINT)


def _make_client(endpoint_url: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=BotoConfig(signature_version="s3v4"),
    )


class S3Client:
    def __init__(self):
        self.bucket = S3_BUCKET
        # Internal client — talks to MinIO over the docker network.
        self.client = _make_client(S3_ENDPOINT)
        # Presign client — signs for the public host the uploader will use.
        # If the public endpoint matches the internal one, this is the same.
        self.presign = (
            self.client if S3_PUBLIC_ENDPOINT == S3_ENDPOINT
            else _make_client(S3_PUBLIC_ENDPOINT)
        )

    # ---- internal ops (use internal client) ----
    def file_exists(self, s3_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception:
            return False

    def get_file_size(self, s3_key: str) -> int:
        resp = self.client.head_object(Bucket=self.bucket, Key=s3_key)
        return resp["ContentLength"]

    def delete_file(self, s3_key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=s3_key)

    def delete_prefix(self, prefix: str) -> int:
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        if "Contents" not in resp:
            return 0
        objects = [{"Key": obj["Key"]} for obj in resp["Contents"]]
        self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})
        return len(objects)

    # ---- presigned URLs (use presign client, signed for public host) ----
    def get_presigned_url(self, s3_key: str, expires_in: int = 3600) -> str:
        return self.presign.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": s3_key},
            ExpiresIn=expires_in,
        )

    def get_presigned_upload_url(self, s3_key: str, expires_in: int = 3600) -> str:
        return self.presign.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": s3_key},
            ExpiresIn=expires_in,
        )
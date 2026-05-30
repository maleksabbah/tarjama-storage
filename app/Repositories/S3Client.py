# app/Repositories/S3Client.py
"""
Boto3 wrapper for MinIO/S3. Same interface as the flat S3_client.py,
moved into a class so it can be DI'd into services.
"""
import os
from typing import List, Optional

import boto3
from botocore.client import Config as BotoConfig


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "asr-bucket")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_PUBLIC_ENDPOINT = os.getenv("S3_PUBLIC_ENDPOINT", S3_ENDPOINT)


class S3Client:
    def __init__(self):
        self.bucket = S3_BUCKET
        self.endpoint = S3_ENDPOINT
        self.public_endpoint = S3_PUBLIC_ENDPOINT
        self.client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
            config=BotoConfig(signature_version="s3v4"),
        )

    def _make_public_url(self, url: str) -> str:
        if self.public_endpoint != self.endpoint:
            return url.replace(self.endpoint, self.public_endpoint)
        return url

    def file_exists(self, s3_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception:
            return False

    def get_file_size(self, s3_key: str) -> int:
        resp = self.client.head_object(Bucket=self.bucket, Key=s3_key)
        return resp["ContentLength"]

    def get_presigned_url(self, s3_key: str, expires_in: int = 3600) -> str:
        url = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": s3_key},
            ExpiresIn=expires_in,
        )
        return self._make_public_url(url)

    def get_presigned_upload_url(
        self, s3_key: str, expires_in: int = 3600,
    ) -> str:
        url = self.client.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": s3_key},
            ExpiresIn=expires_in,
        )
        return self._make_public_url(url)

    def delete_file(self, s3_key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=s3_key)

    def delete_prefix(self, prefix: str) -> int:
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        if "Contents" not in resp:
            return 0
        objects = [{"Key": obj["Key"]} for obj in resp["Contents"]]
        self.client.delete_objects(
            Bucket=self.bucket, Delete={"Objects": objects},
        )
        return len(objects)
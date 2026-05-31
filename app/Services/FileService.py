# storage: app/Services/FileService.py
"""
File registry service. Wraps FileRepository (Postgres) + S3Client (MinIO).

Constructed as FileService(files=<FileRepository>, s3=<S3Client>).
"""
import uuid
from typing import List, Optional

from app.Config.Config import config
from app.Entities import File
from app.Repositories import FileRepository, S3Client


class FileNotFound(Exception):
    pass


class FileService:
    def __init__(self, files: FileRepository, s3: S3Client):
        self.files = files
        self.s3 = s3

    # ─── Registration ───────────────────────────────────────────────────
    async def register(
        self,
        *,
        job_id: str,
        user_id: int,
        category: str,
        file_type: str,
        path: str,
        size_bytes: int = 0,
        mime_type: Optional[str] = None,
        expiry_days: Optional[int] = None,
    ) -> File:
        # Idempotency: same job + same S3 key = same logical file.
        idempotency_key = f"{job_id}:{path}"
        existing = await self.files.get_by_idempotency_key(idempotency_key)
        if existing:
            return existing

        if not size_bytes:
            try:
                size_bytes = self.s3.get_file_size(path)
            except Exception:
                size_bytes = 0

        effective_expiry = (
            expiry_days if expiry_days is not None
            else config.DEFAULT_EXPIRY_DAYS
        )

        return await self.files.create(
            job_id=job_id,
            user_id=user_id,
            category=category,
            file_type=file_type,
            path=path,
            size_bytes=size_bytes,
            mime_type=mime_type,
            expiry_days=effective_expiry,
            idempotency_key=idempotency_key,
        )

    # ─── Listing ────────────────────────────────────────────────────────
    async def list_for_job(self, job_id: str, user_id: Optional[int] = None) -> List[File]:
        return await self.files.list_by_job(job_id, user_id=user_id)

    async def list_for_user(
        self,
        user_id: int,
        category: Optional[str] = None,
        file_type: Optional[str] = None,
    ) -> List[File]:
        return await self.files.list_by_user(
            user_id, category=category, file_type=file_type,
        )

    # ─── Usage ──────────────────────────────────────────────────────────
    async def get_usage(self, user_id: int) -> dict:
        return await self.files.get_user_usage(user_id)

    # ─── Download (presigned GET) ───────────────────────────────────────
    async def get_download_url(self, file_id: str, user_id: int) -> dict:
        file = await self.files.get(file_id)
        if not file or file.user_id != user_id:
            raise FileNotFound(file_id)
        url = self.s3.get_presigned_url(file.path)
        return {"download_url": url, "file_id": file_id, "expires_in": 3600}

    def get_download_url_by_path(self, path: str) -> dict:
        url = self.s3.get_presigned_url(path)
        return {"download_url": url, "path": path, "expires_in": 3600}

    # ─── Deletion ───────────────────────────────────────────────────────
    async def delete_one(self, file_id: str, user_id: int) -> None:
        file = await self.files.get(file_id)
        if not file or file.user_id != user_id:
            raise FileNotFound(file_id)
        try:
            self.s3.delete_file(file.path)
        except Exception:
            pass
        await self.files.delete(file_id)

    async def delete_for_job(self, job_id: str) -> int:
        files = await self.files.list_by_job(job_id)
        for f in files:
            try:
                self.s3.delete_file(f.path)
            except Exception:
                pass
        return await self.files.delete_by_job(job_id)

    # ─── Presigned upload ───────────────────────────────────────────────
    def get_upload_url(self, user_id: int) -> dict:
        upload_id = uuid.uuid4().hex
        s3_key = f"uploads/{user_id}/{upload_id}"
        url = self.s3.get_presigned_upload_url(s3_key)
        return {
            "upload_url": url,
            "upload_id": upload_id,
            "s3_key": s3_key,
            "user_id": user_id,
            "expires_in": 3600,
        }
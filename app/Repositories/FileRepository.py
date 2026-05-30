# storage: app/Repositories/FileRepository.py
"""
SQLAlchemy queries on the files table.
"""
import uuid
from datetime import datetime, timedelta
from typing import Optional, List

from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities import File


class FileRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def _gen_id() -> str:
        return str(uuid.uuid4())

    async def create(
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
        idempotency_key: Optional[str] = None,
    ) -> File:
        expires_at = (
            datetime.utcnow() + timedelta(days=expiry_days) if expiry_days else None
        )
        file = File(
            id=self._gen_id(),
            job_id=job_id,
            user_id=user_id,
            category=category,
            type=file_type,
            path=path,
            size_bytes=size_bytes,
            mime_type=mime_type,
            expires_at=expires_at,
            idempotency_key=idempotency_key,
        )
        self.session.add(file)
        await self.session.flush()
        return file

    async def get(self, file_id: str) -> Optional[File]:
        result = await self.session.execute(
            select(File).where(File.id == file_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Optional[File]:
        result = await self.session.execute(
            select(File).where(File.idempotency_key == key)
        )
        return result.scalar_one_or_none()

    async def list_by_job(
        self, job_id: str, user_id: Optional[int] = None,
    ) -> List[File]:
        query = select(File).where(File.job_id == job_id)
        if user_id is not None:
            query = query.where(File.user_id == user_id)
        query = query.order_by(File.created_at)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def list_by_user(
        self,
        user_id: int,
        category: Optional[str] = None,
        file_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[File]:
        query = select(File).where(File.user_id == user_id)
        if category:
            query = query.where(File.category == category)
        if file_type:
            query = query.where(File.type == file_type)
        query = query.order_by(File.created_at.desc()).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_user_usage(self, user_id: int) -> dict:
        result = await self.session.execute(
            select(
                func.count(File.id).label("file_count"),
                func.coalesce(func.sum(File.size_bytes), 0).label("total_bytes"),
            ).where(File.user_id == user_id)
        )
        row = result.one()
        return {
            "file_count": row.file_count,
            "total_bytes": row.total_bytes,
            "total_mb": round(row.total_bytes / (1024 * 1024), 2),
        }

    async def delete(self, file_id: str) -> bool:
        result = await self.session.execute(
            delete(File).where(File.id == file_id)
        )
        return result.rowcount > 0

    async def delete_by_job(self, job_id: str) -> int:
        result = await self.session.execute(
            delete(File).where(File.job_id == job_id)
        )
        return result.rowcount

    async def list_expired(self) -> List[File]:
        now = datetime.utcnow()
        result = await self.session.execute(
            select(File).where(File.expires_at.is_not(None), File.expires_at < now)
        )
        return list(result.scalars().all())
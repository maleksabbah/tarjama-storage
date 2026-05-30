# app/Dependencies.py
"""
FastAPI dependency factories.
Per-request DB session, S3 client, services.
"""
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.Config.Database import SessionLocal
from app.Repositories import FileRepository, S3Client
from app.Services import FileService
from app.ExceptionHandler import MissingUserHeader


# ─── Per-request DB session (HTTP routes) ───────────────────────────────

async def get_db_session() -> AsyncIterator[AsyncSession]:
    session = SessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


# ─── Session scope (background tasks: cleanup loop) ─────────────────────

@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """
    Same open/commit/close shape as get_db_session, but as a regular
    async context manager so non-FastAPI code can use it.
    """
    session = SessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


# ─── S3 client (built once in main.py lifespan) ─────────────────────────

def get_s3_client(request: Request) -> S3Client:
    return request.app.state.s3_client


# ─── Repositories ───────────────────────────────────────────────────────

def get_file_repo(
    session: AsyncSession = Depends(get_db_session),
) -> FileRepository:
    return FileRepository(session)


# ─── Services ───────────────────────────────────────────────────────────

def get_file_service(
    files: FileRepository = Depends(get_file_repo),
    s3: S3Client = Depends(get_s3_client),
) -> FileService:
    return FileService(files=files, s3=s3)


# ─── Auth (X-User-ID header from gateway) ───────────────────────────────

def current_user_id(request: Request) -> int:
    raw = request.headers.get("X-User-ID")
    if not raw:
        raise MissingUserHeader()
    try:
        return int(raw)
    except ValueError:
        raise MissingUserHeader("Malformed X-User-ID header")
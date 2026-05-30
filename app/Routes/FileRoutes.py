# app/Routes/FileRoutes.py
"""
Storage Service routes — file registry API.
8 endpoints. All require X-User-ID header (set by gateway after JWT verify).
"""
from fastapi import APIRouter, Depends, Request

from app.Services import FileService
from app.Dependencies import get_file_service, current_user_id
from app.Dtos import (
    RegisterFileRequest,
    FileResponse,
    FileListResponse,
    UsageResponse,
    PresignResponse,
    DownloadResponse,
    DeleteResponse,
)


router = APIRouter(tags=["files"])


# ─── Registration ───────────────────────────────────────────────────────

@router.post("/files/register", response_model=FileResponse)
async def register_file(
    req: RegisterFileRequest,
    files: FileService = Depends(get_file_service),
) -> FileResponse:
    """Called by workers after uploading to S3."""
    file = await files.register(
        job_id=req.job_id,
        user_id=req.user_id,
        category=req.category,
        file_type=req.type,
        path=req.path,
        size_bytes=req.size_bytes or 0,
        mime_type=req.mime_type,
        expiry_days=req.expiry_days,
    )
    return FileResponse.model_validate(file)


# ─── Listing ────────────────────────────────────────────────────────────

@router.get("/files", response_model=FileListResponse)
async def list_files(
    user_id: int = Depends(current_user_id),
    files: FileService = Depends(get_file_service),
    job_id: str | None = None,
    category: str | None = None,
    file_type: str | None = None,
):
    if job_id:
        rows = await files.list_for_job(job_id, user_id=user_id)
    else:
        rows = await files.list_for_user(
            user_id, category=category, file_type=file_type,
        )
    return FileListResponse(
        files=[FileResponse.model_validate(r) for r in rows],
        total=len(rows),
    )


@router.get("/files/usage", response_model=UsageResponse)
async def storage_usage(
    user_id: int = Depends(current_user_id),
    files: FileService = Depends(get_file_service),
) -> UsageResponse:
    return UsageResponse(**await files.get_usage(user_id))


# ─── Download (presigned URL) ───────────────────────────────────────────

@router.get("/files/{file_id}/download", response_model=DownloadResponse)
async def download_file(
    file_id: str,
    user_id: int = Depends(current_user_id),
    files: FileService = Depends(get_file_service),
) -> DownloadResponse:
    return DownloadResponse(**await files.get_download_url(file_id, user_id))


@router.get("/files/download-by-path")
async def download_by_path(
    path: str,
    user_id: int = Depends(current_user_id),  # auth check; user must be logged in
    files: FileService = Depends(get_file_service),
):
    """Issue a presigned URL for an arbitrary S3 key (unregistered files)."""
    return files.get_download_url_by_path(path)


# ─── Deletion ───────────────────────────────────────────────────────────

@router.delete("/files/{file_id}", response_model=DeleteResponse)
async def delete_file(
    file_id: str,
    user_id: int = Depends(current_user_id),
    files: FileService = Depends(get_file_service),
) -> DeleteResponse:
    await files.delete_one(file_id, user_id)
    return DeleteResponse(status="deleted", file_id=file_id)


@router.delete("/files/job/{job_id}", response_model=DeleteResponse)
async def delete_job_files(
    job_id: str,
    user_id: int = Depends(current_user_id),
    files: FileService = Depends(get_file_service),
) -> DeleteResponse:
    count = await files.delete_for_job(job_id)
    return DeleteResponse(
        status="deleted",
        job_id=job_id,
        files_deleted=count,
    )


# ─── Presigned upload ───────────────────────────────────────────────────

@router.post("/files/presign", response_model=PresignResponse)
async def presign_upload(
    user_id: int = Depends(current_user_id),
    files: FileService = Depends(get_file_service),
) -> PresignResponse:
    return PresignResponse(**files.get_upload_url(user_id))
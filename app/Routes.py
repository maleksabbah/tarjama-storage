"""
Storage Service Routes (S3 version)
File registry API — register, list, download via presigned URLs, delete.
"""
import os
import uuid
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from app import Database as db
from app import S3_client as s3
from app.Config import config

router = APIRouter()


def get_user_id(request: Request) -> int:
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=401, detail="Missing X-User-ID header")
    return int(user_id)


# =============================================================================
# File registration
# =============================================================================

@router.post("/files/register")
async def register_file(request: Request):
    """Register a file in the database. Called by workers after uploading to S3."""
    body = await request.json()

    required = ["job_id", "user_id", "category", "type", "path"]
    for field in required:
        if field not in body:
            raise HTTPException(status_code=400, detail=f"Missing field: {field}")

    # Get file size from S3 if not provided
    size_bytes = body.get("size_bytes", 0)
    if not size_bytes:
        try:
            size_bytes = s3.get_file_size(body["path"])
        except Exception:
            pass

    file_record = await db.register_file(
        job_id=body["job_id"],
        user_id=body["user_id"],
        category=body["category"],
        file_type=body["type"],
        path=body["path"],
        size_bytes=size_bytes,
        mime_type=body.get("mime_type"),
        expiry_days=body.get("expiry_days", config.DEFAULT_EXPIRY_DAYS),
    )

    return file_record


# =============================================================================
# File listing
# =============================================================================

@router.get("/files")
async def list_files(request: Request, job_id: str = None,
                     user_id: int = None, category: str = None,
                     file_type: str = None):
    uid = user_id or get_user_id(request)

    if job_id:
        files = await db.get_files_by_job(job_id, user_id=uid)
    else:
        files = await db.get_files_by_user(uid, category=category, file_type=file_type)

    return {"files": files, "total": len(files)}


@router.get("/files/usage")
async def storage_usage(request: Request):
    user_id = get_user_id(request)
    return await db.get_user_storage_usage(user_id)


# =============================================================================
# File download (via presigned URL)
# =============================================================================

@router.get("/files/{file_id}/download")
async def download_file(file_id: str, request: Request):
    """Get a presigned download URL for a file."""
    user_id = get_user_id(request)

    file_record = await db.get_file(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    if file_record["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your file")

    s3_key = file_record["path"]
    if not s3.file_exists(s3_key):
        raise HTTPException(status_code=404, detail="File not found in storage")

    # Return presigned URL — user downloads directly from S3
    url = s3.get_presigned_url(s3_key, expires_in=3600)
    return {"download_url": url, "file_id": file_id, "expires_in": 3600}


# =============================================================================
# File deletion
# =============================================================================

@router.delete("/files/{file_id}")
async def delete_file(file_id: str, request: Request):
    user_id = get_user_id(request)

    file_record = await db.get_file(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    if file_record["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your file")

    # Delete from S3
    try:
        s3.delete_file(file_record["path"])
    except Exception:
        pass

    # Delete from database
    await db.delete_file(file_id)

    return {"status": "deleted", "file_id": file_id}


@router.delete("/files/job/{job_id}")
async def delete_job_files(job_id: str, request: Request):
    user_id = get_user_id(request)

    # Delete all files for this job from S3
    prefixes = [
        f"uploads/{job_id}/",
        f"audio/{job_id}/",
        f"chunks/{job_id}/",
        f"results/{job_id}/",
    ]
    for prefix in prefixes:
        s3.delete_prefix(prefix)

    # Delete from database
    count = await db.delete_files_by_job(job_id)

    return {"status": "deleted", "job_id": job_id, "files_deleted": count}


# =============================================================================
# Presigned upload URL
# =============================================================================

@router.post("/files/presign")
async def presign_upload(request: Request, user_id: int = None):
    """Generate a presigned URL for direct upload to S3."""
    uid = user_id or get_user_id(request)

    upload_id = str(uuid.uuid4())
    s3_key = f"uploads/{upload_id}/video.mp4"

    upload_url = s3.get_presigned_upload_url(s3_key, expires_in=3600)

    return {
        "upload_url": upload_url,
        "upload_id": upload_id,
        "s3_key": s3_key,
        "user_id": uid,
        "expires_in": 3600,
    }

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict


class FileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_id: str
    user_id: int
    category: str
    type: str
    path: str
    size_bytes: int
    mime_type: Optional[str]
    created_at: datetime
    expires_at: Optional[datetime]


class FileListResponse(BaseModel):
    files: List[FileResponse]
    total: int


class UsageResponse(BaseModel):
    file_count: int
    total_bytes: int
    total_mb: float


class PresignResponse(BaseModel):
    upload_url: str
    upload_id: str
    s3_key: str
    user_id: int
    expires_in: int


class DownloadResponse(BaseModel):
    download_url: str
    file_id: str
    expires_in: int


class DeleteResponse(BaseModel):
    status: str
    file_id: Optional[str] = None
    job_id: Optional[str] = None
    files_deleted: Optional[int] = None
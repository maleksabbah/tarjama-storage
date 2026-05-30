from typing import Optional
from pydantic import BaseModel


class RegisterFileRequest(BaseModel):
    job_id: str
    user_id: int
    category: str
    type: str
    path: str
    size_bytes: Optional[int] = 0
    mime_type: Optional[str] = None
    expiry_days: Optional[int] = None


class ListFilesQuery(BaseModel):
    job_id: Optional[str] = None
    user_id: Optional[int] = None
    category: Optional[str] = None
    file_type: Optional[str] = None
    limit: int = 50
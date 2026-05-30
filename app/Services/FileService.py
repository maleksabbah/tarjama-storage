# storage: app/Services/FileService.py — only the register method changes
# (everything else from before stays the same)

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
    # If the worker is retrying after a crash, return the existing row.
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
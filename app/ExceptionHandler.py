# app/ExceptionHandler.py
"""
Domain exceptions and FastAPI handlers.
Services raise these; the handler translates them into HTTP responses.
"""
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


# ─── Base ───────────────────────────────────────────────────────────────

class DomainException(Exception):
    status_code: int = 400
    code: str = "domain_error"
    message: str = "Domain error"

    def __init__(
        self,
        message: Optional[str] = None,
        **extra,
    ):
        self.message = message or self.message
        self.extra = extra
        super().__init__(self.message)

    def to_payload(self) -> dict:
        payload = {"code": self.code, "detail": self.message}
        payload.update(self.extra)
        return payload


# ─── File errors ────────────────────────────────────────────────────────

class FileNotFound(DomainException):
    status_code = 404
    code = "file_not_found"
    message = "File not found"

    def __init__(self, file_id: str):
        super().__init__(f"File {file_id} not found", file_id=file_id)


class FileForbidden(DomainException):
    status_code = 403
    code = "file_forbidden"
    message = "Not your file"

    def __init__(self, file_id: str):
        super().__init__(f"File {file_id} is not yours", file_id=file_id)


class FileMissingInStorage(DomainException):
    status_code = 404
    code = "file_missing_in_storage"
    message = "File registered in DB but missing from S3"

    def __init__(self, file_id: str):
        super().__init__(
            f"File {file_id} is registered but not in storage",
            file_id=file_id,
        )


# ─── Auth errors ────────────────────────────────────────────────────────

class MissingUserHeader(DomainException):
    status_code = 401
    code = "missing_user_header"
    message = "Missing X-User-ID header"


# ─── Validation errors ──────────────────────────────────────────────────

class MissingField(DomainException):
    status_code = 400
    code = "missing_field"

    def __init__(self, field: str):
        super().__init__(f"Missing field: {field}", field=field)


# ─── Handler registration ───────────────────────────────────────────────

def register_exception_handlers(app: FastAPI) -> None:

    @app.exception_handler(DomainException)
    async def _domain_handler(request: Request, exc: DomainException):
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.to_payload(),
        )

    @app.exception_handler(Exception)
    async def _global_handler(request: Request, exc: Exception):
        # Don't leak internal errors. Log and return 500.
        print(f"  [STORAGE] Unhandled error: {exc!r}")
        return JSONResponse(
            status_code=500,
            content={"code": "internal_error", "detail": "Internal server error"},
        )
# storage: app/Config/Config.py
"""
Storage service config.
File registry (Postgres) + S3/MinIO lifecycle.

S3 credentials are read directly from env in Repositories/S3Client.py.
This Config holds DB + cleanup settings only.
"""
import os


def _async_url(url: str) -> str:
    """Ensure the URL uses the asyncpg driver for SQLAlchemy async engine."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


class Config:
    # Database (force asyncpg driver regardless of how the env passes it)
    DATABASE_URL: str = _async_url(os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@postgres:5432/storage_db",
    ))
    DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "2"))
    DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

    # File lifecycle / cleanup
    DEFAULT_EXPIRY_DAYS: int = int(os.getenv("DEFAULT_EXPIRY_DAYS", "7"))
    CLEANUP_INTERVAL_HOURS: int = int(os.getenv("CLEANUP_INTERVAL_HOURS", "6"))


config = Config()
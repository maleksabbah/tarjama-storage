# storage: app/Config/Config.py
"""
Storage service config.
File registry (Postgres) + S3/MinIO lifecycle.

NOTE: S3 credentials are read directly from env in Repositories/S3Client.py
(S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_REGION,
S3_PUBLIC_ENDPOINT). This Config holds DB + cleanup settings only.
"""
import os


class Config:
    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@postgres:5432/storage_db",
    )
    DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "2"))
    DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

    # File lifecycle / cleanup
    DEFAULT_EXPIRY_DAYS: int = int(os.getenv("DEFAULT_EXPIRY_DAYS", "7"))
    CLEANUP_INTERVAL_HOURS: int = int(os.getenv("CLEANUP_INTERVAL_HOURS", "6"))


config = Config()
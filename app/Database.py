"""
Storage Service Database
asyncpg connection pool + raw SQL for the files table.
"""
import uuid
from datetime import datetime, timedelta, timezone
import asyncpg
from app.Config import config


pool: asyncpg.Pool = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(
        config.DATABASE_URL,
        min_size=config.DB_POOL_MIN,
        max_size=config.DB_POOL_MAX,
    )

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id VARCHAR(36) PRIMARY KEY,
                job_id VARCHAR(36) NOT NULL,
                user_id INTEGER NOT NULL,
                category VARCHAR(50) NOT NULL,
                type VARCHAR(50) NOT NULL,
                path TEXT NOT NULL,
                size_bytes BIGINT DEFAULT 0,
                mime_type VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_job_id ON files(job_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_user_id ON files(user_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_category ON files(category)
        """)


async def close_db():
    global pool
    if pool:
        await pool.close()


def gen_id() -> str:
    return str(uuid.uuid4())


# =============================================================================
# File queries
# =============================================================================

async def register_file(job_id: str, user_id: int, category: str,
                        file_type: str, path: str, size_bytes: int = 0,
                        mime_type: str = None, expiry_days: int = None) -> dict:
    """Register a file in the database."""
    file_id = gen_id()
    expires_at = None
    if expiry_days:
        expires_at = datetime.utcnow() + timedelta(days=expiry_days)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO files (id, job_id, user_id, category, type, path,
                              size_bytes, mime_type, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            """,
            file_id, job_id, user_id, category, file_type, path,
            size_bytes, mime_type, expires_at,
        )
        return dict(row)


async def get_file(file_id: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM files WHERE id = $1", file_id)
        return dict(row) if row else None


async def get_files_by_job(job_id: str, user_id: int = None) -> list:
    async with pool.acquire() as conn:
        if user_id:
            rows = await conn.fetch(
                "SELECT * FROM files WHERE job_id = $1 AND user_id = $2 ORDER BY created_at",
                job_id, user_id,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM files WHERE job_id = $1 ORDER BY created_at",
                job_id,
            )
        return [dict(r) for r in rows]


async def get_files_by_user(user_id: int, category: str = None,
                            file_type: str = None, limit: int = 50) -> list:
    query = "SELECT * FROM files WHERE user_id = $1"
    params = [user_id]
    idx = 2

    if category:
        query += f" AND category = ${idx}"
        params.append(category)
        idx += 1

    if file_type:
        query += f" AND type = ${idx}"
        params.append(file_type)
        idx += 1

    query += f" ORDER BY created_at DESC LIMIT ${idx}"
    params.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(r) for r in rows]


async def get_user_storage_usage(user_id: int) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) as file_count, COALESCE(SUM(size_bytes), 0) as total_bytes
            FROM files WHERE user_id = $1
            """,
            user_id,
        )
        return {
            "file_count": row["file_count"],
            "total_bytes": row["total_bytes"],
            "total_mb": round(row["total_bytes"] / (1024 * 1024), 2),
        }


async def delete_file(file_id: str) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM files WHERE id = $1", file_id)
        return result == "DELETE 1"


async def delete_files_by_job(job_id: str) -> int:
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM files WHERE job_id = $1", job_id)
        count = int(result.split(" ")[1])
        return count


async def get_expired_files() -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM files WHERE expires_at IS NOT NULL AND expires_at < NOW()"
        )
        return [dict(r) for r in rows]
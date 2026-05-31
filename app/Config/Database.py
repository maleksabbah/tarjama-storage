# app/Config/Database.py
"""
Async SQLAlchemy engine + session factory.
"""
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.Config.Config import config


engine = create_async_engine(
    config.DATABASE_URL,
    pool_size=config.DB_POOL_MIN,
    max_overflow=config.DB_POOL_MAX - config.DB_POOL_MIN,
    pool_pre_ping=True,
    echo=False,
)


SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def close_db() -> None:
    await engine.dispose()
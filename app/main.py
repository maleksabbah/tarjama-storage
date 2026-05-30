# app/main.py
"""
ASR Storage Service.
File registry + S3 lifecycle. HTTP server (FastAPI) on port 8002.
Trusts X-User-ID header from gateway (gateway has already verified the JWT).

Run:
  uvicorn app.main:app --host 0.0.0.0 --port 8002 --reload
"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.Config.Config import config
from app.Config.Database import close_db
from app.Dependencies import session_scope
from app.ExceptionHandler import register_exception_handlers
from app.Repositories import FileRepository, S3Client
from app.Routes import file_router
from app.Services import CleanupService


# ─── Cleanup loop ───────────────────────────────────────────────────────

async def cleanup_loop(service: CleanupService) -> None:
    interval = config.CLEANUP_INTERVAL_HOURS * 3600
    print(f"  [CLEANUP] Running every {config.CLEANUP_INTERVAL_HOURS} hours")
    while True:
        try:
            async with session_scope() as session:
                deleted = await service.cleanup_once(FileRepository(session))
                if deleted:
                    print(f"  [CLEANUP] Deleted {deleted} expired files")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"  [CLEANUP] Error: {e}")
        await asyncio.sleep(interval)


# ─── Lifespan ───────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting Storage Service...")
    app.state.s3_client = S3Client()

    cleanup_service = CleanupService(s3=app.state.s3_client)
    cleanup_task = asyncio.create_task(cleanup_loop(cleanup_service))
    print("  Cleanup scheduler started")
    print("Storage Service ready.")

    try:
        yield
    finally:
        # Shutdown
        print("Shutting down Storage Service...")
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        await close_db()
        print("Storage Service stopped.")


# ─── App ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Tarjama Storage Service",
    description="File Registry and Storage Management",
    version="2.0.0",
    lifespan=lifespan,
)

register_exception_handlers(app)
app.include_router(file_router)


# ─── Health & root ──────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "storage"}


@app.get("/")
async def root() -> dict:
    return {
        "service": "Tarjama Storage Service",
        "version": "2.0.0",
        "docs": "/docs",
    }
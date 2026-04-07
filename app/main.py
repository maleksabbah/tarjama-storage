"""
ASR Storage Service
====================
File registry — tracks where files are stored, manages cleanup and downloads.
Runs as an HTTP server (FastAPI).

Run:
  uvicorn app.main:app --host 0.0.0.0 --port 8002 --reload
"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.Config import config
from app.Database import init_db, close_db
from app.Cleanup import cleanup_loop
from app.Routes import router


cleanup_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cleanup_task

    print("Starting Storage Service...")
    await init_db()
    print("  PostgreSQL connected")

    cleanup_task = asyncio.create_task(cleanup_loop())
    print("  Cleanup scheduler started")
    print("Storage Service ready.")

    yield

    print("Shutting down Storage Service...")
    if cleanup_task:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
    await close_db()
    print("Storage Service stopped.")


app = FastAPI(
    title="ASR Storage Service",
    description="File Registry and Storage Management",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "storage"}


@app.get("/")
async def root():
    return {
        "service": "ASR Storage Service",
        "version": "1.0.0",
        "docs": "/docs",
    }
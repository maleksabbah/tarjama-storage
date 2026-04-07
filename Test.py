"""
Storage Service Unit Tests
============================
Tests routes, database queries, and cleanup logic.

Run:
  pytest Test.py -v
"""

from app.main import app as fastapi_app

# Explicit imports for patch
import app.Database
import app.Routes
import app.Cleanup


import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient, ASGITransport


# =============================================================================
# Fixtures
# =============================================================================

@pytest_asyncio.fixture
async def client():
    with patch("app.main.init_db", new_callable=AsyncMock), \
         patch("app.main.close_db", new_callable=AsyncMock), \
         patch("app.main.cleanup_loop", new_callable=AsyncMock):

        transport = ASGITransport(app=fastapi_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

def mock_file(file_id="f_001", job_id="j_123", user_id=1,
              category="output", file_type="transcript_json"):
    return {
        "id": file_id,
        "job_id": job_id,
        "user_id": user_id,
        "category": category,
        "type": file_type,
        "path": f"storage/results/{job_id}/transcript.json",
        "size_bytes": 2048,
        "mime_type": "application/json",
        "created_at": "2026-03-14T00:00:00",
        "expires_at": "2026-03-21T00:00:00",
    }


# =============================================================================
# Health endpoints
# =============================================================================

@pytest.mark.asyncio
class TestHealthEndpoints:
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["service"] == "storage"

    async def test_root(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200
        assert resp.json()["service"] == "ASR Storage Service"


# =============================================================================
# File registration
# =============================================================================

@pytest.mark.asyncio
class TestRegisterFile:
    async def test_register_success(self, client):
        file_record = mock_file()

        with patch("app.Routes.db.register_file", new_callable=AsyncMock, return_value=file_record):
            resp = await client.post("/files/register", json={
                "job_id": "j_123",
                "user_id": 1,
                "category": "output",
                "type": "transcript_json",
                "path": "storage/results/j_123/transcript.json",
                "size_bytes": 2048,
            })
            assert resp.status_code == 200
            assert resp.json()["id"] == "f_001"

    async def test_register_missing_field(self, client):
        resp = await client.post("/files/register", json={
            "job_id": "j_123",
            # missing user_id, category, type, path
        })
        assert resp.status_code == 400
        assert "Missing field" in resp.json()["detail"]


# =============================================================================
# File listing
# =============================================================================

@pytest.mark.asyncio
class TestListFiles:
    async def test_list_by_job(self, client):
        files = [mock_file("f_001"), mock_file("f_002", file_type="subtitle_srt")]

        with patch("app.Routes.db.get_files_by_job", new_callable=AsyncMock, return_value=files):
            resp = await client.get("/files?job_id=j_123", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            assert resp.json()["total"] == 2

    async def test_list_by_user(self, client):
        files = [mock_file("f_001"), mock_file("f_002"), mock_file("f_003")]

        with patch("app.Routes.db.get_files_by_user", new_callable=AsyncMock, return_value=files):
            resp = await client.get("/files", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            assert resp.json()["total"] == 3

    async def test_list_missing_user_header(self, client):
        resp = await client.get("/files")
        assert resp.status_code == 401


# =============================================================================
# Storage usage
# =============================================================================

@pytest.mark.asyncio
class TestStorageUsage:
    async def test_get_usage(self, client):
        usage = {"file_count": 15, "total_bytes": 52428800, "total_mb": 50.0}

        with patch("app.Routes.db.get_user_storage_usage", new_callable=AsyncMock, return_value=usage):
            resp = await client.get("/files/usage", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["file_count"] == 15
            assert data["total_mb"] == 50.0


# =============================================================================
# File download
# =============================================================================

@pytest.mark.asyncio
class TestDownloadFile:
    async def test_download_not_found(self, client):
        with patch("app.Routes.db.get_file", new_callable=AsyncMock, return_value=None):
            resp = await client.get("/files/f_999/download", headers={"X-User-ID": "1"})
            assert resp.status_code == 404

    async def test_download_wrong_user(self, client):
        file_record = mock_file(user_id=1)

        with patch("app.Routes.db.get_file", new_callable=AsyncMock, return_value=file_record):
            resp = await client.get("/files/f_001/download", headers={"X-User-ID": "999"})
            assert resp.status_code == 403

    async def test_download_file_missing_on_disk(self, client):
        file_record = mock_file(user_id=1)

        with patch("app.Routes.db.get_file", new_callable=AsyncMock, return_value=file_record), \
             patch("os.path.exists", return_value=False):
            resp = await client.get("/files/f_001/download", headers={"X-User-ID": "1"})
            assert resp.status_code == 404


# =============================================================================
# File deletion
# =============================================================================

@pytest.mark.asyncio
class TestDeleteFile:
    async def test_delete_success(self, client):
        file_record = mock_file(user_id=1)

        with patch("app.Routes.db.get_file", new_callable=AsyncMock, return_value=file_record), \
             patch("app.Routes.db.delete_file", new_callable=AsyncMock, return_value=True), \
             patch("os.path.exists", return_value=True), \
             patch("os.remove"):
            resp = await client.delete("/files/f_001", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            assert resp.json()["status"] == "deleted"

    async def test_delete_not_found(self, client):
        with patch("app.Routes.db.get_file", new_callable=AsyncMock, return_value=None):
            resp = await client.delete("/files/f_999", headers={"X-User-ID": "1"})
            assert resp.status_code == 404

    async def test_delete_wrong_user(self, client):
        file_record = mock_file(user_id=1)

        with patch("app.Routes.db.get_file", new_callable=AsyncMock, return_value=file_record):
            resp = await client.delete("/files/f_001", headers={"X-User-ID": "999"})
            assert resp.status_code == 403


# =============================================================================
# Delete job files
# =============================================================================

@pytest.mark.asyncio
class TestDeleteJobFiles:
    async def test_delete_all_job_files(self, client):
        files = [mock_file("f_001"), mock_file("f_002")]

        with patch("app.Routes.db.get_files_by_job", new_callable=AsyncMock, return_value=files), \
             patch("app.Routes.db.delete_files_by_job", new_callable=AsyncMock, return_value=2), \
             patch("os.path.exists", return_value=True), \
             patch("os.remove"):
            resp = await client.delete("/files/job/j_123", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            assert resp.json()["files_deleted"] == 2


# =============================================================================
# Presign upload
# =============================================================================

@pytest.mark.asyncio
class TestPresignUpload:
    async def test_presign_returns_path(self, client):
        with patch("os.makedirs"):
            resp = await client.post("/files/presign?user_id=1", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            data = resp.json()
            assert "upload_id" in data
            assert "upload_path" in data
            assert data["user_id"] == 1


# =============================================================================
# Cleanup
# =============================================================================

@pytest.mark.asyncio
class TestCleanup:
    async def test_cleanup_deletes_expired(self):
        expired_files = [
            mock_file("f_old1", file_type="chunk"),
            mock_file("f_old2", file_type="chunk"),
        ]

        with patch("app.Cleanup.db.get_expired_files", new_callable=AsyncMock, return_value=expired_files), \
             patch("app.Cleanup.db.delete_file", new_callable=AsyncMock, return_value=True), \
             patch("os.path.exists", return_value=True), \
             patch("os.remove"):
            from app.Cleanup import cleanup_expired
            deleted = await cleanup_expired()
            assert deleted == 2

    async def test_cleanup_no_expired(self):
        with patch("app.Cleanup.db.get_expired_files", new_callable=AsyncMock, return_value=[]):
            from app.Cleanup import cleanup_expired
            deleted = await cleanup_expired()
            assert deleted == 0

    async def test_cleanup_handles_missing_file(self):
        expired_files = [mock_file("f_old1")]

        with patch("app.Cleanup.db.get_expired_files", new_callable=AsyncMock, return_value=expired_files), \
             patch("app.Cleanup.db.delete_file", new_callable=AsyncMock, return_value=True), \
             patch("os.path.exists", return_value=False):
            from app.Cleanup import cleanup_expired
            deleted = await cleanup_expired()
            assert deleted == 1  # still deletes from DB even if not on disk
"""
Cleanup policy: find expired files, delete from S3, delete from DB.
One pass per call. Caller (main.py) owns the loop and the session lifecycle.
"""
from app.Repositories import FileRepository, S3Client


class CleanupService:
    def __init__(self,
                 s3: S3Client):
        self.s3 = s3
    async def cleanup_once(self,files:FileRepository) -> int:
        expired = await files.list_expired()
        if not expired:
            return 0
        deleted = 0

        for file in expired:
            try:
                self.s3.delete_file(file.path)
            except Exception as e:
                print(f"  [CLEANUP] S3 delete failed for {file.path}: {e}")
            await files.delete(file.path)
            deleted += 1
        return deleted




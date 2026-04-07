"""
Storage Cleanup (S3 version)
Periodically deletes expired files from S3 and database.
"""
import asyncio
from app.Config import config
from app import Database as db
from app import S3_client as s3


async def cleanup_expired():
    """Find and delete all expired files."""
    expired = await db.get_expired_files()
    if not expired:
        return 0

    deleted = 0
    for file_record in expired:
        s3_key = file_record["path"]
        file_id = file_record["id"]

        # Delete from S3
        try:
            s3.delete_file(s3_key)
        except Exception as e:
            print(f"  [CLEANUP] Failed to delete from S3 {s3_key}: {e}")

        # Delete from database
        await db.delete_file(file_id)
        deleted += 1

    return deleted


async def cleanup_loop():
    """Run cleanup on a schedule."""
    interval = config.CLEANUP_INTERVAL_HOURS * 3600
    print(f"  [CLEANUP] Running every {config.CLEANUP_INTERVAL_HOURS} hours")

    while True:
        try:
            deleted = await cleanup_expired()
            if deleted > 0:
                print(f"  [CLEANUP] Deleted {deleted} expired files")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"  [CLEANUP] Error: {e}")

        await asyncio.sleep(interval)




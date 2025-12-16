import asyncio
import os
from app.services.backups.base import BackupHandler

class MongoDBBackupHandler(BackupHandler):
    @property
    def file_extension(self) -> str:
        return ".archive"

    async def backup(self, file_path: str, progress_callback=None):
        # mongodump --uri="mongodb://user:pass@host:port/db" --archive=file_path
        uri = f"mongodb://{self.connection_details['username']}:{self.connection_details['password']}@{self.connection_details['host']}:{self.connection_details['port']}/{self.connection_details['database_name']}?authSource=admin"
        
        cmd = [
            "mongodump",
            "--uri", uri,
            "--archive=" + file_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=asyncio.subprocess.PIPE
        )

        async def monitor():
            while True:
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    if progress_callback:
                        progress_callback("Dumping", {"file_size": size})
                await asyncio.sleep(1)

        monitor_task = None
        if progress_callback:
            monitor_task = asyncio.create_task(monitor())

        try:
            _, stderr = await process.communicate()
        finally:
            if monitor_task:
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass
        
        if process.returncode != 0:
            raise Exception(f"MongoDB Backup failed: {stderr.decode()}")

    async def restore(self, file_path: str):
        # mongorestore --uri="mongodb://..." --archive=file_path
        uri = f"mongodb://{self.connection_details['username']}:{self.connection_details['password']}@{self.connection_details['host']}:{self.connection_details['port']}/{self.connection_details['database_name']}?authSource=admin"
        
        cmd = [
            "mongorestore",
            "--uri", uri,
            "--archive=" + file_path,
            "--drop" # Drops collections before restoring
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"MongoDB Restore failed: {stderr.decode()}")

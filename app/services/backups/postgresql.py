import asyncio
import os
from app.services.backups.base import BackupHandler

class PostgreSQLBackupHandler(BackupHandler):
    @property
    def file_extension(self) -> str:
        return ".sql"

    async def backup(self, file_path: str):
        # PGPASSWORD=password pg_dump -h host -p port -U user -d dbname -f file_path
        env = os.environ.copy()
        env['PGPASSWORD'] = self.connection_details['password']
        
        cmd = [
            "pg_dump",
            "-h", self.connection_details['host'],
            "-p", str(self.connection_details['port']),
            "-U", self.connection_details['username'],
            "-d", self.connection_details['database_name'],
            "-f", file_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        _, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"PostgreSQL Backup failed: {stderr.decode()}")

    async def restore(self, file_path: str):
        # PGPASSWORD=password psql -h host -p port -U user -d dbname -f file_path
        env = os.environ.copy()
        env['PGPASSWORD'] = self.connection_details['password']
        
        cmd = [
            "psql",
            "-h", self.connection_details['host'],
            "-p", str(self.connection_details['port']),
            "-U", self.connection_details['username'],
            "-d", self.connection_details['database_name'],
            "-f", file_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        _, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"PostgreSQL Restore failed: {stderr.decode()}")

import asyncio
import os
from app.services.backups.base import BackupHandler

class MySQLBackupHandler(BackupHandler):
    @property
    def file_extension(self) -> str:
        return ".sql"

    async def backup(self, file_path: str):
        # mysqldump -h host -P port -u user -p'password' dbname > file_path
        # Note: Sending password via env var MYSQL_PWD is safer than command line
        env = os.environ.copy()
        env['MYSQL_PWD'] = self.connection_details['password']
        
        cmd = [
            "mysqldump",
            "-h", self.connection_details['host'],
            "-P", str(self.connection_details['port']),
            "-u", self.connection_details['username'],
            self.connection_details['database_name']
        ]
        
        with open(file_path, "w") as f:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=f,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"MySQL Backup failed: {stderr.decode()}")

    async def restore(self, file_path: str):
        # mysql -h host -P port -u user -p'password' dbname < file_path
        env = os.environ.copy()
        env['MYSQL_PWD'] = self.connection_details['password']
        
        cmd = [
            "mysql",
            "-h", self.connection_details['host'],
            "-P", str(self.connection_details['port']),
            "-u", self.connection_details['username'],
            self.connection_details['database_name']
        ]
        
        with open(file_path, "r") as f:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=f,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"MySQL Restore failed: {stderr.decode()}")

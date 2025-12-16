import asyncio
import os
from app.services.backups.base import BackupHandler

class MySQLBackupHandler(BackupHandler):
    @property
    def file_extension(self) -> str:
        return ".sql"

    async def backup(self, file_path: str, progress_callback=None):
        # mysqldump -h host -P port -u user -p'password' dbname > file_path
        # Note: Sending password via env var MYSQL_PWD is safer than command line
        env = os.environ.copy()
        env['MYSQL_PWD'] = self.connection_details['password']
        
        cmd = [
            "mysqldump",
            "-h", self.connection_details['host'],
            "-P", str(self.connection_details['port']),
            "-u", self.connection_details['username']
        ]

        if self.connection_details.get('database_name'):
             cmd.append(self.connection_details['database_name'])
        else:
             cmd.append("--all-databases")
        
        with open(file_path, "w") as f:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=f,
                stderr=asyncio.subprocess.PIPE,
                env=env
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

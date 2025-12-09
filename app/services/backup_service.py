import os
import subprocess
import tempfile
import json
import asyncio
from datetime import datetime
from app.core.storage import get_storage_backend
from app.utils.security import decrypt_password

class BackupService:
    def __init__(self, database):
        self.database = database
        self.storage = get_storage_backend()
        self.connection_details = {
            "host": database.host,
            "port": database.port,
            "database_name": database.database_name,
            "username": database.username,
            "password": decrypt_password(database.password)
        }

    async def backup(self) -> str:
        """
        Performs a backup of the database and uploads it to storage.
        Returns the filename of the backup.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.database.type.lower()}_{self.database.name}_{timestamp}"
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            if self.database.type == "MySQL":
                filename += ".sql"
                await self._backup_mysql(temp_path)
            elif self.database.type == "PostgreSQL":
                filename += ".sql"
                await self._backup_postgresql(temp_path)
            elif self.database.type == "MongoDB":
                filename += ".archive"
                await self._backup_mongodb(temp_path)
            # Add other types here
            else:
                 raise NotImplementedError(f"Backup for {self.database.type} not implemented")

            # Upload to storage
            uploaded_filename = self.storage.upload(temp_path, filename)
            return uploaded_filename
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    async def restore(self, backup_filename: str):
        """
        Restores the database from a backup file in storage.
        """
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            # Download from storage
            self.storage.download(backup_filename, temp_path)
            
            if self.database.type == "MySQL":
                await self._restore_mysql(temp_path)
            elif self.database.type == "PostgreSQL":
                await self._restore_postgresql(temp_path)
            elif self.database.type == "MongoDB":
                await self._restore_mongodb(temp_path)
            else:
                 raise NotImplementedError(f"Restore for {self.database.type} not implemented")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    async def _backup_mysql(self, file_path: str):
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

    async def _restore_mysql(self, file_path: str):
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

    async def _backup_postgresql(self, file_path: str):
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

    async def _restore_postgresql(self, file_path: str):
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

    async def _backup_mongodb(self, file_path: str):
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
        _, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"MongoDB Backup failed: {stderr.decode()}")

    async def _restore_mongodb(self, file_path: str):
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

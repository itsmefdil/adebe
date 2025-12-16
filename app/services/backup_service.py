import os
import tempfile
from datetime import datetime
from app.core.storage import get_storage_backend
from app.utils.security import decrypt_password
from app.services.backups import get_backup_handler

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

    async def backup(self, progress_callback=None) -> str:
        """
        Performs a backup of the database and uploads it to storage.
        Returns the filename of the backup.
        """
        handler = get_backup_handler(self.database.type, self.connection_details)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Ensure we use the handler's extension
        db_name = self.database.database_name or "all_databases"
        filename = f"{self.database.type.lower()}_{db_name}_{timestamp}{handler.file_extension}"
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            if progress_callback:
                progress_callback("Starting Backup", {"step": "init"})

            await handler.backup(temp_path, progress_callback)

            if progress_callback:
                progress_callback("Uploading to Storage", {"step": "upload"})

            # Upload to storage
            uploaded_filename = self.storage.upload(temp_path, filename)
            
            if progress_callback:
                progress_callback("Completed", {"step": "done", "filename": uploaded_filename})
                
            return uploaded_filename
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    async def restore(self, backup_filename: str):
        """
        Restores the database from a backup file in storage.
        """
        handler = get_backup_handler(self.database.type, self.connection_details)
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            # Download from storage
            self.storage.download(backup_filename, temp_path)
            
            # Use specific handler for restoration
            await handler.restore(temp_path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    async def delete_backup(self, backup_filename: str):
        """
        Deletes a backup file from storage.
        """
        self.storage.delete(backup_filename)

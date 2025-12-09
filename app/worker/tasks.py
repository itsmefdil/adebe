import asyncio
from celery import shared_task
from datetime import datetime
from app.database import SessionLocal, ConnectionManager
from app.services.backup_service import BackupService

def params_to_sync(func):
    """Decorator to run async functions synchronously."""
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper

@shared_task(bind=True)
def backup_database(self, db_id: int):
    """
    Celery task to backup a database.
    """
    db = SessionLocal()
    try:
        manager = ConnectionManager(db)
        database = manager.get_connection(db_id)
        
        if not database:
            return {"status": "error", "message": "Database not found"}
        
        service = BackupService(database)
        
        # Run async backup method
        filename = asyncio.run(service.backup())
        
        return {"status": "success", "filename": filename}
        
    except Exception as e:
        # self.retry(exc=e) # Optional: retry logic
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@shared_task(bind=True)
def restore_database(self, db_id: int, backup_filename: str):
    """
    Celery task to restore a database from backup.
    """
    db = SessionLocal()
    try:
        manager = ConnectionManager(db)
        database = manager.get_connection(db_id)
        
        if not database:
            return {"status": "error", "message": "Database not found"}
        
        service = BackupService(database)
        
        # Run async restore method
        asyncio.run(service.restore(backup_filename))
        
        return {"status": "success", "message": "Database restored successfully"}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@shared_task(bind=True)
def export_table_task(self, db_id: int, table_name: str, format: str = 'csv'):
    """
    Celery task to export a table and upload to storage.
    """
    import os
    import tempfile
    from app.services.mysql_service import MySQLService
    from app.core.storage import get_storage_backend
    
    db = SessionLocal()
    temp_path = None
    try:
        manager = ConnectionManager(db)
        database = manager.get_connection(db_id)
        
        if not database:
            raise Exception("Database not found")
        
        # Determine filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"export_{database.name}_{table_name}_{timestamp}.{format}"
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as tmp:
            temp_path = tmp.name
            
        # Dispatch based on DB type (Currently only MySQL implemented)
        if database.type == "MySQL":
            service = MySQLService(database)
            service.export_table_to_file(table_name, temp_path, format)
        else:
            raise NotImplementedError(f"Export not implemented for {database.type}")
            
        # Upload to storage
        storage = get_storage_backend()
        storage.upload(temp_path, filename)
        
        return {"status": "success", "filename": filename}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        db.close()

@shared_task(bind=True)
def import_table_task(self, db_id: int, table_name: str, file_identifier: str, format: str = 'csv'):
    """
    Celery task to download file and import into table.
    """
    import os
    import tempfile
    from app.services.mysql_service import MySQLService
    from app.core.storage import get_storage_backend
    
    db = SessionLocal()
    temp_path = None
    try:
        manager = ConnectionManager(db)
        database = manager.get_connection(db_id)
        
        if not database:
            raise Exception("Database not found")
            
        storage = get_storage_backend()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as tmp:
            temp_path = tmp.name
        
        # Download
        storage.download(file_identifier, temp_path)
        
        rows_affected = 0
        if database.type == "MySQL":
            service = MySQLService(database)
            rows_affected = service.import_table_from_file(table_name, temp_path, format)
        else:
             raise NotImplementedError(f"Import not implemented for {database.type}")
             
        return {"status": "success", "rows_affected": rows_affected}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
         if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
         db.close()

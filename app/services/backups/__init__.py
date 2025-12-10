from .base import BackupHandler
from .mysql import MySQLBackupHandler
from .postgresql import PostgreSQLBackupHandler
from .mongodb import MongoDBBackupHandler
from typing import Dict, Any, Type

def get_backup_handler(db_type: str, connection_details: Dict[str, Any]) -> BackupHandler:
    handlers: Dict[str, Type[BackupHandler]] = {
        "MySQL": MySQLBackupHandler,
        "PostgreSQL": PostgreSQLBackupHandler,
        "MongoDB": MongoDBBackupHandler,
    }
    
    handler_class = handlers.get(db_type)
    if not handler_class:
        raise NotImplementedError(f"Backup handler for {db_type} not implemented")
        
    return handler_class(connection_details)

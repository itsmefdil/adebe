from abc import ABC, abstractmethod
from typing import Dict, Any

class BackupHandler(ABC):
    def __init__(self, connection_details: Dict[str, Any]):
        self.connection_details = connection_details

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """Return the default file extension for backups"""
        pass

    @abstractmethod
    async def backup(self, file_path: str):
        """Perform backup to the given file path"""
        pass

    @abstractmethod
    async def restore(self, file_path: str):
        """Restore from the given file path"""
        pass

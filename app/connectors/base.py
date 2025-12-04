from abc import ABC, abstractmethod

class BaseConnector(ABC):
    def __init__(self, connection_details: dict):
        self.connection_details = connection_details
        self.connection = None

    @abstractmethod
    def connect(self):
        """Establish connection to the database."""
        pass

    @abstractmethod
    def close(self):
        """Close the connection."""
        pass

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """Test if the connection is valid. Returns (success, message)."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str):
        """Execute a raw query."""
        pass

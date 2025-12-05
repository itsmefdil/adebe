from pymongo import MongoClient
from .base import BaseConnector

class MongoConnector(BaseConnector):
    def connect(self):
        host = self.connection_details.get("host")
        port = self.connection_details.get("port")
        
        # Handle port 0 or None by defaulting to 27017
        if not port:
            port = 27017
        else:
            try:
                port = int(port)
                if port == 0:
                    port = 27017
            except ValueError:
                port = 27017

        username = self.connection_details.get("username")
        password = self.connection_details.get("password")
        database_name = self.connection_details.get("database_name")
        
        # Only use auth if both username and password are provided and not empty
        if username and password:
            # URL encode username and password to handle special characters
            import urllib.parse
            username = urllib.parse.quote_plus(username)
            password = urllib.parse.quote_plus(password)
            if database_name:
                uri = f"mongodb://{username}:{password}@{host}:{port}/{database_name}"
            else:
                uri = f"mongodb://{username}:{password}@{host}:{port}/"
        else:
            if database_name:
                uri = f"mongodb://{host}:{port}/{database_name}"
            else:
                uri = f"mongodb://{host}:{port}/"
            
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.connection = self.client

    def close(self):
        if hasattr(self, 'client') and self.client:
            self.client.close()
            self.client = None

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.connect()
            # The ping command is cheap and effectively tests connectivity
            self.client.admin.command('ping')
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def execute_query(self, query: str):
        # MongoDB queries are complex to represent as a single string in this generic interface
        # For now, we'll assume the query is a JSON string or handle basic commands
        return {"error": "Raw query execution not fully supported for MongoDB in this demo"}

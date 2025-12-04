from pymongo import MongoClient
from .base import BaseConnector

class MongoConnector(BaseConnector):
    def connect(self):
        host = self.connection_details.get("host")
        port = self.connection_details.get("port", 27017)
        username = self.connection_details.get("username")
        password = self.connection_details.get("password")
        
        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/"
        else:
            uri = f"mongodb://{host}:{port}/"
            
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.connection = self.client

    def close(self):
        if self.client:
            self.client.close()

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.connect()
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

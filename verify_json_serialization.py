
from app.services.mongo_service import MongoService
from unittest.mock import MagicMock, patch
from bson import ObjectId
from datetime import datetime

def test_json_serialization():
    print("Testing MongoService JSON serialization...")
    
    # Mock database object
    mock_db = MagicMock()
    mock_db.host = "localhost"
    mock_db.port = 27017
    mock_db.database_name = "test_db"
    mock_db.username = "user"
    mock_db.password = "encrypted_pass"
    
    with patch('app.services.mongo_service.decrypt_password', return_value="password"), \
         patch('app.services.mongo_service.MongoConnector') as MockConnector:
        
        # Setup mock connector and client
        mock_connector_instance = MockConnector.return_value
        mock_client = MagicMock()
        mock_connector_instance.client = mock_client
        
        # Mock database commands
        mock_mongo_db = MagicMock()
        mock_client.__getitem__.return_value = mock_mongo_db
        mock_connector_instance.client.get_database.return_value = mock_mongo_db
        
        # Initialize service
        service = MongoService(mock_db)
        service.db = mock_mongo_db 
        
        # Mock document with datetime
        mock_doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "name": "Test Document",
            "created_at": datetime(2023, 1, 1, 12, 0, 0),
            "nested": {
                "date": datetime(2023, 1, 2, 12, 0, 0)
            },
            "list": [datetime(2023, 1, 3, 12, 0, 0)]
        }
        
        # Mock find to return cursor with doc
        mock_cursor = MagicMock()
        mock_cursor.skip.return_value.limit.return_value = [mock_doc]
        mock_mongo_db.__getitem__.return_value.find.return_value = mock_cursor
        mock_mongo_db.__getitem__.return_value.count_documents.return_value = 1
        
        # Test browse_collection
        result = service.browse_collection("test_collection", 1, 10)
        
        doc = result['documents'][0]
        print("Serialized Doc:", doc)
        
        # Verify serialization
        if isinstance(doc['created_at'], str) and "2023-01-01" in doc['created_at']:
            print("Test Passed: created_at is string")
        else:
            print(f"Test Failed: created_at is {type(doc['created_at'])}")
            
        if isinstance(doc['nested']['date'], str):
             print("Test Passed: nested date is string")
        else:
             print(f"Test Failed: nested date is {type(doc['nested']['date'])}")

if __name__ == "__main__":
    test_json_serialization()

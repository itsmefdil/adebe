from app.connectors.mongo_connector import MongoConnector
from app.utils.security import decrypt_password
import math
from bson import ObjectId
import json

class MongoService:
    def __init__(self, database):
        self.connection_details = {
            "host": database.host,
            "port": database.port,
            "database_name": database.database_name,
            "username": database.username,
            "password": decrypt_password(database.password)
        }
        self.connector = MongoConnector(self.connection_details)
        self.connector.connect()
        self.db = self.connector.client[self.connection_details["database_name"]] if self.connection_details["database_name"] else self.connector.client.get_database()

    def get_dashboard_stats(self):
        try:
            # Check if user has admin privileges by trying to run serverStatus
            is_mongo_admin = False
            try:
                server_status = self.connector.client.admin.command("serverStatus")
                is_mongo_admin = True  # If successful, user has admin privileges
            except Exception:
                server_status = {}
            
            # Database Stats
            try:
                db_stats = self.db.command("dbStats")
            except Exception:
                db_stats = {}
            
            # Collections Stats
            try:
                collections = self.db.list_collection_names()
                collection_stats = []
                
                for col_name in collections:
                    try:
                        stats = self.db.command("collStats", col_name)
                        collection_stats.append({
                            "name": col_name,
                            "count": stats.get("count", 0),
                            "avg_size": stats.get("avgObjSize", 0),
                            "size": stats.get("size", 0),
                            "storage_size": stats.get("storageSize", 0)
                        })
                    except Exception:
                        # Fallback if collStats fails
                        collection_stats.append({
                            "name": col_name,
                            "count": 0,
                            "avg_size": 0,
                            "size": 0,
                            "storage_size": 0
                        })
            except Exception:
                collection_stats = []
            
            # Current Ops (requires admin privileges)
            try:
                current_ops = self.connector.client.admin.command("currentOp", {"$all": True})
                ops = current_ops.get("inprog", [])[:10] # Limit to 10
            except Exception:
                ops = []
            
            # List all databases with their collections (admin only)
            databases_list = []
            if is_mongo_admin:  # Only try if user is admin
                try:
                    db_list = self.connector.client.list_database_names()
                    for db_name in db_list:
                        if db_name not in ['admin', 'local', 'config']:  # Skip system databases
                            try:
                                db_obj = self.connector.client[db_name]
                                collections = db_obj.list_collection_names()
                                db_stats_for_db = db_obj.command("dbStats") # Renamed to avoid conflict with main db_stats
                                databases_list.append({
                                    "name": db_name,
                                    "collections": collections,
                                    "collection_count": len(collections),
                                    "data_size": db_stats_for_db.get("dataSize", 0),
                                    "storage_size": db_stats_for_db.get("storageSize", 0)
                                })
                            except Exception:
                                # If we can't get stats for a database, skip it
                                pass
                except Exception:
                    databases_list = []
            
            return {
                "is_mongo_admin": is_mongo_admin,  # Flag to indicate MongoDB admin privileges
                "connections": server_status.get("connections", {}).get("current", "N/A"),
                "inserts_per_sec": server_status.get("opcounters", {}).get("insert", "N/A"),
                "queries_per_sec": server_status.get("opcounters", {}).get("query", "N/A"),
                "data_size": db_stats.get("dataSize", 0),
                "collections": collection_stats,
                "current_ops": ops,
                "version": server_status.get("version", "Unknown"),
                "databases": databases_list
            }
        except Exception as e:
            print(f"Error fetching MongoDB stats: {e}")
            return {
                "is_mongo_admin": False,
                "connections": "N/A",
                "inserts_per_sec": "N/A",
                "queries_per_sec": "N/A",
                "data_size": 0,
                "collections": [],
                "current_ops": [],
                "version": "Unknown",
                "databases": []
            }

    def browse_collection(self, collection_name: str, page: int, limit: int, filter_query: dict = None):
        if filter_query is None:
            filter_query = {}
            
        collection = self.db[collection_name]
        
        total_docs = collection.count_documents(filter_query)
        total_pages = math.ceil(total_docs / limit)
        skip = (page - 1) * limit
        
        cursor = collection.find(filter_query).skip(skip).limit(limit)
        documents = list(cursor)
        
        # Serialize documents (convert ObjectId and datetime)
        documents = [self._serialize_doc(doc) for doc in documents]
        
        # Calculate columns for table view
        columns = set()
        for doc in documents:
            if doc:
                columns.update(doc.keys())
        
        sorted_columns = sorted(list(columns))
        if '_id' in sorted_columns:
            sorted_columns.remove('_id')
            sorted_columns.insert(0, '_id')
        
        return {
            "documents": documents,
            "columns": sorted_columns,
            "total_docs": total_docs,
            "total_pages": total_pages,
            "current_page": page,
            "per_page": limit
        }

    def get_document(self, collection_name: str, doc_id: str):
        collection = self.db[collection_name]
        try:
            oid = ObjectId(doc_id)
            doc = collection.find_one({"_id": oid})
        except Exception:
            # Fallback for non-ObjectId _id
            doc = collection.find_one({"_id": doc_id})
            
        if doc:
            doc = self._serialize_doc(doc)
        return doc

    def _serialize_doc(self, doc):
        import base64
        from bson.binary import Binary
        
        if doc is None:
            return None
        if isinstance(doc, ObjectId):
            return str(doc)
        if isinstance(doc, Binary):
            return base64.b64encode(doc).decode('utf-8')
        if isinstance(doc, bytes):
            return base64.b64encode(doc).decode('utf-8')
        if hasattr(doc, 'isoformat'):
            return doc.isoformat()
        if isinstance(doc, list):
            return [self._serialize_doc(item) for item in doc]
        if isinstance(doc, dict):
            new_doc = {}
            for k, v in doc.items():
                new_doc[k] = self._serialize_doc(v)
            return new_doc
        return doc

    def insert_document(self, collection_name: str, document_data: dict):
        collection = self.db[collection_name]
        # If _id is provided as string but looks like ObjectId, should we convert it?
        # For now, let's trust the input or maybe convert if it's a specific format.
        # But usually insert doesn't require _id.
        return collection.insert_one(document_data)

    def update_document(self, collection_name: str, doc_id: str, document_data: dict):
        collection = self.db[collection_name]
        try:
            oid = ObjectId(doc_id)
            filter_query = {"_id": oid}
        except Exception:
            filter_query = {"_id": doc_id}
            
        # Remove _id from update data if present, as it's immutable
        if '_id' in document_data:
            del document_data['_id']
            
        return collection.replace_one(filter_query, document_data)

    def delete_document(self, collection_name: str, doc_id: str):
        collection = self.db[collection_name]
        try:
            oid = ObjectId(doc_id)
            filter_query = {"_id": oid}
        except Exception:
            filter_query = {"_id": doc_id}
            
        return collection.delete_one(filter_query)

    def create_collection(self, collection_name: str):
        return self.db.create_collection(collection_name)

    def drop_collection(self, collection_name: str):
        return self.db.drop_collection(collection_name)

    def run_command(self, command: dict):
        result = self.db.command(command)
        return self._serialize_doc(result)

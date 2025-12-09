from app.connectors.es_connector import ESConnector
from app.utils.security import decrypt_password
import requests
import json

class ElasticsearchService:
    def __init__(self, database):
        self.connection_details = {
            "host": database.host,
            "port": database.port,
            "username": database.username,
        }
        if database.password:
             self.connection_details["password"] = decrypt_password(database.password)

        self.connector = ESConnector(self.connection_details)
        self.connector.connect()

    def close(self):
        self.connector.close()

    def get_dashboard_stats(self):
        stats = self.connector.get_cluster_stats()
        indices = self.connector.get_indices()
        return stats, indices

    def execute_query(self, method: str, endpoint: str, query: str = None):
        url = f"{self.connector.base_url}{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(
                    url,
                    auth=self.connector.auth,
                    verify=False,
                    timeout=30
                )
            elif method == "POST":
                headers = {"Content-Type": "application/json"}
                response = requests.post(
                    url,
                    auth=self.connector.auth,
                    data=query if query else "{}",
                    headers=headers,
                    verify=False,
                    timeout=30
                )
            elif method == "PUT":
                headers = {"Content-Type": "application/json"}
                response = requests.put(
                    url,
                    auth=self.connector.auth,
                    data=query if query else "{}",
                    headers=headers,
                    verify=False,
                    timeout=30
                )
            elif method == "DELETE":
                response = requests.delete(
                    url,
                    auth=self.connector.auth,
                    verify=False,
                    timeout=30
                )
            else:
                return {"error": "Unsupported method"}
            
            try:
                return response.json()
            except:
                return response.text
                
        except Exception as e:
            return {"error": str(e)}

    def inspect_index(self, index_name: str):
        try:
            # Get index info
            indices_response = requests.get(
                f"{self.connector.base_url}/_cat/indices/{index_name}?format=json",
                auth=self.connector.auth,
                verify=False,
                timeout=10
            )
            index_info = indices_response.json()[0] if indices_response.status_code == 200 else None
            
            # Get mappings
            mappings_response = requests.get(
                f"{self.connector.base_url}/{index_name}/_mapping",
                auth=self.connector.auth,
                verify=False,
                timeout=10
            )
            mappings = mappings_response.json() if mappings_response.status_code == 200 else None
            
            # Get settings
            settings_response = requests.get(
                f"{self.connector.base_url}/{index_name}/_settings",
                auth=self.connector.auth,
                verify=False,
                timeout=10
            )
            settings = settings_response.json() if settings_response.status_code == 200 else None
            
            return {
                "index_info": index_info,
                "mappings": mappings,
                "settings": settings
            }
        except Exception as e:
            return {"error": str(e)}

    def search_index(self, index_name: str, query: str, size: int = 10):
        try:
            if query and query.strip():
                search_body = {
                    "query": {
                        "query_string": {
                            "query": query
                        }
                    },
                    "size": size
                }
            else:
                search_body = {
                    "query": {"match_all": {}},
                    "size": size
                }
            
            response = requests.post(
                f"{self.connector.base_url}/{index_name}/_search",
                auth=self.connector.auth,
                json=search_body,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get('hits', {}).get('hits', [])
            else:
                return {"error": response.text}
                
        except Exception as e:
            return {"error": str(e)}

    def create_index(self, index_name: str, shards: int = 1, replicas: int = 1, mappings: dict = None, settings: dict = None):
        body = {
            "settings": {
                "number_of_shards": shards,
                "number_of_replicas": replicas
            }
        }
        
        if mappings:
            body["mappings"] = mappings
        
        if settings:
             body["settings"].update(settings)
             
        try:
            response = requests.put(
                f"{self.connector.base_url}/{index_name}",
                auth=self.connector.auth,
                json=body,
                verify=False,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                return True, "OK"
            else:
                return False, response.text
        except Exception as e:
             return False, str(e)

    def delete_index(self, index_name: str):
        try:
            response = requests.delete(
                f"{self.connector.base_url}/{index_name}",
                auth=self.connector.auth,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                return True, "OK"
            else:
                return False, response.text
        except Exception as e:
            return False, str(e)

    def create_document(self, index_name: str, body: dict, doc_id: str = None):
        try:
            if doc_id and doc_id.strip():
                response = requests.put(
                    f"{self.connector.base_url}/{index_name}/_doc/{doc_id}",
                    auth=self.connector.auth,
                    json=body,
                    verify=False,
                    timeout=30
                )
            else:
                response = requests.post(
                    f"{self.connector.base_url}/{index_name}/_doc",
                    auth=self.connector.auth,
                    json=body,
                    verify=False,
                    timeout=30
                )
                
            if response.status_code in [200, 201]:
                return True, "OK"
            else:
                return False, response.text
        except Exception as e:
            return False, str(e)

    def update_document(self, index_name: str, doc_id: str, body: dict):
        try:
            response = requests.put(
                f"{self.connector.base_url}/{index_name}/_doc/{doc_id}",
                auth=self.connector.auth,
                json=body,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                return True, "OK"
            else:
                return False, response.text
        except Exception as e:
            return False, str(e)

    def delete_document(self, index_name: str, doc_id: str):
        try:
            response = requests.delete(
                f"{self.connector.base_url}/{index_name}/_doc/{doc_id}",
                auth=self.connector.auth,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                return True, "OK"
            else:
                return False, response.text
        except Exception as e:
            return False, str(e)

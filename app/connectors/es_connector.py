from elasticsearch import Elasticsearch
from .base import BaseConnector
import requests
from requests.auth import HTTPBasicAuth

class ESConnector(BaseConnector):
    def __init__(self, connection_details: dict):
        super().__init__(connection_details)
        self.client = None
        self.base_url = None
        self.auth = None

    def connect(self):
        host = str(self.connection_details.get("host", "")).strip().rstrip("/")
        port = self.connection_details.get("port")
        username = self.connection_details.get("username")
        password = self.connection_details.get("password")
        
        # Handle scheme in host or default
        if "://" in host:
            # Check if port is already in the host string (after protocol)
            # e.g. http://192.168.1.2:31920
            host_part = host.split("://")[1]
            if ":" in host_part:
                url = host
            else:
                # Scheme present but no port, append if valid port provided
                if port and int(port) > 0:
                    url = f"{host}:{port}"
                else:
                    url = host
        else:
            # No scheme, construct full URL
            scheme = "https" if port == 443 else "http"
            if port and int(port) > 0:
                url = f"{scheme}://{host}:{port}"
            else:
                url = f"{scheme}://{host}:9200"
        
        self.base_url = url
        print(f"[ES] Connecting to: {url}")
        
        # Setup auth if provided
        if username and str(username).strip():
            self.auth = HTTPBasicAuth(username, password if password else "")
        else:
            self.auth = None
        
        # Keep ES client for compatibility but don't use for critical operations
        try:
            if self.auth:
                self.client = Elasticsearch(
                    url, 
                    basic_auth=(username, password if password else ""),
                    verify_certs=False,
                    ssl_show_warn=False,
                    request_timeout=30
                )
            else:
                self.client = Elasticsearch(
                    url,
                    verify_certs=False,
                    ssl_show_warn=False,
                    request_timeout=30
                )
        except:
            # If ES client fails, we'll use requests directly
            pass
        
        self.connection = self.client

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.connect()
            # Use requests directly to avoid product check
            response = requests.get(
                self.base_url,
                auth=self.auth,
                verify=False,
                timeout=10
            )
            if response.status_code == 200:
                return True, "Connection successful"
            return False, f"Server returned status {response.status_code}"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def execute_query(self, query: str):
        # ES queries are JSON
        return {"error": "Raw query execution not fully supported for ES in this demo"}

    def get_cluster_stats(self):
        try:
            self.connect()
            # Use requests directly
            health_response = requests.get(
                f"{self.base_url}/_cluster/health",
                auth=self.auth,
                verify=False,
                timeout=10
            )
            stats_response = requests.get(
                f"{self.base_url}/_cluster/stats",
                auth=self.auth,
                verify=False,
                timeout=10
            )
            
            if health_response.status_code == 200 and stats_response.status_code == 200:
                health = health_response.json()
                stats = stats_response.json()
                return {
                    "health": health.get("status"),
                    "nodes": health.get("number_of_nodes"),
                    "indices": stats.get("indices", {}).get("count"),
                    "docs": stats.get("indices", {}).get("docs", {}).get("count"),
                    "size": stats.get("indices", {}).get("store", {}).get("size_in_bytes")
                }
            return None
        except Exception as e:
            print(f"Error fetching stats: {e}")
            return None
        finally:
            self.close()

    def get_indices(self):
        try:
            self.connect()
            # Use requests directly
            response = requests.get(
                f"{self.base_url}/_cat/indices?format=json",
                auth=self.auth,
                verify=False,
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            print(f"Error fetching indices: {e}")
            return []
        finally:
            self.close()


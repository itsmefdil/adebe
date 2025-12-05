import mysql.connector
from mysql.connector import pooling
import hashlib
from .base import BaseConnector

# Global storage for connection pools
_connection_pools = {}

class MySQLConnector(BaseConnector):
    def _get_pool_key(self):
        """Generate a unique key for the connection pool based on credentials."""
        # Create a string identifier from connection details
        # We include all details that define the connection
        details_str = f"{self.connection_details.get('host')}:{self.connection_details.get('port')}:{self.connection_details.get('username')}:{self.connection_details.get('database_name')}"
        # Use MD5 for a shorter key (security not critical here as it's just an internal dict key)
        return hashlib.md5(details_str.encode()).hexdigest()

    def connect(self):
        pool_key = self._get_pool_key()
        
        if pool_key not in _connection_pools:
            try:
                _connection_pools[pool_key] = pooling.MySQLConnectionPool(
                    pool_name=pool_key,
                    pool_size=10,  # Increased pool size to handle concurrent requests
                    pool_reset_session=True,
                    host=self.connection_details.get("host"),
                    port=self.connection_details.get("port", 3306),
                    user=self.connection_details.get("username"),
                    password=self.connection_details.get("password"),
                    database=self.connection_details.get("database_name")
                )
            except mysql.connector.Error as err:
                # If pool creation fails, we might want to raise it or handle it
                # For now, let it propagate so the caller sees the error
                raise err

        # Get a connection from the pool
        self.connection = _connection_pools[pool_key].get_connection()

    def close(self):
        if self.connection:
            self.connection.close()  # Returns connection to the pool
            self.connection = None

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.connect()
            if self.connection.is_connected():
                return True, "Connection successful"
            return False, "Connection established but is_connected() returned False"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def execute_query(self, query: str, params: tuple = None):
        cursor = None
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params)
            if query.strip().upper().startswith("SELECT") or query.strip().upper().startswith("SHOW"):
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return {"affected_rows": cursor.rowcount}
        except Exception as e:
            # Re-raise or return error dict? The original code returned dict for error in execute_query
            # but raised in test_connection. Let's stick to original behavior for execute_query
            return {"error": str(e)}
        finally:
            if cursor:
                cursor.close()
            self.close()

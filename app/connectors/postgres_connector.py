import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from .base import BaseConnector
import threading

class PostgresConnectionPool:
    """Singleton connection pool manager for PostgreSQL connections."""
    _pools = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_pool(cls, connection_key: str, connection_details: dict, min_conn=1, max_conn=10):
        """Get or create a connection pool for the given connection details."""
        with cls._lock:
            if connection_key not in cls._pools:
                try:
                    cls._pools[connection_key] = pool.ThreadedConnectionPool(
                        min_conn,
                        max_conn,
                        host=connection_details.get("host"),
                        port=connection_details.get("port", 5432),
                        user=connection_details.get("username"),
                        password=connection_details.get("password"),
                        dbname=connection_details.get("database_name"),
                        connect_timeout=10,  # 10 second timeout
                        keepalives=1,
                        keepalives_idle=30,
                        keepalives_interval=10,
                        keepalives_count=5
                    )
                except Exception as e:
                    print(f"Failed to create connection pool: {e}")
                    return None
            return cls._pools[connection_key]
    
    @classmethod
    def close_pool(cls, connection_key: str):
        """Close a specific connection pool."""
        with cls._lock:
            if connection_key in cls._pools:
                cls._pools[connection_key].closeall()
                del cls._pools[connection_key]


class PostgresConnector(BaseConnector):
    _use_pool = True  # Enable connection pooling by default
    
    def __init__(self, connection_details: dict):
        super().__init__(connection_details)
        self._pool_connection = None
        # Create a unique key for this connection
        self._connection_key = f"{connection_details.get('host')}:{connection_details.get('port')}:{connection_details.get('database_name')}:{connection_details.get('username')}"
    
    def _get_pool(self):
        """Get the connection pool for this connector."""
        return PostgresConnectionPool.get_pool(self._connection_key, self.connection_details)
    
    def connect(self):
        """Get a connection - either from pool or create new."""
        if self._use_pool:
            pool = self._get_pool()
            if pool:
                try:
                    self._pool_connection = pool.getconn()
                    self.connection = self._pool_connection
                    return
                except Exception as e:
                    print(f"Pool connection failed, falling back to direct: {e}")
        
        # Fallback to direct connection
        self.connection = psycopg2.connect(
            host=self.connection_details.get("host"),
            port=self.connection_details.get("port", 5432),
            user=self.connection_details.get("username"),
            password=self.connection_details.get("password"),
            dbname=self.connection_details.get("database_name"),
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        self._pool_connection = None

    def close(self):
        """Return connection to pool or close it."""
        if self.connection:
            if self._pool_connection and self._use_pool:
                # Return to pool instead of closing
                pool = self._get_pool()
                if pool:
                    try:
                        pool.putconn(self._pool_connection)
                        self._pool_connection = None
                        self.connection = None
                        return
                    except Exception as e:
                        print(f"Failed to return connection to pool: {e}")
            
            # Close directly if not using pool or pool return failed
            try:
                self.connection.close()
            except:
                pass
            self.connection = None
            self._pool_connection = None

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.connect()
            # Execute a simple query to verify connection works
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def execute_query(self, query: str, params=None):
        cursor = None
        try:
            self.connect()
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params)
            if cursor.description:
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return {"status": "success"}
        except Exception as e:
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
            return {"error": str(e)}
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            self.close()

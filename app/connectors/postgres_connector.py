import psycopg2
from psycopg2.extras import RealDictCursor
from .base import BaseConnector

class PostgresConnector(BaseConnector):
    def connect(self):
        self.connection = psycopg2.connect(
            host=self.connection_details.get("host"),
            port=self.connection_details.get("port", 5432),
            user=self.connection_details.get("username"),
            password=self.connection_details.get("password"),
            dbname=self.connection_details.get("database_name")
        )

    def close(self):
        if self.connection:
            self.connection.close()

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.connect()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def execute_query(self, query: str, params=None):
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
            self.connection.rollback()
            return {"error": str(e)}
        finally:
            if self.connection:
                cursor.close()
                self.close()


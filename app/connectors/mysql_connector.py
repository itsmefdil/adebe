import mysql.connector
from .base import BaseConnector

class MySQLConnector(BaseConnector):
    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.connection_details.get("host"),
            port=self.connection_details.get("port", 3306),
            user=self.connection_details.get("username"),
            password=self.connection_details.get("password"),
            database=self.connection_details.get("database_name")
        )

    def close(self):
        if self.connection:
            self.connection.close()

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

    def execute_query(self, query: str):
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            if query.strip().upper().startswith("SELECT") or query.strip().upper().startswith("SHOW"):
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return {"affected_rows": cursor.rowcount}
        except Exception as e:
            return {"error": str(e)}
        finally:
            if self.connection:
                cursor.close()
                self.close()

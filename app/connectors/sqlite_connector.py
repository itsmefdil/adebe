import sqlite3
from .base import BaseConnector

class SQLiteConnector(BaseConnector):
    def connect(self):
        self.connection = sqlite3.connect(self.connection_details.get("host"))
        self.connection.row_factory = sqlite3.Row

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

    def execute_query(self, query: str):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query)
            if query.strip().upper().startswith("SELECT") or query.strip().upper().startswith("PRAGMA"):
                result = [dict(row) for row in cursor.fetchall()]
                return result
            else:
                self.connection.commit()
                return {"affected_rows": cursor.rowcount}
        except Exception as e:
            return {"error": str(e)}
        finally:
            self.close()

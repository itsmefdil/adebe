from app.connectors.sqlite_connector import SQLiteConnector
from app.utils.security import decrypt_password

class SQLiteService:
    def __init__(self, database):
        self.connection_details = {
            "host": database.host,
            "database_name": database.database_name,
            "username": database.username,
        }
        if database.password:
            self.connection_details["password"] = decrypt_password(database.password)
            
        self.connector = SQLiteConnector(self.connection_details)

    def get_dashboard_stats(self):
        """Get all stats needed for the dashboard."""
        try:
            file_info = self.connector.get_file_info()
            tables = self.connector.get_tables()
            pragma = self.connector.get_pragma_settings()
            
            return {
                "file_info": file_info,
                "tables": tables,
                "pragma": pragma
            }
        except Exception as e:
            return {"error": str(e)}

    def browse_table(self, table_name: str, page: int = 1, limit: int = 50):
        offset = (page - 1) * limit
        return self.connector.get_table_data(table_name, limit, offset)

    def delete_row(self, table_name: str, row_id: int):
        return self.connector.delete_row(table_name, row_id)

    def get_table_info(self, table_name: str):
        return self.connector.get_table_info(table_name)
    
    def insert_row(self, table_name: str, data: dict):
        return self.connector.insert_row(table_name, data)

    def get_row(self, table_name: str, row_id: int):
        return self.connector.get_row(table_name, row_id)

    def update_row(self, table_name: str, row_id: int, data: dict):
        return self.connector.update_row(table_name, row_id, data)

    def create_table(self, table_name: str, columns: list):
        return self.connector.create_table(table_name, columns)

    def drop_table(self, table_name: str):
        return self.connector.drop_table(table_name)

    def execute_query(self, query: str):
        return self.connector.execute_query(query)

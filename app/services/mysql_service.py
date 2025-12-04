from app.connectors.mysql_connector import MySQLConnector
from app.utils.security import decrypt_password

class MySQLService:
    def __init__(self, database):
        self.connection_details = {
            "host": database.host,
            "port": database.port,
            "database_name": database.database_name,
            "username": database.username,
            "password": decrypt_password(database.password)
        }
        self.connector = MySQLConnector(self.connection_details)

    def get_dashboard_stats(self):
        try:
            status_vars = self.connector.execute_query("SHOW STATUS")
            variables = self.connector.execute_query("SHOW VARIABLES")
            tables = self.connector.execute_query("SHOW TABLE STATUS")
            processlist = self.connector.execute_query("SHOW PROCESSLIST")
            
            status_dict = {item['Variable_name']: item['Value'] for item in status_vars} if status_vars else {}
            var_dict = {item['Variable_name']: item['Value'] for item in variables} if variables else {}
            
            return {
                "status": status_dict,
                "variables": var_dict,
                "tables": tables if tables else [],
                "processlist": processlist if processlist else []
            }
        except Exception as e:
            # Log error?
            print(f"Error fetching MySQL stats: {e}")
            return {
                "status": {},
                "variables": {},
                "tables": [],
                "processlist": []
            }

    def get_processlist(self):
        return self.connector.execute_query("SHOW PROCESSLIST")

    def browse_table(self, table_name: str, page: int, limit: int, search: str = None, sort_by: str = None, sort_order: str = "ASC"):
        # Get column names first
        columns_info = self.connector.execute_query(f"SHOW COLUMNS FROM `{table_name}`")
        columns = [col['Field'] for col in columns_info] if columns_info else []
        
        # Identify primary key
        primary_key = next((col['Field'] for col in columns_info if col['Key'] == 'PRI'), columns[0] if columns else None)
        
        # Build search query
        where_clause = ""
        params = []
        if search and columns:
            conditions = [f"`{col}` LIKE %s" for col in columns]
            where_clause = " WHERE " + " OR ".join(conditions)
            params = [f"%{search}%"] * len(columns)
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM `{table_name}`{where_clause}"
        count_result = self.connector.execute_query(count_query, tuple(params))
        total_rows = count_result[0]['total'] if count_result else 0
        
        # Calculate pagination
        total_pages = (total_rows + limit - 1) // limit
        start_index = (page - 1) * limit + 1 if total_rows > 0 else 0
        end_index = min(page * limit, total_rows)
        
        # Build sort clause
        order_clause = ""
        if sort_by and sort_by in columns:
            order_clause = f" ORDER BY `{sort_by}` {sort_order}"
        
        # Get table data
        offset = (page - 1) * limit
        data_query = f"SELECT * FROM `{table_name}`{where_clause}{order_clause} LIMIT %s OFFSET %s"
        data_params = params + [limit, offset]
        rows = self.connector.execute_query(data_query, tuple(data_params))
        
        return {
            "columns": columns,
            "primary_key": primary_key,
            "rows": rows if rows else [],
            "total_rows": total_rows,
            "total_pages": total_pages,
            "start_index": start_index,
            "end_index": end_index
        }

    def get_table_structure(self, table_name: str):
        # Get column information
        columns = self.connector.execute_query(f"SHOW FULL COLUMNS FROM `{table_name}`")
        
        # Get index information
        indexes = self.connector.execute_query(f"SHOW INDEX FROM `{table_name}`")
        
        # Group indexes by Key_name
        index_dict = {}
        if indexes:
            for idx in indexes:
                key_name = idx['Key_name']
                if key_name not in index_dict:
                    index_dict[key_name] = {
                        'name': key_name,
                        'type': idx['Index_type'],
                        'unique': 'Yes' if idx['Non_unique'] == 0 else 'No',
                        'columns': []
                    }
                index_dict[key_name]['columns'].append(idx['Column_name'])
        
        # Convert to list
        indexes_list = []
        for key, value in index_dict.items():
            value['columns_str'] = ', '.join(value['columns'])
            indexes_list.append(value)
            
        return {
            "columns": columns if columns else [],
            "indexes": indexes_list
        }

    def delete_row(self, table_name: str, primary_key_column: str, primary_key_value: str):
        query = f"DELETE FROM `{table_name}` WHERE `{primary_key_column}` = %s"
        return self.connector.execute_query(query, (primary_key_value,))

    def get_row(self, table_name: str, primary_key_column: str, primary_key_value: str):
        query = f"SELECT * FROM `{table_name}` WHERE `{primary_key_column}` = %s"
        result = self.connector.execute_query(query, (primary_key_value,))
        return result[0] if result else None

    def insert_row(self, table_name: str, data: dict):
        columns = [f"`{col}`" for col in data.keys()]
        placeholders = ["%s"] * len(data)
        query = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        return self.connector.execute_query(query, tuple(data.values()))

    def update_row(self, table_name: str, primary_key_column: str, primary_key_value: str, data: dict):
        set_clause = ", ".join([f"`{col}` = %s" for col in data.keys()])
        query = f"UPDATE `{table_name}` SET {set_clause} WHERE `{primary_key_column}` = %s"
        params = list(data.values()) + [primary_key_value]
        return self.connector.execute_query(query, tuple(params))

    def create_table(self, table_name: str, columns: list):
        # columns is a list of dicts: {'name': 'id', 'type': 'INT', 'length': 11, 'nullable': False, 'primary_key': True, 'auto_increment': True}
        col_defs = []
        primary_keys = []
        
        for col in columns:
            definition = f"`{col['name']}` {col['type']}"
            if col.get('length'):
                definition += f"({col['length']})"
            
            if not col.get('nullable', True):
                definition += " NOT NULL"
            
            if col.get('auto_increment'):
                definition += " AUTO_INCREMENT"
                
            if col.get('default'):
                definition += f" DEFAULT '{col['default']}'"
                
            col_defs.append(definition)
            
            if col.get('primary_key'):
                primary_keys.append(f"`{col['name']}`")
        
        if primary_keys:
            col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            
        query = f"CREATE TABLE `{table_name}` ({', '.join(col_defs)})"
        return self.connector.execute_query(query)

    def drop_table(self, table_name: str):
        return self.connector.execute_query(f"DROP TABLE `{table_name}`")

    def execute_raw_query(self, query: str):
        # This is a raw query execution. Security implications should be considered.
        # For a "Query Builder" feature, we assume the user is authorized to run arbitrary SQL.
        return self.connector.execute_query(query)

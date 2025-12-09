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

    def add_column(self, table_name: str, column_def: dict):
        # column_def: {'name': 'new_col', 'type': 'VARCHAR', 'length': 100, 'nullable': True, 'default': None, 'after': 'id'}
        definition = self._build_column_definition(column_def)
        
        query = f"ALTER TABLE `{table_name}` ADD COLUMN {definition}"
        if column_def.get('after'):
            query += f" AFTER `{column_def['after']}`"
        elif column_def.get('first'):
            query += " FIRST"
            
        return self.connector.execute_query(query)

    def modify_column(self, table_name: str, column_name: str, column_def: dict):
        definition = self._build_column_definition(column_def)
        # CHANGE COLUMN old_name new_name definition
        query = f"ALTER TABLE `{table_name}` CHANGE COLUMN `{column_name}` {definition}"
        return self.connector.execute_query(query)
    
    def drop_column(self, table_name: str, column_name: str):
        query = f"ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`"
        return self.connector.execute_query(query)

    def drop_index(self, table_name: str, index_name: str):
        query = f"DROP INDEX `{index_name}` ON `{table_name}`"
        return self.connector.execute_query(query)

    def _build_column_definition(self, col: dict):
        definition = f"`{col['name']}` {col['type']}"
        if col.get('length'):
            definition += f"({col['length']})"
        
        if not col.get('nullable', True):
            definition += " NOT NULL"
        
        if col.get('auto_increment'):
            definition += " AUTO_INCREMENT"
            
        if col.get('default'):
            definition += f" DEFAULT '{col['default']}'"
            
        return definition

    def export_table_to_file(self, table_name: str, file_path: str, format: str = 'csv'):
        """Expots table data to a file (CSV or JSON)."""
        import csv
        import json
        
        # Get columns
        columns_info = self.connector.execute_query(f"SHOW COLUMNS FROM `{table_name}`")
        columns = [col['Field'] for col in columns_info] if columns_info else []
        
        # Get data
        query = f"SELECT * FROM `{table_name}`"
        # Use a server-side cursor or fetchmany if possible for large datasets, 
        # but for now we'll fetchall from the connector wrapper (which fetches all).
        # Improvement: Update connector to support yielding rows.
        rows = self.connector.execute_query(query)
        
        if format == 'csv':
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                if rows:
                    writer.writerows(rows)
        elif format == 'json':
            # Handle non-serializable types
            from datetime import date, datetime
            from decimal import Decimal
            
            def default_serializer(obj):
                if isinstance(obj, (date, datetime)):
                    return obj.isoformat()
                if isinstance(obj, Decimal):
                    return float(obj)
                raise TypeError(f"Type {type(obj)} not serializable")

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(rows if rows else [], f, default=default_serializer, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    def import_table_from_file(self, table_name: str, file_path: str, format: str = 'csv'):
        """Imports data from a file into a table."""
        import csv
        import json
        from datetime import datetime

        data_to_insert = []
        
        if format == 'csv':
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data_to_insert = list(reader)
        elif format == 'json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data_to_insert = json.load(f)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        if not data_to_insert:
            return 0
            
        # Insert in batches
        batch_size = 1000
        total_inserted = 0
        
        # Get current table columns to validate/filter data
        columns_info = self.connector.execute_query(f"SHOW COLUMNS FROM `{table_name}`")
        valid_columns = {col['Field'] for col in columns_info}
        
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            
            # Clean batch keys
            cleaned_batch = []
            for row in batch:
                cleaned_row = {k: v for k, v in row.items() if k in valid_columns}
                if cleaned_row:
                    cleaned_batch.append(cleaned_row)
            
            if not cleaned_batch:
                continue
                
            # Prepare insert query
            # Assuming all rows in batch have same keys. If not, this might fail.
            # Ideally we check keys of the first item
            keys = list(cleaned_batch[0].keys())
            columns_str = ", ".join([f"`{k}`" for k in keys])
            placeholders = ", ".join(["%s"] * len(keys))
            
            query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            
            # Execute batch
            # Our connector executes one by one in generic execute_query if we pass tuple?
            # No, our connector doesn't support executemany yet properly exposed.
            # Let's verify connector.execute_query implementation.
            # It takes (query, params).
            # We will insert one by one for safety or use bulk insert syntax.
            
            # Bulk insert syntax construction
            values_str_list = []
            all_params = []
            
            for row in cleaned_batch:
                values_str_list.append(f"({placeholders})")
                for k in keys:
                    val = row.get(k)
                    # Simple handling for empty strings that should be NULL?
                    if val == '':
                        val = None
                    all_params.append(val)
            
            full_query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES " + ", ".join(values_str_list)
            
            self.connector.execute_query(full_query, tuple(all_params))
            total_inserted += len(cleaned_batch)
            
        return total_inserted

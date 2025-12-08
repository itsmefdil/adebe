from app.connectors.postgres_connector import PostgresConnector
from app.utils.security import decrypt_password

class PostgreSQLService:
    def __init__(self, database):
        self.connection_details = {
            "host": database.host,
            "port": database.port,
            "database_name": database.database_name,
            "username": database.username,
            "password": decrypt_password(database.password)
        }
        self.connector = PostgresConnector(self.connection_details)

    def get_dashboard_stats(self):
        """Get dashboard statistics including version, connections, tables, and activities."""
        try:
            # Get version
            version_result = self.connector.execute_query("SELECT version()")
            version = version_result[0]['version'] if version_result else 'Unknown'
            
            # Get connection stats
            conn_stats = self.connector.execute_query("""
                SELECT 
                    (SELECT count(*) FROM pg_stat_activity) as active_connections,
                    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
            """)
            
            # Get database size
            db_size = self.connector.execute_query("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """)
            
            # Get tables info
            tables = self.connector.execute_query("""
                SELECT 
                    schemaname as schema,
                    tablename as name,
                    'table' as type,
                    tableowner as owner,
                    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size,
                    pg_total_relation_size(schemaname || '.' || tablename) as size_bytes,
                    (SELECT reltuples::bigint FROM pg_class WHERE oid = (schemaname || '.' || tablename)::regclass) as estimated_rows
                FROM pg_tables 
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename
            """)
            
            # Get active queries
            activities = self.connector.execute_query("""
                SELECT 
                    pid,
                    usename as user,
                    state,
                    query,
                    EXTRACT(EPOCH FROM (now() - query_start))::numeric(10,2) as duration_sec,
                    query_start
                FROM pg_stat_activity 
                WHERE state IS NOT NULL 
                AND query NOT LIKE '%pg_stat_activity%'
                ORDER BY query_start DESC
                LIMIT 10
            """)
            
            # Get server settings
            settings = self.connector.execute_query("""
                SELECT name, setting, unit, short_desc 
                FROM pg_settings 
                WHERE name IN ('max_connections', 'shared_buffers', 'work_mem', 'maintenance_work_mem', 
                              'effective_cache_size', 'server_version', 'data_directory')
            """)
            settings_dict = {s['name']: s['setting'] for s in settings} if settings else {}
            
            return {
                "version": version,
                "active_connections": conn_stats[0]['active_connections'] if conn_stats else 0,
                "max_connections": conn_stats[0]['max_connections'] if conn_stats else 0,
                "database_size": db_size[0]['size'] if db_size else '0 MB',
                "tables": tables if tables else [],
                "activities": activities if activities else [],
                "settings": settings_dict
            }
        except Exception as e:
            print(f"Error fetching PostgreSQL stats: {e}")
            return {
                "version": "Unknown",
                "active_connections": 0,
                "max_connections": 0,
                "database_size": "0 MB",
                "tables": [],
                "activities": [],
                "settings": {}
            }

    def get_activity(self):
        """Get current database activity."""
        return self.connector.execute_query("""
            SELECT 
                pid,
                usename as user,
                state,
                query,
                EXTRACT(EPOCH FROM (now() - query_start))::numeric(10,2) as duration_sec,
                query_start
            FROM pg_stat_activity 
            WHERE state IS NOT NULL 
            AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start DESC
            LIMIT 10
        """)

    def browse_table(self, schema: str, table_name: str, page: int, limit: int, 
                     search: str = None, sort_by: str = None, sort_order: str = "ASC"):
        """Browse table data with pagination, search, and sorting."""
        full_table_name = f'"{schema}"."{table_name}"'
        
        # Get column names
        columns_info = self.connector.execute_query("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table_name))
        
        # Handle error case
        if isinstance(columns_info, dict) and "error" in columns_info:
            return {"error": columns_info["error"], "columns": [], "rows": [], "total_rows": 0}
        
        columns = [col['column_name'] for col in columns_info] if columns_info else []
        
        # Get primary key column
        pk_result = self.connector.execute_query("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (f'{schema}.{table_name}',))
        
        primary_key = None
        if isinstance(pk_result, list) and pk_result:
            primary_key = pk_result[0]['attname']
        elif columns:
            primary_key = columns[0]
        
        # Build search query
        where_clause = ""
        params = []
        if search and columns:
            conditions = [f'"{col}"::text ILIKE %s' for col in columns]
            where_clause = " WHERE " + " OR ".join(conditions)
            params = [f"%{search}%"] * len(columns)
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM {full_table_name}{where_clause}"
        count_result = self.connector.execute_query(count_query, tuple(params) if params else None)
        
        total_rows = 0
        if isinstance(count_result, list) and count_result:
            total_rows = count_result[0]['total']
        
        # Calculate pagination
        total_pages = (total_rows + limit - 1) // limit if total_rows > 0 else 1
        start_index = (page - 1) * limit + 1 if total_rows > 0 else 0
        end_index = min(page * limit, total_rows)
        
        # Build sort clause
        order_clause = ""
        if sort_by and sort_by in columns:
            order_clause = f' ORDER BY "{sort_by}" {sort_order}'
        
        # Get table data
        offset = (page - 1) * limit
        data_query = f"SELECT * FROM {full_table_name}{where_clause}{order_clause} LIMIT %s OFFSET %s"
        data_params = params + [limit, offset]
        rows = self.connector.execute_query(data_query, tuple(data_params))
        
        # Handle rows error case
        if isinstance(rows, dict) and "error" in rows:
            return {"error": rows["error"], "columns": columns, "rows": [], "total_rows": 0}
        
        return {
            "columns": columns,
            "columns_info": columns_info if columns_info else [],
            "primary_key": primary_key,
            "rows": rows if isinstance(rows, list) else [],
            "total_rows": total_rows,
            "total_pages": total_pages,
            "start_index": start_index,
            "end_index": end_index
        }

    def get_table_structure(self, schema: str, table_name: str):
        """Get table structure including columns and indexes."""
        # Get column information
        columns = self.connector.execute_query(f"""
            SELECT 
                column_name as name,
                data_type as type,
                character_maximum_length as max_length,
                is_nullable as nullable,
                column_default as default_value,
                udt_name as udt_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table_name))
        
        # Get index information
        indexes = self.connector.execute_query(f"""
            SELECT
                i.relname as name,
                am.amname as type,
                CASE WHEN ix.indisunique THEN 'Yes' ELSE 'No' END as unique,
                CASE WHEN ix.indisprimary THEN 'Yes' ELSE 'No' END as primary,
                array_to_string(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), ', ') as columns
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON i.relam = am.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = %s AND n.nspname = %s
            GROUP BY i.relname, am.amname, ix.indisunique, ix.indisprimary
        """, (table_name, schema))
        
        # Get constraints
        constraints = self.connector.execute_query(f"""
            SELECT 
                conname as name,
                contype as type,
                pg_get_constraintdef(c.oid) as definition
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE t.relname = %s AND n.nspname = %s
        """, (table_name, schema))
        
        return {
            "columns": columns if columns else [],
            "indexes": indexes if indexes else [],
            "constraints": constraints if constraints else []
        }

    def delete_row(self, schema: str, table_name: str, primary_key_column: str, primary_key_value: str):
        """Delete a row from the table."""
        query = f'DELETE FROM "{schema}"."{table_name}" WHERE "{primary_key_column}" = %s'
        return self.connector.execute_query(query, (primary_key_value,))

    def get_row(self, schema: str, table_name: str, primary_key_column: str, primary_key_value: str):
        """Get a single row by primary key."""
        query = f'SELECT * FROM "{schema}"."{table_name}" WHERE "{primary_key_column}" = %s'
        result = self.connector.execute_query(query, (primary_key_value,))
        return result[0] if result else None

    def insert_row(self, schema: str, table_name: str, data: dict):
        """Insert a new row into the table."""
        columns = [f'"{col}"' for col in data.keys()]
        placeholders = ["%s"] * len(data)
        query = f'INSERT INTO "{schema}"."{table_name}" ({", ".join(columns)}) VALUES ({", ".join(placeholders)})'
        return self.connector.execute_query(query, tuple(data.values()))

    def update_row(self, schema: str, table_name: str, primary_key_column: str, primary_key_value: str, data: dict):
        """Update a row in the table."""
        set_clause = ", ".join([f'"{col}" = %s' for col in data.keys()])
        query = f'UPDATE "{schema}"."{table_name}" SET {set_clause} WHERE "{primary_key_column}" = %s'
        params = list(data.values()) + [primary_key_value]
        return self.connector.execute_query(query, tuple(params))

    def create_table(self, schema: str, table_name: str, columns: list):
        """Create a new table."""
        col_defs = []
        primary_keys = []
        
        for col in columns:
            definition = f'"{col["name"]}" {col["type"]}'
            if col.get("length"):
                definition += f'({col["length"]})'
            
            if not col.get("nullable", True):
                definition += " NOT NULL"
            
            if col.get("default"):
                definition += f" DEFAULT '{col['default']}'"
                
            col_defs.append(definition)
            
            if col.get("primary_key"):
                primary_keys.append(f'"{col["name"]}"')
        
        if primary_keys:
            col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            
        query = f'CREATE TABLE "{schema}"."{table_name}" ({", ".join(col_defs)})'
        return self.connector.execute_query(query)

    def drop_table(self, schema: str, table_name: str):
        """Drop a table."""
        return self.connector.execute_query(f'DROP TABLE "{schema}"."{table_name}"')

    def execute_raw_query(self, query: str):
        """Execute a raw SQL query."""
        return self.connector.execute_query(query)

    def add_column(self, schema: str, table_name: str, column_def: dict):
        """Add a new column to a table."""
        definition = self._build_column_definition(column_def)
        query = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN {definition}'
        return self.connector.execute_query(query)

    def modify_column(self, schema: str, table_name: str, column_name: str, column_def: dict):
        """Modify an existing column."""
        # PostgreSQL uses ALTER COLUMN, not MODIFY
        queries = []
        
        # Change type
        type_def = column_def['type']
        if column_def.get('length'):
            type_def += f"({column_def['length']})"
        queries.append(f'ALTER TABLE "{schema}"."{table_name}" ALTER COLUMN "{column_name}" TYPE {type_def}')
        
        # Change nullable
        if column_def.get('nullable', True):
            queries.append(f'ALTER TABLE "{schema}"."{table_name}" ALTER COLUMN "{column_name}" DROP NOT NULL')
        else:
            queries.append(f'ALTER TABLE "{schema}"."{table_name}" ALTER COLUMN "{column_name}" SET NOT NULL')
        
        # Change default
        if column_def.get('default'):
            queries.append(f'ALTER TABLE "{schema}"."{table_name}" ALTER COLUMN "{column_name}" SET DEFAULT \'{column_def["default"]}\'')
        else:
            queries.append(f'ALTER TABLE "{schema}"."{table_name}" ALTER COLUMN "{column_name}" DROP DEFAULT')
        
        # Execute all queries
        for q in queries:
            try:
                self.connector.execute_query(q)
            except Exception as e:
                return {"error": str(e)}
        
        return {"status": "success"}
    
    def drop_column(self, schema: str, table_name: str, column_name: str):
        """Drop a column from a table."""
        query = f'ALTER TABLE "{schema}"."{table_name}" DROP COLUMN "{column_name}"'
        return self.connector.execute_query(query)

    def drop_index(self, schema: str, index_name: str):
        """Drop an index."""
        query = f'DROP INDEX "{schema}"."{index_name}"'
        return self.connector.execute_query(query)

    def _build_column_definition(self, col: dict):
        """Build column definition string for ALTER TABLE."""
        definition = f'"{col["name"]}" {col["type"]}'
        if col.get("length"):
            definition += f"({col['length']})"
        
        if not col.get("nullable", True):
            definition += " NOT NULL"
            
        if col.get("default"):
            definition += f" DEFAULT '{col['default']}'"
            
        return definition

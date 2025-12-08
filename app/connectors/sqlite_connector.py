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

    def get_tables(self):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT type, name, sql FROM sqlite_master WHERE type IN ('table', 'index', 'view') ORDER BY type DESC, name")
            tables = [dict(row) for row in cursor.fetchall()]
            
            # Fetch row counts for tables
            for table in tables:
                if table['type'] == 'table':
                    try:
                        cursor.execute(f"SELECT count(*) as count FROM {table['name']}")
                        table['rows'] = cursor.fetchone()['count']
                    except:
                        table['rows'] = 0
                else:
                    table['rows'] = None
            return tables
        finally:
            self.close()

    def drop_table(self, table_name: str):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE {table_name}")
            self.connection.commit()
            return True, "Table dropped successfully"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def get_table_info(self, table_name: str):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return [dict(row) for row in cursor.fetchall()]
        finally:
            self.close()

    def get_table_data(self, table_name: str, limit: int = 100, offset: int = 0):
        try:
            self.connect()
            cursor = self.connection.cursor()
            # Fetch columns first
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row['name'] for row in cursor.fetchall()]
            
            # Fetch data with rowid if available (common in sqlite)
            try:
                cursor.execute(f"SELECT rowid AS rowid, * FROM {table_name} LIMIT ? OFFSET ?", (limit, offset))
                rows = [dict(row) for row in cursor.fetchall()]
                # If rowid exists, make sure it's in the dict
                if rows and 'rowid' not in rows[0]:
                    # Some tables might not have rowid (e.g. WITHOUT ROWID), handle gracefully
                    pass
            except:
                # Fallback purely to selectable columns if rowid fails
                cursor.execute(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (limit, offset))
                rows = [dict(row) for row in cursor.fetchall()]

            count_query = f"SELECT count(*) as total FROM {table_name}"
            cursor.execute(count_query)
            total_rows = cursor.fetchone()['total']

            return {
                "columns": columns,
                "rows": rows,
                "total_rows": total_rows
            }
        finally:
            self.close()

    def get_pragma_settings(self):
        settings = ["journal_mode", "synchronous", "encoding", "foreign_keys"]
        result = {}
        try:
            self.connect()
            cursor = self.connection.cursor()
            for setting in settings:
                cursor.execute(f"PRAGMA {setting}")
                row = cursor.fetchone()
                if row:
                    # Pragma results can vary, usually it's the first column
                    result[setting] = row[0] if len(row) > 0 else "Unknown"
            return result
        finally:
            self.close()

    def get_file_info(self):
        import os
        from datetime import datetime
        
        db_path = self.connection_details.get("host")
        if not os.path.exists(db_path):
            return None
            
        stats = os.stat(db_path)
        return {
            "path": db_path,
            "size": stats.st_size,
            "size_formatted": f"{stats.st_size / (1024 * 1024):.2f} MB",
            "permissions": oct(stats.st_mode)[-3:],
            "last_modified": datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        }

    def delete_row(self, table_name: str, row_id: int):
        try:
            self.connect()
            cursor = self.connection.cursor()
            # Try deleting by rowid first
            cursor.execute(f"DELETE FROM {table_name} WHERE rowid = ?", (row_id,))
            self.connection.commit()
            return cursor.rowcount > 0
        except Exception as e:
            # Fallback for tables without rowid or other issues? 
            # For now assume rowid usage for simple CRUD
            return False
        finally:
            self.close()

    def get_row(self, table_name: str, row_id: int):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT rowid AS rowid, * FROM {table_name} WHERE rowid = ?", (row_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            self.close()

    def insert_row(self, table_name: str, data: dict):
        keys = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['?'] * len(keys))
        columns = ', '.join(keys)
        
        try:
            self.connect()
            cursor = self.connection.cursor()
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(query, values)
            self.connection.commit()
            return True, "Row inserted successfully"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def update_row(self, table_name: str, row_id: int, data: dict):
        set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
        values = list(data.values())
        values.append(row_id)
        
        try:
            self.connect()
            cursor = self.connection.cursor()
            query = f"UPDATE {table_name} SET {set_clause} WHERE rowid = ?"
            cursor.execute(query, values)
            self.connection.commit()
            return True, "Row updated successfully"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

    def create_table(self, table_name: str, columns: list):
        # columns example: [{"name": "id", "type": "INTEGER", "pk": True, "notnull": True, "auto_increment": True}, ...]
        try:
            self.connect()
            cursor = self.connection.cursor()
            
            col_defs = []
            for col in columns:
                def_str = f"{col['name']} {col['type']}"
                if col.get('pk'):
                    def_str += " PRIMARY KEY"
                if col.get('auto_increment') and col['type'].upper() == 'INTEGER':
                    def_str += " AUTOINCREMENT"
                if col.get('notnull'):
                    def_str += " NOT NULL"
                if col.get('default'):
                    def_str += f" DEFAULT {col['default']}"
                col_defs.append(def_str)
            
            query = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
            cursor.execute(query)
            self.connection.commit()
            return True, "Table created successfully"
        except Exception as e:
            return False, str(e)
        finally:
            self.close()

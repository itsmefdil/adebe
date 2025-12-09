import sqlite3
import os

# Database file path
db_path = "app.db"

def migrate():
    if not os.path.exists(db_path):
        print(f"Database file not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if category column exists
        cursor.execute("PRAGMA table_info(database_connections)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if "category" not in columns:
            print("Adding 'category' column to 'database_connections' table...")
            cursor.execute("ALTER TABLE database_connections ADD COLUMN category VARCHAR DEFAULT 'development'")
            conn.commit()
            print("Migration successful.")
        else:
            print("'category' column already exists.")

    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()

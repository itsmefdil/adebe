from sqlalchemy.orm import Session
from .models import DatabaseConnection, SessionLocal, init_db

# Initialize the database on module import (for simplicity in this demo)
init_db()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class ConnectionManager:
    def __init__(self, db: Session):
        self.db = db

    def get_all_connections(self):
        return self.db.query(DatabaseConnection).all()

    def get_connection(self, connection_id: int):
        return self.db.query(DatabaseConnection).filter(DatabaseConnection.id == connection_id).first()

    def create_connection(self, connection_data: dict):
        db_connection = DatabaseConnection(**connection_data)
        self.db.add(db_connection)
        self.db.commit()
        self.db.refresh(db_connection)
        return db_connection

    def update_connection(self, connection_id: int, connection_data: dict):
        db_connection = self.get_connection(connection_id)
        if db_connection:
            for key, value in connection_data.items():
                setattr(db_connection, key, value)
            self.db.commit()
            self.db.refresh(db_connection)
        return db_connection

    def delete_connection(self, connection_id: int):
        db_connection = self.get_connection(connection_id)
        if db_connection:
            self.db.delete(db_connection)
            self.db.commit()
            return True
        return False

from sqlalchemy.orm import Session
from sqlalchemy import func
from .models import DatabaseConnection, ActivityLog, SessionLocal, init_db
from app.utils.security import encrypt_password
from datetime import datetime, timedelta

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
        if "password" in connection_data and connection_data["password"]:
            connection_data["password"] = encrypt_password(connection_data["password"])
        
        db_connection = DatabaseConnection(**connection_data)
        self.db.add(db_connection)
        self.db.commit()
        self.db.refresh(db_connection)
        return db_connection

    def update_connection(self, connection_id: int, connection_data: dict):
        db_connection = self.get_connection(connection_id)
        if db_connection:
            # Handle password update: only encrypt if it's different from the stored encrypted password
            if "password" in connection_data:
                new_password = connection_data["password"]
                # If password is empty or None, keep the existing password
                if not new_password:
                    connection_data.pop("password")
                # If password is different from the stored encrypted password, encrypt it
                elif new_password != db_connection.password:
                    connection_data["password"] = encrypt_password(new_password)
                # If password is the same as stored (already encrypted), don't re-encrypt
                else:
                    connection_data.pop("password")
            
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
    
    def get_stats(self) -> dict:
        """Get dashboard statistics"""
        total = self.db.query(func.count(DatabaseConnection.id)).scalar() or 0
        return {
            "total_databases": total,
            "connected_count": 0  # Will be calculated via health checks
        }


class ActivityLogManager:
    def __init__(self, db: Session):
        self.db = db
    
    def log_activity(self, action: str, description: str, database_name: str = None, icon_type: str = "info"):
        """Log a new activity"""
        activity = ActivityLog(
            action=action,
            description=description,
            database_name=database_name,
            icon_type=icon_type
        )
        self.db.add(activity)
        self.db.commit()
        self.db.refresh(activity)
        return activity
    
    def get_recent_activities(self, limit: int = 5):
        """Get recent activities with human-readable time"""
        activities = self.db.query(ActivityLog).order_by(
            ActivityLog.created_at.desc()
        ).limit(limit).all()
        
        # Add time_ago property to each activity
        result = []
        now = datetime.utcnow()
        for activity in activities:
            time_diff = now - activity.created_at
            if time_diff < timedelta(minutes=1):
                time_ago = "just now"
            elif time_diff < timedelta(hours=1):
                mins = int(time_diff.total_seconds() / 60)
                time_ago = f"{mins} min{'s' if mins > 1 else ''} ago"
            elif time_diff < timedelta(days=1):
                hours = int(time_diff.total_seconds() / 3600)
                time_ago = f"{hours} hour{'s' if hours > 1 else ''} ago"
            else:
                days = time_diff.days
                time_ago = f"{days} day{'s' if days > 1 else ''} ago"
            
            result.append({
                "id": activity.id,
                "action": activity.action,
                "description": activity.description,
                "database_name": activity.database_name,
                "icon_type": activity.icon_type,
                "time_ago": time_ago
            })
        
        return result

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class DatabaseConnection(Base):
    __tablename__ = "database_connections"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    type = Column(String)  # MySQL, PostgreSQL, SQLite, MongoDB, Elasticsearch
    host = Column(String)
    port = Column(Integer)
    database_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    password = Column(String, nullable=True)  # In a real app, encrypt this!

# SQLite database for storing application data (connections, settings, etc.)
SQLALCHEMY_DATABASE_URL = "sqlite:///./app/data/app.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

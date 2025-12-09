# Adebe Database Manager

A powerful, async-enabled database management interface supporting MySQL, PostgreSQL, MongoDB, SQLite, and Elasticsearch.

## Features

- **Multi-Database Support**: Connect to and manage varied database systems.
- **Import/Export**: Asynchronously export tables to CSV/JSON or import data from files.
- **Backup/Restore**: Full database backup and restore functionality to Local, S3, or FTP storage.
- **Celery & Redis**: Robust background processing for long-running tasks.
- **Modern UI**: Clean, responsive interface built with Tailwind CSS and Jinja2.

## Prerequisites

- **Python 3.12+**
- **uv**: Fast Python package installer and resolver.
- **Redis**: Required for background task processing.
- **Database Tools**: `mysqldump`, `mysql` (for MySQL); `pg_dump`, `psql` (for PostgreSQL); `mongodump`, `mongorestore` (for MongoDB).

## Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd adebe
    ```

2.  **Install dependencies**:
    ```bash
    uv sync
    ```

3.  **Environment Configuration**:
    Copy `.env.example` to `.env` and configure your settings:
    ```bash
    cp .env.example .env
    ```
    
    Update `.env` with your specific configuration:
    ```properties
    # Security
    SECRET_KEY=your-secret-key
    ENCRYPTION_KEY=your-encryption-key
    
    # Celery & Redis
    CELERY_BROKER_URL=redis://localhost:6379/0
    CELERY_RESULT_BACKEND=redis://localhost:6379/0
    
    # Storage (LOCAL, S3, FTP)
    STORAGE_TYPE=LOCAL
    ```

## Running the Application

You need to run both the web server and the Celery worker.

### 1. Start Redis
Make sure Redis is running:
```bash
redis-server
```

### 2. Start Web Server
Run the FastAPI application:
```bash
uv run uvicorn app.main:app --reload
```
Access the dashboard at `http://localhost:8000`.

### 3. Start Celery Worker
Run the background worker for processing backups and imports:
```bash
uv run celery -A app.core.celery_app worker --loglevel=info
```

## Usage

- **Add Database**: Go to the dashboard and add your database credentials.
- **Browse Tables**: Click on a table to view data.
- **Import/Export**: Use the buttons in the table view to import/export data.
- **Backups**: Use the "Backups" button in the database dashboard to manage backups.

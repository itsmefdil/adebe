from fastapi import APIRouter, Request, Form, status, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from sqlalchemy.orm import Session
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager

router = APIRouter(prefix="/databases")

@router.get("", response_class=HTMLResponse)
def list_databases(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    databases = manager.get_all_connections()
    return templates.TemplateResponse("databases/list.html", {"request": request, "user": user, "databases": databases})

@router.get("/new", response_class=HTMLResponse)
def new_database(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/form.html", {"request": request, "user": user, "is_edit": False})

@router.post("/new")
def create_database(
    request: Request,
    name: str = Form(...),
    type: str = Form(...),
    host: str = Form(...),
    port: int = Form(0),
    database_name: str = Form(None),
    username: str = Form(None),
    password: str = Form(None),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    connection_data = {
        "name": name,
        "type": type,
        "host": host,
        "port": port,
        "database_name": database_name,
        "username": username,
        "password": password
    }
    
    manager = ConnectionManager(db)
    manager.create_connection(connection_data)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@router.get("/{db_id}/edit", response_class=HTMLResponse)
def edit_database(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database:
        return RedirectResponse(url="/databases")

    return templates.TemplateResponse("databases/form.html", {"request": request, "user": user, "is_edit": True, "database": database})

@router.post("/{db_id}/edit")
def update_database(
    request: Request,
    db_id: int,
    name: str = Form(...),
    type: str = Form(...),
    host: str = Form(...),
    port: int = Form(0),
    database_name: str = Form(None),
    username: str = Form(None),
    password: str = Form(None),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    connection_data = {
        "name": name,
        "type": type,
        "host": host,
        "port": port,
        "database_name": database_name,
        "username": username,
        "password": password
    }
    
    manager = ConnectionManager(db)
    manager.update_connection(db_id, connection_data)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@router.post("/{db_id}/delete")
def delete_database(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    manager.delete_connection(db_id)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

from app.utils.security import decrypt_password

@router.post("/test", response_class=HTMLResponse)
def test_database_connection(
    request: Request,
    type: str = Form(...),
    host: str = Form(...),
    port: int = Form(0),
    database_name: str = Form(None),
    username: str = Form(None),
    password: str = Form(None)
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<span class="text-sm font-medium text-red-600">Unauthorized</span>', status_code=401)

    # Decrypt password if it was sent encrypted (e.g. from edit form)
    # If it's a raw password (new connection or changed), decrypt_password returns it as is
    decrypted_password = decrypt_password(password)

    connection_details = {
        "host": host,
        "port": port,
        "database_name": database_name,
        "username": username,
        "password": decrypted_password
    }

    try:
        connector = None
        if type == "MySQL":
            from app.connectors.mysql_connector import MySQLConnector
            connector = MySQLConnector(connection_details)
        elif type == "PostgreSQL":
            from app.connectors.postgres_connector import PostgresConnector
            connector = PostgresConnector(connection_details)
        elif type == "SQLite":
            from app.connectors.sqlite_connector import SQLiteConnector
            connector = SQLiteConnector(connection_details)
        elif type == "MongoDB":
            from app.connectors.mongo_connector import MongoConnector
            connector = MongoConnector(connection_details)
        elif type == "Elasticsearch":
            from app.connectors.es_connector import ESConnector
            connector = ESConnector(connection_details)
        
        if connector:
            success, msg = connector.test_connection()
            if success:
                return HTMLResponse(f'<span class="text-sm font-medium text-green-600">{msg}</span>')
            else:
                return HTMLResponse(f'<span class="text-sm font-medium text-red-600">Connection failed: {msg}</span>')
        else:
             return HTMLResponse(f'<span class="text-sm font-medium text-red-600">Unsupported database type: {type}</span>')

    except Exception as e:
        return HTMLResponse(f'<span class="text-sm font-medium text-red-600">{str(e)}</span>')

@router.get("/{db_id}/health", response_class=HTMLResponse)
def check_connection_health(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-slate-300"></span>Unknown</span>')
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database:
        return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-red-500"></span>Not Found</span>')
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": decrypt_password(database.password)
    }
    
    try:
        connector = None
        if database.type == "MySQL":
            from app.connectors.mysql_connector import MySQLConnector
            connector = MySQLConnector(connection_details)
        elif database.type == "PostgreSQL":
            from app.connectors.postgres_connector import PostgresConnector
            connector = PostgresConnector(connection_details)
        elif database.type == "SQLite":
            from app.connectors.sqlite_connector import SQLiteConnector
            connector = SQLiteConnector(connection_details)
        elif database.type == "MongoDB":
            from app.connectors.mongo_connector import MongoConnector
            connector = MongoConnector(connection_details)
        elif database.type == "Elasticsearch":
            from app.connectors.es_connector import ESConnector
            connector = ESConnector(connection_details)
        
        if connector:
            success, msg = connector.test_connection()
            if success:
                return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-green-500"></span>Connected</span>')
            else:
                return HTMLResponse(f'<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-red-500"></span>Error</span>')
        else:
            return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-slate-300"></span>Unknown Type</span>')
    except Exception as e:
        return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-red-500"></span>Error</span>')

@router.get("/{db_id}/manage-button", response_class=HTMLResponse)
def get_manage_button(request: Request, db_id: int, db: Session = Depends(get_db)):
    """Return manage button only if database is connected"""
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<span class="flex-1 px-4 py-2 bg-slate-300 text-slate-500 text-sm font-medium rounded-lg flex items-center justify-center gap-2 cursor-not-allowed">Unauthorized</span>')
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database:
        return HTMLResponse('<span class="flex-1 px-4 py-2 bg-slate-300 text-slate-500 text-sm font-medium rounded-lg flex items-center justify-center gap-2 cursor-not-allowed">Not Found</span>')
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": decrypt_password(database.password)
    }
    
    try:
        connector = None
        if database.type == "MySQL":
            from app.connectors.mysql_connector import MySQLConnector
            connector = MySQLConnector(connection_details)
        elif database.type == "PostgreSQL":
            from app.connectors.postgres_connector import PostgresConnector
            connector = PostgresConnector(connection_details)
        elif database.type == "SQLite":
            from app.connectors.sqlite_connector import SQLiteConnector
            connector = SQLiteConnector(connection_details)
        elif database.type == "MongoDB":
            from app.connectors.mongo_connector import MongoConnector
            connector = MongoConnector(connection_details)
        elif database.type == "Elasticsearch":
            from app.connectors.es_connector import ESConnector
            connector = ESConnector(connection_details)
        
        if connector:
            success, msg = connector.test_connection()
            if success:
                # Return the manage button based on database type
                if database.type == "Elasticsearch":
                    href = f"/databases/{db_id}/elasticsearch"
                elif database.type == "MySQL":
                    href = f"/databases/{db_id}/mysql"
                elif database.type == "PostgreSQL":
                    href = f"/databases/postgresql"
                elif database.type == "SQLite":
                    href = f"/databases/sqlite"
                elif database.type == "MongoDB":
                    href = f"/databases/{db_id}/mongodb"
                else:
                    href = "#"
                
                return HTMLResponse(f'''<a href="{href}" class="flex-1 px-4 py-2 bg-primary text-white text-sm font-medium rounded-lg hover:bg-blue-600 transition-colors flex items-center justify-center gap-2">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
                    </svg>
                    Manage
                </a>''')
            else:
                # Not connected - show disabled state
                return HTMLResponse('''<span class="flex-1 px-4 py-2 bg-slate-300 text-slate-500 text-sm font-medium rounded-lg flex items-center justify-center gap-2 cursor-not-allowed">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636"></path>
                    </svg>
                    Disconnected
                </span>''')
        else:
            return HTMLResponse('<span class="flex-1 px-4 py-2 bg-slate-300 text-slate-500 text-sm font-medium rounded-lg flex items-center justify-center gap-2 cursor-not-allowed">Unknown Type</span>')
    except Exception as e:
        return HTMLResponse('''<span class="flex-1 px-4 py-2 bg-slate-300 text-slate-500 text-sm font-medium rounded-lg flex items-center justify-center gap-2 cursor-not-allowed">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
            </svg>
            Error
        </span>''')

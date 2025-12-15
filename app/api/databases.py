from fastapi import APIRouter, Request, Form, status, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from sqlalchemy.orm import Session
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager, ActivityLogManager

router = APIRouter(prefix="/databases")

@router.get("", response_class=HTMLResponse)
def list_databases(request: Request, category: str = None, type: str = None, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    all_databases = manager.get_all_connections()
    
    databases = all_databases
    if category and category != 'all':
        databases = [d for d in databases if d.category == category]
        
    if type and type != 'all':
        databases = [d for d in databases if d.type == type]
        
    template = "databases/partials/content.html" if request.headers.get("hx-request") else "databases/list.html"
        
    return templates.TemplateResponse(template, {
        "request": request, 
        "user": user, 
        "databases": databases,
        "current_category": category or 'all',
        "current_type": type or 'all'
    })

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
    category: str = Form("development"),
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
        "password": password,
        "category": category
    }
    
    manager = ConnectionManager(db)
    manager.create_connection(connection_data)
    
    # Log activity
    activity_manager = ActivityLogManager(db)
    activity_manager.log_activity(
        action="connection_created",
        description=f"Database '{name}' created",
        database_name=name,
        icon_type="success"
    )
    
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
    category: str = Form("development"),
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
        "password": password,
        "category": category
    }
    
    manager = ConnectionManager(db)
    manager.update_connection(db_id, connection_data)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@router.patch("/{db_id}/category", response_class=HTMLResponse)
def update_database_category(
    request: Request,
    db_id: int,
    category: str = Form(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database:
        return HTMLResponse("Database not found", status_code=404)
        
    # Update only the category
    manager.update_connection(db_id, {"category": category})
    
    # Return the new badge/select element
    # return templates.TemplateResponse("databases/partials/category_badge.html", {"request": request, "db": database})
    # Since we are using a select, we can just return a success toast or nothing if we want, 
    # but the user asked for a "toggle". 
    # Let's return a simple success span or keep the select as is. 
    # Actually, if we use hx-patch on the select itself, we don't need to return much other than maybe the selected state 
    # or just 200 OK.
    
    # Let's return the updated select component to be safe/clean
    return templates.TemplateResponse("components/category_select.html", {"request": request, "db": manager.get_connection(db_id)})

@router.post("/{db_id}/delete")
def delete_database(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    db_name = database.name if database else "Unknown"
    manager.delete_connection(db_id)
    
    # Log activity
    activity_manager = ActivityLogManager(db)
    activity_manager.log_activity(
        action="connection_deleted",
        description=f"Database '{db_name}' deleted",
        database_name=db_name,
        icon_type="warning"
    )
    
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
                    href = f"/databases/{db_id}/postgresql"
                elif database.type == "SQLite":
                    href = f"/databases/sqlite?id={db_id}"
                elif database.type == "MongoDB":
                    href = f"/databases/{db_id}/mongodb"
                else:
                    href = "#"
                
                return HTMLResponse(f'<a href="{href}" class="text-primary hover:text-blue-700 font-medium">Manage</a>')
            else:
                # Not connected - show disabled state
                return HTMLResponse('<span class="text-slate-400 cursor-not-allowed">Disconnected</span>')
        else:
            return HTMLResponse('<span class="text-slate-400">-</span>')
    except Exception as e:
        return HTMLResponse('<span class="text-red-500 text-sm">Error</span>')


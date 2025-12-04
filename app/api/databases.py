from fastapi import APIRouter, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager

router = APIRouter(prefix="/databases")

@router.get("", response_class=HTMLResponse)
async def list_databases(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    databases = manager.get_all_connections()
    return templates.TemplateResponse("databases/list.html", {"request": request, "user": user, "databases": databases})

@router.get("/new", response_class=HTMLResponse)
async def new_database(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/form.html", {"request": request, "user": user, "is_edit": False})

@router.post("/new")
async def create_database(
    request: Request,
    name: str = Form(...),
    type: str = Form(...),
    host: str = Form(...),
    port: int = Form(0),
    database_name: str = Form(None),
    username: str = Form(None),
    password: str = Form(None)
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
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    manager.create_connection(connection_data)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@router.get("/{db_id}/edit", response_class=HTMLResponse)
async def edit_database(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database:
        return RedirectResponse(url="/databases")

    return templates.TemplateResponse("databases/form.html", {"request": request, "user": user, "is_edit": True, "database": database})

@router.post("/{db_id}/edit")
async def update_database(
    request: Request,
    db_id: int,
    name: str = Form(...),
    type: str = Form(...),
    host: str = Form(...),
    port: int = Form(0),
    database_name: str = Form(None),
    username: str = Form(None),
    password: str = Form(None)
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
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    manager.update_connection(db_id, connection_data)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@router.post("/{db_id}/delete")
async def delete_database(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    manager.delete_connection(db_id)
    
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@router.post("/test", response_class=HTMLResponse)
async def test_database_connection(
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

    connection_details = {
        "host": host,
        "port": port,
        "database_name": database_name,
        "username": username,
        "password": password
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
async def check_connection_health(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-slate-300"></span>Unknown</span>')
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database:
        return HTMLResponse('<span class="flex items-center gap-2"><span class="w-2 h-2 rounded-full bg-red-500"></span>Not Found</span>')
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": database.password
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


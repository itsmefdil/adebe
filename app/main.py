from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session
from .auth import authenticate_user
from .database import get_db, ConnectionManager, ActivityLogManager
from .dependencies import templates, get_current_user
from .api import databases, mysql, postgresql, mongodb, sqlite, elasticsearch, backups
from .exceptions import global_exception_handler, not_found_handler
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.openapi.utils import get_openapi
import secrets
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

security = HTTPBasic()

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    current_username = credentials.username.encode("utf8")
    current_password = credentials.password.encode("utf8")
    correct_username = os.getenv("SWAGGER_USER", "admin").encode("utf8")
    correct_password = os.getenv("SWAGGER_PASSWORD", "admin").encode("utf8")
    
    is_correct_username = secrets.compare_digest(current_username, correct_username)
    is_correct_password = secrets.compare_digest(current_password, correct_password)
    
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

@app.get("/docs", include_in_schema=False)
async def get_swagger_documentation(username: str = Depends(get_current_username)):
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Docs")

@app.get("/redoc", include_in_schema=False)
async def get_redoc_documentation(username: str = Depends(get_current_username)):
    return get_redoc_html(openapi_url="/openapi.json", title="Docs")

@app.get("/openapi.json", include_in_schema=False)
async def openapi(username: str = Depends(get_current_username)):
    return get_openapi(title="Adebe API", version="0.1.0", routes=app.routes)

app.add_exception_handler(Exception, global_exception_handler)
app.add_exception_handler(404, not_found_handler)

# Add session middleware for simple auth
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY", "default-secret-key"))

app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include all database routers
app.include_router(databases.router)
app.include_router(mysql.router)
app.include_router(elasticsearch.router)
app.include_router(postgresql.router)
app.include_router(mongodb.router)
app.include_router(sqlite.router)
app.include_router(backups.router)

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard")
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = authenticate_user(username, password)
    if not user:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})
    
    request.session["user"] = user
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/logout")
async def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(url="/login")

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    # Fetch real connections and stats
    manager = ConnectionManager(db)
    databases = manager.get_some_connections()
    stats = manager.get_stats()
    
    # Fetch recent activities
    activity_manager = ActivityLogManager(db)
    activities = activity_manager.get_recent_activities(limit=5)
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request, 
        "user": user,
        "databases": databases,
        "stats": stats,
        "activities": activities
    })

@app.get("/query", response_class=HTMLResponse)
async def query_builder(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    databases = manager.get_all_connections()
    
    return templates.TemplateResponse("query.html", {
        "request": request, 
        "user": user,
        "databases": databases
    })

@app.get("/visualization", response_class=HTMLResponse)
async def visualization(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    databases = manager.get_all_connections()
    
    return templates.TemplateResponse("visualization.html", {
        "request": request, 
        "user": user,
        "databases": databases
    })

@app.get("/import", response_class=HTMLResponse)
async def import_data(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("operations/import.html", {"request": request, "user": user})

@app.get("/export", response_class=HTMLResponse)
async def export_data(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("operations/export.html", {"request": request, "user": user})

@app.get("/backup", response_class=HTMLResponse)
async def backup_restore(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("operations/backup.html", {"request": request, "user": user})

@app.post("/dashboard/quick-query", response_class=HTMLResponse)
async def execute_quick_query(
    request: Request,
    database_id: int = Form(...),
    query: str = Form(...),
    db: Session = Depends(get_db)
):
    """Execute a quick query from dashboard"""
    from app.utils.security import decrypt_password
    
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<div class="text-red-600 text-sm">Unauthorized</div>')
    
    manager = ConnectionManager(db)
    database = manager.get_connection(database_id)
    
    if not database:
        return HTMLResponse('<div class="text-red-600 text-sm">Database not found</div>')
    
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
        else:
            return HTMLResponse(f'<div class="text-yellow-600 text-sm">Quick query not supported for {database.type}</div>')
        
        if connector:
            result = connector.execute_query(query)
            
            if isinstance(result, dict) and "error" in result:
                return HTMLResponse(f'<div class="text-red-600 text-sm">{result["error"]}</div>')
            
            if isinstance(result, list) and len(result) > 0:
                # Build HTML table
                headers = result[0].keys()
                html = '<div class="overflow-x-auto"><table class="min-w-full text-sm text-left">'
                html += '<thead class="bg-slate-100"><tr>'
                for header in headers:
                    html += f'<th class="px-3 py-2 font-medium text-slate-700">{header}</th>'
                html += '</tr></thead><tbody class="divide-y divide-slate-200">'
                
                for row in result[:100]:  # Limit to 100 rows
                    html += '<tr class="hover:bg-slate-50">'
                    for header in headers:
                        val = row.get(header, '')
                        html += f'<td class="px-3 py-2 text-slate-600">{val}</td>'
                    html += '</tr>'
                
                html += '</tbody></table></div>'
                if len(result) > 100:
                    html += f'<div class="text-sm text-slate-500 mt-2">Showing 100 of {len(result)} rows</div>'
                return HTMLResponse(html)
            elif isinstance(result, dict) and "affected_rows" in result:
                return HTMLResponse(f'<div class="text-green-600 text-sm">Query executed. Affected rows: {result["affected_rows"]}</div>')
            else:
                return HTMLResponse('<div class="text-slate-500 text-sm">Query executed. No results returned.</div>')
    except Exception as e:
        return HTMLResponse(f'<div class="text-red-600 text-sm">Error: {str(e)}</div>')

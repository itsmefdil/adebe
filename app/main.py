from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from .auth import authenticate_user
import os

app = FastAPI()

# Add session middleware for simple auth
app.add_middleware(SessionMiddleware, secret_key="secret-key-should-be-env-var")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        return None
    return user

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
async def dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    # Mock data for dashboard
    databases = [
        {"name": "Production DB", "type": "PostgreSQL", "status": "Connected"},
        {"name": "Analytics DB", "type": "MySQL", "status": "Connected"},
        {"name": "Cache", "type": "Redis", "status": "Connected"},
    ]
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request, 
        "user": user,
        "databases": databases
    })

@app.get("/databases", response_class=HTMLResponse)
async def list_databases(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    databases = [
        {"name": "Production DB", "type": "PostgreSQL", "host": "localhost", "port": 5432, "status": "Connected"},
        {"name": "Analytics DB", "type": "MySQL", "host": "192.168.1.10", "port": 3306, "status": "Connected"},
        {"name": "App Data", "type": "SQLite", "host": "/var/lib/data/app.db", "port": 0, "status": "Connected"},
        {"name": "User Store", "type": "MongoDB", "host": "mongo-cluster", "port": 27017, "status": "Connected"},
        {"name": "Search Cluster", "type": "Elasticsearch", "host": "es-cluster", "port": 9200, "status": "Connected"},
        {"name": "Cache", "type": "Redis", "host": "localhost", "port": 6379, "status": "Connected"},
    ]
    return templates.TemplateResponse("databases/list.html", {"request": request, "user": user, "databases": databases})

@app.get("/databases/elasticsearch", response_class=HTMLResponse)
async def elasticsearch_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/elasticsearch.html", {"request": request, "user": user})

@app.get("/databases/mysql", response_class=HTMLResponse)
async def mysql_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/mysql.html", {"request": request, "user": user})

@app.get("/databases/mysql/tables/{table_name}/browse", response_class=HTMLResponse)
async def mysql_table_browse(request: Request, table_name: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/mysql_browse.html", {"request": request, "user": user, "table_name": table_name})

@app.get("/databases/mysql/tables/{table_name}/structure", response_class=HTMLResponse)
async def mysql_table_structure(request: Request, table_name: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/mysql_structure.html", {"request": request, "user": user, "table_name": table_name})

@app.get("/databases/postgresql", response_class=HTMLResponse)
async def postgresql_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/postgresql.html", {"request": request, "user": user})

@app.get("/databases/sqlite", response_class=HTMLResponse)
async def sqlite_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/sqlite.html", {"request": request, "user": user})

@app.get("/databases/mongodb", response_class=HTMLResponse)
async def mongodb_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/mongodb.html", {"request": request, "user": user})

@app.get("/databases/new", response_class=HTMLResponse)
async def new_database(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/form.html", {"request": request, "user": user, "is_edit": False})

@app.post("/databases/new")
async def create_database(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    # Mock creation
    return RedirectResponse(url="/databases", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/databases/{db_id}/edit", response_class=HTMLResponse)
async def edit_database(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/form.html", {"request": request, "user": user, "is_edit": True})

@app.get("/query", response_class=HTMLResponse)
async def query_builder(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("query.html", {"request": request, "user": user})

@app.get("/visualization", response_class=HTMLResponse)
async def visualization(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("visualization.html", {"request": request, "user": user})

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

from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session
from .auth import authenticate_user
from .database import get_db, ConnectionManager
from .dependencies import templates, get_current_user
from .api import databases, mysql, postgresql, mongodb, sqlite, elasticsearch
from .exceptions import global_exception_handler, not_found_handler
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

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
    
    # Fetch real connections
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    databases = manager.get_all_connections()
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request, 
        "user": user,
        "databases": databases
    })

@app.get("/query", response_class=HTMLResponse)
async def query_builder(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    databases = manager.get_all_connections()
    
    return templates.TemplateResponse("query.html", {
        "request": request, 
        "user": user,
        "databases": databases
    })

@app.get("/visualization", response_class=HTMLResponse)
async def visualization(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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

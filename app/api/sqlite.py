from fastapi import APIRouter, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager
from app.services.sqlite_service import SQLiteService
from pydantic import BaseModel
import json

router = APIRouter(prefix="/databases", tags=["sqlite"])

@router.get("/sqlite", response_class=HTMLResponse)
async def sqlite_dashboard(request: Request, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    if not database:
        return RedirectResponse(url="/databases")
        
    service = SQLiteService(database)
    stats = service.get_dashboard_stats()
    
    if "error" in stats:
        return HTMLResponse(f"Error connecting to database: {stats['error']}")

    return templates.TemplateResponse("databases/sqlite/dashboard.html", {
        "request": request, 
        "user": user,
        "database": database,
        "file_info": stats.get("file_info"),
        "tables": stats.get("tables"),
        "pragma": stats.get("pragma")
    })

@router.get("/sqlite/table/{table_name}", response_class=HTMLResponse)
async def view_table(request: Request, table_name: str, id: int, page: int = 1, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    if not database:
         return HTMLResponse("Database not found")
    
    service = SQLiteService(database)
    limit = 50
    data = service.browse_table(table_name, page, limit)
    
    return templates.TemplateResponse("databases/sqlite/browse.html", {
        "request": request,
        "user": user,
        "database": database,
        "table_name": table_name,
        "data": data,
        "page": page,
        "limit": limit
    })

@router.delete("/sqlite/table/{table_name}/{row_id}")
async def delete_row(request: Request, table_name: str, row_id: int, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    service = SQLiteService(database)
    success = service.delete_row(table_name, row_id)
    
    if success:
        return HTMLResponse("")
    else:
        return HTMLResponse("Failed to delete", status_code=500)

@router.get("/sqlite/table/{table_name}/add", response_class=HTMLResponse)
async def add_row(request: Request, table_name: str, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    service = SQLiteService(database)
    columns = service.get_table_info(table_name)
    
    return templates.TemplateResponse("databases/sqlite/form.html", {
        "request": request,
        "user": user,
        "database": database,
        "table_name": table_name,
        "columns": columns,
        "is_edit": False
    })

@router.post("/sqlite/table/{table_name}/add")
async def add_row_post(request: Request, table_name: str, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    form_data = await request.form()
    data = dict(form_data)
    
    service = SQLiteService(database)
    success, msg = service.insert_row(table_name, data)
    
    if success:
        return RedirectResponse(url=f"/databases/sqlite/table/{table_name}?id={id}", status_code=303)
    else:
        columns = service.get_table_info(table_name)
        return templates.TemplateResponse("databases/sqlite/form.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": columns,
            "is_edit": False,
            "error": msg,
            "form_data": data
        })

@router.get("/sqlite/table/{table_name}/edit/{row_id}", response_class=HTMLResponse)
async def edit_row(request: Request, table_name: str, row_id: int, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    service = SQLiteService(database)
    columns = service.get_table_info(table_name)
    row_data = service.get_row(table_name, row_id)
    
    if not row_data:
        return RedirectResponse(url=f"/databases/sqlite/table/{table_name}?id={id}")
    
    return templates.TemplateResponse("databases/sqlite/form.html", {
        "request": request,
        "user": user,
        "database": database,
        "table_name": table_name,
        "columns": columns,
        "is_edit": True,
        "row_id": row_id,
        "form_data": row_data
    })

@router.post("/sqlite/table/{table_name}/edit/{row_id}")
async def edit_row_post(request: Request, table_name: str, row_id: int, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    form_data = await request.form()
    data = dict(form_data)
    
    service = SQLiteService(database)
    success, msg = service.update_row(table_name, row_id, data)
    
    if success:
        return RedirectResponse(url=f"/databases/sqlite/table/{table_name}?id={id}", status_code=303)
    else:
        columns = service.get_table_info(table_name)
        return templates.TemplateResponse("databases/sqlite/form.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": columns,
            "is_edit": True,
            "row_id": row_id,
            "error": msg,
            "form_data": data
        })

@router.get("/sqlite/create-table", response_class=HTMLResponse)
async def create_table(request: Request, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    return templates.TemplateResponse("databases/sqlite/create_table.html", {
        "request": request,
        "user": user,
        "database": database
    })

@router.post("/sqlite/create-table")
async def create_table_post(request: Request, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    form_data = await request.form()
    
    schema_json = form_data.get("schema_json")
    table_name = form_data.get("table_name")
    
    if not schema_json or not table_name:
        return templates.TemplateResponse("databases/sqlite/create_table.html", {
            "request": request,
            "user": user,
            "database": database,
            "error": "Missing table name or columns"
        })
        
    try:
        columns = json.loads(schema_json)
    except:
         return templates.TemplateResponse("databases/sqlite/create_table.html", {
            "request": request,
            "user": user,
            "database": database,
            "error": "Invalid column data"
        })
    
    service = SQLiteService(database)
    success, msg = service.create_table(table_name, columns)
    
    if success:
        return RedirectResponse(url=f"/databases/sqlite?id={id}", status_code=303)
    else:
        return templates.TemplateResponse("databases/sqlite/create_table.html", {
            "request": request,
            "user": user,
            "database": database,
            "error": msg
        })

@router.get("/sqlite/table/{table_name}/structure", response_class=HTMLResponse)
async def view_structure(request: Request, table_name: str, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    service = SQLiteService(database)
    columns = service.get_table_info(table_name)
    
    return templates.TemplateResponse("databases/sqlite/structure.html", {
        "request": request,
        "user": user,
        "database": database,
        "table_name": table_name,
        "columns": columns
    })

@router.get("/sqlite/query", response_class=HTMLResponse)
async def view_query(request: Request, id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
        
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    return templates.TemplateResponse("databases/sqlite/query.html", {
        "request": request,
        "user": user,
        "database": database
    })

class QueryRequest(BaseModel):
    query: str

@router.post("/sqlite/query")
async def run_query(request: Request, id: int, query_req: QueryRequest, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return {"success": False, "error": "Unauthorized"}

    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    service = SQLiteService(database)
    result = service.execute_query(query_req.query)
    
    if isinstance(result, dict) and "error" in result:
        return {"success": False, "error": result["error"]}
    
    if isinstance(result, list):
        if not result:
             return {"success": True, "rows": [], "columns": []}
            
        columns = list(result[0].keys())
        return {"success": True, "rows": result, "columns": columns}
        
    elif isinstance(result, dict) and "affected_rows" in result:
         return {"success": True, "affected_rows": result["affected_rows"]}
    else:
         return {"success": True}

@router.delete("/sqlite/tables/{table_name}")
async def drop_table(request: Request, id: int, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
         return HTMLResponse("Unauthorized", status_code=401)
         
    manager = ConnectionManager(db)
    database = manager.get_connection(id)
    
    service = SQLiteService(database)
    success, msg = service.drop_table(table_name)
    
    return {"success": success, "error": msg if not success else None}

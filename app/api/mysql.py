from fastapi import APIRouter, Request, Query, Body, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from datetime import datetime, date, time
from decimal import Decimal
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager
from app.services.mysql_service import MySQLService

def serialize_value(value):
    """Convert non-JSON-serializable values to JSON-compatible formats."""
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    return value

def serialize_row(row):
    """Serialize a single row dictionary."""
    if isinstance(row, dict):
        return {key: serialize_value(val) for key, val in row.items()}
    return row

router = APIRouter(prefix="/databases", tags=["mysql"])

class ColumnDefinition(BaseModel):
    name: str
    type: str
    length: Optional[str] = None
    primary_key: Optional[bool] = False
    auto_increment: Optional[bool] = False
    nullable: Optional[bool] = True
    default: Optional[str] = None

class CreateTableRequest(BaseModel):
    table_name: str
    columns: List[ColumnDefinition]

class ExecuteQueryRequest(BaseModel):
    query: str

@router.get("/{db_id}/mysql", response_class=HTMLResponse)
def mysql_dashboard(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    service = MySQLService(database)
    stats = service.get_dashboard_stats()
    
    return templates.TemplateResponse("databases/mysql/dashboard.html", {
        "request": request,
        "user": user,
        "database": database,
        **stats
    })

@router.get("/{db_id}/mysql/processlist", response_class=HTMLResponse)
def mysql_processlist(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return HTMLResponse("Invalid database", status_code=400)
    
    try:
        service = MySQLService(database)
        processlist = service.get_processlist()
        
        return templates.TemplateResponse("databases/mysql/partials/processlist_rows.html", {
            "request": request,
            "processlist": processlist if processlist else []
        })
    except Exception as e:
        return HTMLResponse(f'<tr><td colspan="6" class="px-6 py-8 text-center text-red-500">Error: {str(e)}</td></tr>')

@router.get("/{db_id}/mysql/tables/{table_name}/browse", response_class=HTMLResponse)
def mysql_table_browse(
    request: Request, 
    db_id: int, 
    table_name: str,
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=1000),
    search: str = Query(None),
    sort_by: str = Query(None),
    sort_order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$"),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        result = service.browse_table(table_name, page, limit, search, sort_by, sort_order)
        
        return templates.TemplateResponse("databases/mysql/browse.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "current_page": page,
            "per_page": limit,
            "search": search,
            "sort_by": sort_by,
            "sort_order": sort_order,
            **result
        })
    except Exception as e:
        print(f"Error browsing table: {e}")
        return templates.TemplateResponse("databases/mysql/browse.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": [],
            "rows": [],
            "total_rows": 0,
            "error": str(e)
        })

@router.get("/{db_id}/mysql/tables/{table_name}/structure", response_class=HTMLResponse)
def mysql_table_structure(request: Request, db_id: int, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        structure = service.get_table_structure(table_name)
        
        return templates.TemplateResponse("databases/mysql/structure.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            **structure
        })
    except Exception as e:
        print(f"Error getting table structure: {e}")
        return templates.TemplateResponse("databases/mysql/structure.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": [],
            "indexes": [],
            "error": str(e)
        })

@router.delete("/{db_id}/mysql/tables/{table_name}/rows", response_class=JSONResponse)
def mysql_delete_row(
    request: Request, 
    db_id: int, 
    table_name: str,
    pk_column: str = Query(...),
    pk_value: str = Query(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        result = service.delete_row(table_name, pk_column, pk_value)
        
        if "error" in result:
             return JSONResponse({"error": result["error"]}, status_code=500)
             
        return JSONResponse({"success": True, "affected_rows": result.get("affected_rows", 0)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/{db_id}/mysql/tables/{table_name}/insert", response_class=HTMLResponse)
def mysql_insert_row_form(request: Request, db_id: int, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        # We need column info to build the form
        structure = service.get_table_structure(table_name)
        
        return templates.TemplateResponse("databases/mysql/row_form.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": structure['columns'],
            "is_edit": False,
            "row": None
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.post("/{db_id}/mysql/tables/{table_name}/insert", response_class=HTMLResponse)
async def mysql_insert_row(request: Request, db_id: int, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        form_data = await request.form()
        
        # Process form data
        data = {}
        # get_table_structure is blocking, run in threadpool
        structure = await run_in_threadpool(service.get_table_structure, table_name)
        columns = structure['columns']
        
        for col in columns:
            field_name = col['Field']
            is_null = form_data.get(f"{field_name}_null")
            value = form_data.get(field_name)
            
            if is_null:
                data[field_name] = None
            else:
                data[field_name] = value
        
        # insert_row is blocking, run in threadpool
        result = await run_in_threadpool(service.insert_row, table_name, data)
        
        if "error" in result:
             return templates.TemplateResponse("databases/mysql/row_form.html", {
                 "request": request,
                 "user": user,
                 "database": database,
                 "table_name": table_name,
                 "columns": columns,
                 "is_edit": False,
                 "row": data, # Preserve input
                 "error": result["error"]
            })
            
        return RedirectResponse(url=f"/databases/{db_id}/mysql/tables/{table_name}/browse", status_code=303)
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.get("/{db_id}/mysql/tables/{table_name}/rows/edit", response_class=HTMLResponse)
def mysql_edit_row_form(
    request: Request, 
    db_id: int, 
    table_name: str,
    pk_column: str = Query(...),
    pk_value: str = Query(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        structure = service.get_table_structure(table_name)
        row = service.get_row(table_name, pk_column, pk_value)
        
        if not row:
            return HTMLResponse("Row not found", status_code=404)
        
        return templates.TemplateResponse("databases/mysql/row_form.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": structure['columns'],
            "is_edit": True,
            "row": row
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.post("/{db_id}/mysql/tables/{table_name}/rows/edit", response_class=HTMLResponse)
async def mysql_update_row(
    request: Request, 
    db_id: int, 
    table_name: str,
    pk_column: str = Query(...),
    pk_value: str = Query(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        form_data = await request.form()
        
        # Process form data
        data = {}
        # get_table_structure is blocking
        structure = await run_in_threadpool(service.get_table_structure, table_name)
        columns = structure['columns']
        
        for col in columns:
            field_name = col['Field']
            # Skip PK update? Usually yes, unless we want to allow it. 
            # For now let's allow it but we need to be careful.
            # Actually, usually we don't update PK.
            if field_name == pk_column:
                continue
                
            is_null = form_data.get(f"{field_name}_null")
            value = form_data.get(field_name)
            
            if is_null:
                data[field_name] = None
            else:
                data[field_name] = value
        
        # update_row is blocking
        result = await run_in_threadpool(service.update_row, table_name, pk_column, pk_value, data)
        
        if "error" in result:
             return templates.TemplateResponse("databases/mysql/row_form.html", {
                 "request": request,
                 "user": user,
                 "database": database,
                 "table_name": table_name,
                 "columns": columns,
                 "is_edit": True,
                 "row": {**data, pk_column: pk_value}, 
                 "error": result["error"]
            })
            
        return RedirectResponse(url=f"/databases/{db_id}/mysql/tables/{table_name}/browse", status_code=303)
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.get("/{db_id}/mysql/create-table", response_class=HTMLResponse)
def mysql_create_table_form(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
        
    return templates.TemplateResponse("databases/mysql/create_table.html", {
        "request": request,
        "user": user,
        "database": database
    })

@router.post("/{db_id}/mysql/create-table", response_class=JSONResponse)
def mysql_create_table(
    request: Request, 
    db_id: int, 
    data: CreateTableRequest,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        # Convert Pydantic model to dict/list format expected by service
        columns_dict = [col.dict() for col in data.columns]
        service.create_table(data.table_name, columns_dict)
        
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/{db_id}/mysql/tables/{table_name}", response_class=JSONResponse)
def mysql_drop_table(request: Request, db_id: int, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        service.drop_table(table_name)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/{db_id}/mysql/query", response_class=JSONResponse)
def mysql_execute_query(
    request: Request, 
    db_id: int,
    query_data: ExecuteQueryRequest,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        # We need to handle results carefully. 
        # execute_raw_query returns a list of dictionaries.
        # We should also return column names if possible, but the list of dicts implies keys are columns.
        rows = service.execute_raw_query(query_data.query)
        
        columns = []
        if rows and len(rows) > 0:
            columns = list(rows[0].keys())
        
        # Serialize rows to handle datetime, Decimal, and other non-JSON types
        serialized_rows = [serialize_row(row) for row in rows] if rows else []
            
        return JSONResponse({
            "success": True, 
            "columns": columns, 
            "rows": serialized_rows
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/{db_id}/mysql/stats", response_class=JSONResponse)
def mysql_get_stats(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        stats = service.get_dashboard_stats()
        # stats contains 'tables', 'status', 'variables', 'processlist'
        # We need to make sure everything is JSON serializable
        # The tables list contains Row objects or dicts, which should be fine.
        return JSONResponse(stats)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/{db_id}/mysql/tables/{table_name}/columns/add", response_class=HTMLResponse)
def mysql_add_column_form(request: Request, db_id: int, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
        
    try:
        service = MySQLService(database)
        structure = service.get_table_structure(table_name)
        columns = [col['Field'] for col in structure['columns']]
        
        return templates.TemplateResponse("databases/mysql/add_column.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "existing_columns": columns
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.post("/{db_id}/mysql/tables/{table_name}/columns", response_class=JSONResponse)
async def mysql_add_column(
    request: Request, 
    db_id: int, 
    table_name: str, 
    data: ColumnDefinition,
    after: Optional[str] = Query(None),
    first: Optional[bool] = Query(False),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        col_def = data.dict()
        if after:
            col_def['after'] = after
        if first:
            col_def['first'] = True
            
        await run_in_threadpool(service.add_column, table_name, col_def)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/{db_id}/mysql/tables/{table_name}/columns/{column_name}/edit", response_class=HTMLResponse)
def mysql_edit_column_form(
    request: Request, 
    db_id: int, 
    table_name: str, 
    column_name: str, 
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
        
    try:
        service = MySQLService(database)
        structure = service.get_table_structure(table_name)
        # Find the column definition
        column = next((col for col in structure['columns'] if col['Field'] == column_name), None)
        
        if not column:
            return HTMLResponse("Column not found", status_code=404)
            
        return templates.TemplateResponse("databases/mysql/edit_column.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "column": column
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.post("/{db_id}/mysql/tables/{table_name}/columns/{column_name}", response_class=JSONResponse)
async def mysql_modify_column(
    request: Request, 
    db_id: int, 
    table_name: str, 
    column_name: str,
    data: ColumnDefinition,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        await run_in_threadpool(service.modify_column, table_name, column_name, data.dict())
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/{db_id}/mysql/tables/{table_name}/columns/{column_name}", response_class=JSONResponse)
async def mysql_drop_column(
    request: Request, 
    db_id: int, 
    table_name: str, 
    column_name: str,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        await run_in_threadpool(service.drop_column, table_name, column_name)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/{db_id}/mysql/tables/{table_name}/indexes/{index_name}", response_class=JSONResponse)
async def mysql_drop_index(
    request: Request, 
    db_id: int, 
    table_name: str, 
    index_name: str,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = MySQLService(database)
        await run_in_threadpool(service.drop_index, table_name, index_name)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

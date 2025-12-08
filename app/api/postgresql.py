from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, date, time
from decimal import Decimal
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager
from app.services.postgresql_service import PostgreSQLService

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

router = APIRouter(prefix="/databases", tags=["postgresql"])

class ColumnDefinition(BaseModel):
    name: str
    type: str
    length: Optional[str] = None
    primary_key: Optional[bool] = False
    nullable: Optional[bool] = True
    default: Optional[str] = None

class CreateTableRequest(BaseModel):
    table_name: str
    schema_name: str = "public"
    columns: List[ColumnDefinition]

class ExecuteQueryRequest(BaseModel):
    query: str


@router.get("/{db_id}/postgresql", response_class=HTMLResponse)
def postgresql_dashboard(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    service = PostgreSQLService(database)
    stats = service.get_dashboard_stats()
    
    return templates.TemplateResponse("databases/postgresql/dashboard.html", {
        "request": request,
        "user": user,
        "database": database,
        **stats
    })


@router.get("/{db_id}/postgresql/activity", response_class=HTMLResponse)
def postgresql_activity(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return HTMLResponse("Invalid database", status_code=400)
    
    try:
        service = PostgreSQLService(database)
        activities = service.get_activity()
        
        return templates.TemplateResponse("databases/postgresql/partials/activity_rows.html", {
            "request": request,
            "activities": activities if activities else []
        })
    except Exception as e:
        return HTMLResponse(f'<tr><td colspan="5" class="px-6 py-8 text-center text-red-500">Error: {str(e)}</td></tr>')


@router.get("/{db_id}/postgresql/tables/{schema}/{table_name}/browse", response_class=HTMLResponse)
def postgresql_table_browse(
    request: Request, 
    db_id: int, 
    schema: str,
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
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = PostgreSQLService(database)
        result = service.browse_table(schema, table_name, page, limit, search, sort_by, sort_order)
        
        return templates.TemplateResponse("databases/postgresql/browse.html", {
            "request": request,
            "user": user,
            "database": database,
            "schema": schema,
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
        return templates.TemplateResponse("databases/postgresql/browse.html", {
            "request": request,
            "user": user,
            "database": database,
            "schema": schema,
            "table_name": table_name,
            "columns": [],
            "rows": [],
            "total_rows": 0,
            "error": str(e)
        })


@router.get("/{db_id}/postgresql/tables/{schema}/{table_name}/structure", response_class=HTMLResponse)
def postgresql_table_structure(request: Request, db_id: int, schema: str, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = PostgreSQLService(database)
        structure = service.get_table_structure(schema, table_name)
        
        return templates.TemplateResponse("databases/postgresql/structure.html", {
            "request": request,
            "user": user,
            "database": database,
            "schema": schema,
            "table_name": table_name,
            **structure
        })
    except Exception as e:
        print(f"Error getting table structure: {e}")
        return templates.TemplateResponse("databases/postgresql/structure.html", {
            "request": request,
            "user": user,
            "database": database,
            "schema": schema,
            "table_name": table_name,
            "columns": [],
            "indexes": [],
            "error": str(e)
        })


@router.delete("/{db_id}/postgresql/tables/{schema}/{table_name}/rows", response_class=JSONResponse)
def postgresql_delete_row(
    request: Request, 
    db_id: int, 
    schema: str,
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
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        result = service.delete_row(schema, table_name, pk_column, pk_value)
        
        if isinstance(result, dict) and "error" in result:
            return JSONResponse({"error": result["error"]}, status_code=500)
             
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/{db_id}/postgresql/tables/{schema}/{table_name}/insert", response_class=HTMLResponse)
def postgresql_insert_row_form(request: Request, db_id: int, schema: str, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = PostgreSQLService(database)
        structure = service.get_table_structure(schema, table_name)
        
        return templates.TemplateResponse("databases/postgresql/row_form.html", {
            "request": request,
            "user": user,
            "database": database,
            "schema": schema,
            "table_name": table_name,
            "columns": structure['columns'],
            "is_edit": False,
            "row": None
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")


@router.post("/{db_id}/postgresql/tables/{schema}/{table_name}/insert", response_class=HTMLResponse)
async def postgresql_insert_row(request: Request, db_id: int, schema: str, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = PostgreSQLService(database)
        form_data = await request.form()
        
        # Get structure
        structure = await run_in_threadpool(service.get_table_structure, schema, table_name)
        columns = structure['columns']
        
        # Process form data
        data = {}
        for col in columns:
            field_name = col['name']
            is_null = form_data.get(f"{field_name}_null")
            value = form_data.get(field_name)
            
            if is_null:
                data[field_name] = None
            elif value:
                data[field_name] = value
        
        result = await run_in_threadpool(service.insert_row, schema, table_name, data)
        
        if isinstance(result, dict) and "error" in result:
            return templates.TemplateResponse("databases/postgresql/row_form.html", {
                "request": request,
                "user": user,
                "database": database,
                "schema": schema,
                "table_name": table_name,
                "columns": columns,
                "is_edit": False,
                "row": data,
                "error": result["error"]
            })
            
        return RedirectResponse(url=f"/databases/{db_id}/postgresql/tables/{schema}/{table_name}/browse", status_code=303)
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")


@router.get("/{db_id}/postgresql/tables/{schema}/{table_name}/rows/edit", response_class=HTMLResponse)
def postgresql_edit_row_form(
    request: Request, 
    db_id: int, 
    schema: str,
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
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = PostgreSQLService(database)
        structure = service.get_table_structure(schema, table_name)
        row = service.get_row(schema, table_name, pk_column, pk_value)
        
        if not row:
            return HTMLResponse("Row not found", status_code=404)
        
        return templates.TemplateResponse("databases/postgresql/row_form.html", {
            "request": request,
            "user": user,
            "database": database,
            "schema": schema,
            "table_name": table_name,
            "columns": structure['columns'],
            "is_edit": True,
            "row": row,
            "pk_column": pk_column,
            "pk_value": pk_value
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")


@router.post("/{db_id}/postgresql/tables/{schema}/{table_name}/rows/edit", response_class=HTMLResponse)
async def postgresql_update_row(
    request: Request, 
    db_id: int, 
    schema: str,
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
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = PostgreSQLService(database)
        form_data = await request.form()
        
        structure = await run_in_threadpool(service.get_table_structure, schema, table_name)
        columns = structure['columns']
        
        # Process form data
        data = {}
        for col in columns:
            field_name = col['name']
            if field_name == pk_column:
                continue
                
            is_null = form_data.get(f"{field_name}_null")
            value = form_data.get(field_name)
            
            if is_null:
                data[field_name] = None
            else:
                data[field_name] = value
        
        result = await run_in_threadpool(service.update_row, schema, table_name, pk_column, pk_value, data)
        
        if isinstance(result, dict) and "error" in result:
            return templates.TemplateResponse("databases/postgresql/row_form.html", {
                "request": request,
                "user": user,
                "database": database,
                "schema": schema,
                "table_name": table_name,
                "columns": columns,
                "is_edit": True,
                "row": {**data, pk_column: pk_value},
                "error": result["error"]
            })
            
        return RedirectResponse(url=f"/databases/{db_id}/postgresql/tables/{schema}/{table_name}/browse", status_code=303)
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")


@router.get("/{db_id}/postgresql/create-table", response_class=HTMLResponse)
def postgresql_create_table_form(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return RedirectResponse(url="/databases")
        
    return templates.TemplateResponse("databases/postgresql/create_table.html", {
        "request": request,
        "user": user,
        "database": database
    })


@router.post("/{db_id}/postgresql/create-table", response_class=JSONResponse)
def postgresql_create_table(
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
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        columns_dict = [col.dict() for col in data.columns]
        service.create_table(data.schema_name, data.table_name, columns_dict)
        
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/{db_id}/postgresql/tables/{schema}/{table_name}", response_class=JSONResponse)
def postgresql_drop_table(request: Request, db_id: int, schema: str, table_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        service.drop_table(schema, table_name)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/{db_id}/postgresql/query", response_class=JSONResponse)
def postgresql_execute_query(
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
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        rows = service.execute_raw_query(query_data.query)
        
        if isinstance(rows, dict) and "error" in rows:
            return JSONResponse({"error": rows["error"]}, status_code=400)
        
        columns = []
        if rows and len(rows) > 0:
            columns = list(rows[0].keys())
        
        serialized_rows = [serialize_row(row) for row in rows] if rows else []
            
        return JSONResponse({
            "success": True, 
            "columns": columns, 
            "rows": serialized_rows
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/{db_id}/postgresql/stats", response_class=JSONResponse)
def postgresql_get_stats(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        stats = service.get_dashboard_stats()
        return JSONResponse(stats)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/{db_id}/postgresql/tables/{schema}/{table_name}/columns/{column_name}", response_class=JSONResponse)
async def postgresql_drop_column(
    request: Request, 
    db_id: int, 
    schema: str,
    table_name: str, 
    column_name: str,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        await run_in_threadpool(service.drop_column, schema, table_name, column_name)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.delete("/{db_id}/postgresql/indexes/{schema}/{index_name}", response_class=JSONResponse)
async def postgresql_drop_index(
    request: Request, 
    db_id: int, 
    schema: str,
    index_name: str,
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "PostgreSQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        service = PostgreSQLService(database)
        await run_in_threadpool(service.drop_index, schema, index_name)
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

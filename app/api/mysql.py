from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager
from app.services.mysql_service import MySQLService

router = APIRouter(prefix="/databases", tags=["mysql"])

@router.get("/{db_id}/mysql", response_class=HTMLResponse)
async def mysql_dashboard(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_processlist(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_table_browse(
    request: Request, 
    db_id: int, 
    table_name: str,
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=1000),
    search: str = Query(None),
    sort_by: str = Query(None),
    sort_order: str = Query("ASC", regex="^(ASC|DESC|asc|desc)$")
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_table_structure(request: Request, db_id: int, table_name: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_delete_row(
    request: Request, 
    db_id: int, 
    table_name: str,
    pk_column: str = Query(...),
    pk_value: str = Query(...)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_insert_row_form(request: Request, db_id: int, table_name: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_insert_row(request: Request, db_id: int, table_name: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        form_data = await request.form()
        
        # Process form data
        data = {}
        structure = service.get_table_structure(table_name)
        columns = structure['columns']
        
        for col in columns:
            field_name = col['Field']
            is_null = form_data.get(f"{field_name}_null")
            value = form_data.get(field_name)
            
            if is_null:
                data[field_name] = None
            else:
                data[field_name] = value
        
        result = service.insert_row(table_name, data)
        
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
async def mysql_edit_row_form(
    request: Request, 
    db_id: int, 
    table_name: str,
    pk_column: str = Query(...),
    pk_value: str = Query(...)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
    pk_value: str = Query(...)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
    
    try:
        service = MySQLService(database)
        form_data = await request.form()
        
        # Process form data
        data = {}
        structure = service.get_table_structure(table_name)
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
        
        result = service.update_row(table_name, pk_column, pk_value, data)
        
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
async def mysql_create_table_form(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return RedirectResponse(url="/databases")
        
    return templates.TemplateResponse("databases/mysql/create_table.html", {
        "request": request,
        "user": user,
        "database": database
    })

@router.post("/{db_id}/mysql/create-table", response_class=JSONResponse)
async def mysql_create_table(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MySQL":
        return JSONResponse({"error": "Invalid database"}, status_code=400)
    
    try:
        data = await request.json()
        table_name = data.get('table_name')
        columns = data.get('columns')
        
        if not table_name or not columns:
            return JSONResponse({"error": "Missing table name or columns"}, status_code=400)
            
        service = MySQLService(database)
        service.create_table(table_name, columns)
        
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/{db_id}/mysql/tables/{table_name}", response_class=JSONResponse)
async def mysql_drop_table(request: Request, db_id: int, table_name: str):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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
async def mysql_execute_query(request: Request, db_id: int):
    # ... (existing code) ...
    try:
        # ... (existing code) ...
        return JSONResponse({
            "success": True, 
            "columns": columns, 
            "rows": rows if rows else []
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/{db_id}/mysql/stats", response_class=JSONResponse)
async def mysql_get_stats(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
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

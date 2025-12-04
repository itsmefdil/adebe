from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager

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
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.mysql_connector import MySQLConnector
        connector = MySQLConnector(connection_details)
        
        # Get server status
        status_vars = connector.execute_query("SHOW STATUS")
        variables = connector.execute_query("SHOW VARIABLES")
        tables = connector.execute_query("SHOW TABLE STATUS")
        processlist = connector.execute_query("SHOW PROCESSLIST")
        
        # Parse status variables
        status_dict = {item['Variable_name']: item['Value'] for item in status_vars} if status_vars else {}
        var_dict = {item['Variable_name']: item['Value'] for item in variables} if variables else {}
        
        return templates.TemplateResponse("databases/mysql/dashboard.html", {
            "request": request,
            "user": user,
            "database": database,
            "status": status_dict,
            "variables": var_dict,
            "tables": tables if tables else [],
            "processlist": processlist if processlist else []
        })
    except Exception as e:
        print(f"Error fetching MySQL data: {e}")
        return templates.TemplateResponse("databases/mysql/dashboard.html", {
            "request": request,
            "user": user,
            "database": database,
            "status": {},
            "variables": {},
            "tables": [],
            "processlist": []
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
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.mysql_connector import MySQLConnector
        connector = MySQLConnector(connection_details)
        processlist = connector.execute_query("SHOW PROCESSLIST")
        
        html = ""
        if processlist:
            for proc in processlist[:10]:
                html += f"""
                <tr class="hover:bg-slate-50 transition-colors">
                    <td class="px-6 py-4 font-mono text-xs">{proc.get('Id', '')}</td>
                    <td class="px-6 py-4">{proc.get('User', '')}</td>
                    <td class="px-6 py-4">{proc.get('Command', '')}</td>
                    <td class="px-6 py-4">{proc.get('Time', '')}</td>
                    <td class="px-6 py-4">{proc.get('State', '') or ''}</td>
                    <td class="px-6 py-4 font-mono text-xs truncate max-w-xs">{proc.get('Info', '') or 'NULL'}</td>
                </tr>
                """
        else:
            html = '<tr><td colspan="6" class="px-6 py-8 text-center text-slate-500">No active processes</td></tr>'
        
        return HTMLResponse(html)
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
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.mysql_connector import MySQLConnector
        connector = MySQLConnector(connection_details)
        
        # Get column names first
        columns_info = connector.execute_query(f"SHOW COLUMNS FROM `{table_name}`")
        columns = [col['Field'] for col in columns_info] if columns_info else []
        
        # Build search query
        where_clause = ""
        if search and columns:
            safe_search = search.replace("'", "\\'")
            conditions = [f"`{col}` LIKE '%{safe_search}%'" for col in columns]
            where_clause = " WHERE " + " OR ".join(conditions)
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM `{table_name}`{where_clause}"
        count_result = connector.execute_query(count_query)
        total_rows = count_result[0]['total'] if count_result else 0
        
        # Calculate pagination
        total_pages = (total_rows + limit - 1) // limit
        start_index = (page - 1) * limit + 1 if total_rows > 0 else 0
        end_index = min(page * limit, total_rows)
        
        # Build sort clause
        order_clause = ""
        if sort_by and sort_by in columns:
            order_clause = f" ORDER BY `{sort_by}` {sort_order}"
        
        # Get table data
        offset = (page - 1) * limit
        data_query = f"SELECT * FROM `{table_name}`{where_clause}{order_clause} LIMIT {limit} OFFSET {offset}"
        rows = connector.execute_query(data_query)
        
        return templates.TemplateResponse("databases/mysql/browse.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": columns,
            "rows": rows if rows else [],
            "total_rows": total_rows,
            "current_page": page,
            "per_page": limit,
            "total_pages": total_pages,
            "start_index": start_index,
            "end_index": end_index,
            "end_index": end_index,
            "search": search,
            "sort_by": sort_by,
            "sort_order": sort_order
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
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "database_name": database.database_name,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.mysql_connector import MySQLConnector
        connector = MySQLConnector(connection_details)
        
        # Get column information
        columns = connector.execute_query(f"SHOW FULL COLUMNS FROM `{table_name}`")
        
        # Get index information
        indexes = connector.execute_query(f"SHOW INDEX FROM `{table_name}`")
        
        # Group indexes by Key_name
        index_dict = {}
        if indexes:
            for idx in indexes:
                key_name = idx['Key_name']
                if key_name not in index_dict:
                    index_dict[key_name] = {
                        'name': key_name,
                        'type': idx['Index_type'],
                        'unique': 'Yes' if idx['Non_unique'] == 0 else 'No',
                        'columns': []
                    }
                index_dict[key_name]['columns'].append(idx['Column_name'])
        
        # Convert to list
        indexes_list = []
        for key, value in index_dict.items():
            value['columns_str'] = ', '.join(value['columns'])
            indexes_list.append(value)
        
        return templates.TemplateResponse("databases/mysql/structure.html", {
            "request": request,
            "user": user,
            "database": database,
            "table_name": table_name,
            "columns": columns if columns else [],
            "indexes": indexes_list
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

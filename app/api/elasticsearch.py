from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager

router = APIRouter(prefix="/databases", tags=["elasticsearch"])

@router.get("/{db_id}/elasticsearch", response_class=HTMLResponse)
async def elasticsearch_dashboard(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return RedirectResponse(url="/databases")

    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    from app.connectors.es_connector import ESConnector
    connector = ESConnector(connection_details)
    stats = connector.get_cluster_stats()
    indices = connector.get_indices()
    
    return templates.TemplateResponse("databases/elasticsearch/dashboard.html", {
        "request": request, 
        "user": user, 
        "database": database,
        "stats": stats,
        "indices": indices
    })




@router.post("/{db_id}/elasticsearch/query", response_class=HTMLResponse)
async def execute_es_query(
    request: Request,
    db_id: int,
    method: str = Form(...),
    endpoint: str = Form(...),
    query: str = Form("")
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<pre class="text-red-400">Unauthorized</pre>')
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse('<pre class="text-red-400">Invalid database</pre>')
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import json
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Build full URL
        url = f"{connector.base_url}{endpoint}"
        
        # Execute request
        import requests
        
        if method == "GET":
            response = requests.get(
                url,
                auth=connector.auth,
                verify=False,
                timeout=30
            )
        elif method == "POST":
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                url,
                auth=connector.auth,
                data=query if query else "{}",
                headers=headers,
                verify=False,
                timeout=30
            )
        elif method == "PUT":
            headers = {"Content-Type": "application/json"}
            response = requests.put(
                url,
                auth=connector.auth,
                data=query if query else "{}",
                headers=headers,
                verify=False,
                timeout=30
            )
        elif method == "DELETE":
            response = requests.delete(
                url,
                auth=connector.auth,
                verify=False,
                timeout=30
            )
        else:
            return HTMLResponse('<pre class="text-red-400">Unsupported method</pre>')
        
        # Format response
        try:
            result = json.dumps(response.json(), indent=2)
        except:
            result = response.text
        
        connector.close()
        return HTMLResponse(f'<pre>{result}</pre>')
        
    except Exception as e:
        return HTMLResponse(f'<pre class="text-red-400">Error: {str(e)}</pre>')


@router.get("/{db_id}/elasticsearch/{index_name}/inspect", response_class=HTMLResponse)
async def inspect_es_index(request: Request, db_id: int, index_name: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return RedirectResponse(url="/databases")
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Get index info
        indices_response = requests.get(
            f"{connector.base_url}/_cat/indices/{index_name}?format=json",
            auth=connector.auth,
            verify=False,
            timeout=10
        )
        index_info = indices_response.json()[0] if indices_response.status_code == 200 else None
        
        # Get mappings
        mappings_response = requests.get(
            f"{connector.base_url}/{index_name}/_mapping",
            auth=connector.auth,
            verify=False,
            timeout=10
        )
        mappings = mappings_response.json() if mappings_response.status_code == 200 else None
        
        # Get settings
        settings_response = requests.get(
            f"{connector.base_url}/{index_name}/_settings",
            auth=connector.auth,
            verify=False,
            timeout=10
        )
        settings = settings_response.json() if settings_response.status_code == 200 else None
        
        connector.close()
        
        return templates.TemplateResponse("databases/elasticsearch/inspect.html", {
            "request": request,
            "user": user,
            "database": database,
            "index_name": index_name,
            "index_info": index_info,
            "mappings": mappings,
            "settings": settings
        })
    except Exception as e:
        print(f"Error inspecting index: {e}")
        return RedirectResponse(url=f"/databases/{db_id}/elasticsearch")


@router.post("/{db_id}/elasticsearch/{index_name}/search", response_class=HTMLResponse)
async def search_es_index(
    request: Request,
    db_id: int,
    index_name: str,
    query: str = Form(""),
    size: int = Form(10)
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse('<pre class="text-red-400">Unauthorized</pre>')
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse('<pre class="text-red-400">Invalid database</pre>')
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        import json
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Build search query
        if query and query.strip():
            search_body = {
                "query": {
                    "query_string": {
                        "query": query
                    }
                },
                "size": size
            }
        else:
            search_body = {
                "query": {"match_all": {}},
                "size": size
            }
        
        # Execute search
        response = requests.post(
            f"{connector.base_url}/{index_name}/_search",
            auth=connector.auth,
            json=search_body,
            verify=False,
            timeout=30
        )
        
        connector.close()
        
        if response.status_code == 200:
            result = response.json()
            hits = result.get('hits', {}).get('hits', [])
            
            if not hits:
                return HTMLResponse('<div class="text-slate-500 text-sm">No documents found</div>')
            
            # Format results as cards
            html = ''
            for hit in hits:
                doc_id = hit['_id']
                source = hit['_source']
                source_json = json.dumps(source, indent=2)
                escaped_source = source_json.replace("'", "\\'").replace('"', '&quot;')
                
                html += f'''
                <div class="bg-white border border-slate-200 rounded-lg p-4">
                    <div class="flex items-start justify-between mb-2">
                        <div class="flex-1">
                            <div class="text-xs text-slate-500 mb-1">ID: {doc_id}</div>
                            <div class="text-sm font-mono text-slate-700 whitespace-pre-wrap">{json.dumps(source, indent=2)}</div>
                        </div>
                        <div class="flex gap-2 ml-4">
                            <button onclick='showEditDocModal("{doc_id}", {json.dumps(source)})' 
                                    class="text-primary hover:text-blue-700 text-sm font-medium">
                                Edit
                            </button>
                            <button onclick="deleteDocument('{doc_id}')" 
                                    class="text-red-600 hover:text-red-700 text-sm font-medium">
                                Delete
                            </button>
                        </div>
                    </div>
                </div>
                '''
            
            return HTMLResponse(html)
        else:
            return HTMLResponse(f'<pre class="text-red-400">Error: {response.text}</pre>')
        
    except Exception as e:
        return HTMLResponse(f'<pre class="text-red-400">Error: {str(e)}</pre>')


@router.post("/{db_id}/elasticsearch/indices/create")
async def create_es_index(
    request: Request,
    db_id: int,
    index_name: str = Form(...),
    shards: int = Form(1),
    replicas: int = Form(1),
    mappings: str = Form(""),
    settings: str = Form("")
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse("Invalid database", status_code=400)
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        import json
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Build index body
        body = {
            "settings": {
                "number_of_shards": shards,
                "number_of_replicas": replicas
            }
        }
        
        # Add custom mappings if provided
        if mappings and mappings.strip():
            try:
                mappings_obj = json.loads(mappings)
                body["mappings"] = mappings_obj
            except json.JSONDecodeError:
                return HTMLResponse("Invalid mappings JSON", status_code=400)
        
        # Add custom settings if provided
        if settings and settings.strip():
            try:
                settings_obj = json.loads(settings)
                body["settings"].update(settings_obj)
            except json.JSONDecodeError:
                return HTMLResponse("Invalid settings JSON", status_code=400)
        
        # Create index
        response = requests.put(
            f"{connector.base_url}/{index_name}",
            auth=connector.auth,
            json=body,
            verify=False,
            timeout=30
        )
        
        connector.close()
        
        if response.status_code in [200, 201]:
            return HTMLResponse("OK")
        else:
            return HTMLResponse(f"Error: {response.text}", status_code=response.status_code)
        
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}", status_code=500)


@router.post("/{db_id}/elasticsearch/indices/{index_name}/delete")
async def delete_es_index(request: Request, db_id: int, index_name: str):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse("Invalid database", status_code=400)
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Delete index
        response = requests.delete(
            f"{connector.base_url}/{index_name}",
            auth=connector.auth,
            verify=False,
            timeout=30
        )
        
        connector.close()
        
        if response.status_code == 200:
            return HTMLResponse("OK")
        else:
            return HTMLResponse(f"Error: {response.text}", status_code=response.status_code)
        
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}", status_code=500)


@router.post("/{db_id}/elasticsearch/{index_name}/document/create")
async def create_es_document(
    request: Request,
    db_id: int,
    index_name: str,
    doc_body: str = Form(...),
    doc_id: str = Form(None)
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse("Invalid database", status_code=400)
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        import json
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Parse document body
        try:
            body = json.loads(doc_body)
        except json.JSONDecodeError:
            return HTMLResponse("Invalid JSON", status_code=400)
        
        # Create document
        if doc_id and doc_id.strip():
            # Create with specific ID
            response = requests.put(
                f"{connector.base_url}/{index_name}/_doc/{doc_id}",
                auth=connector.auth,
                json=body,
                verify=False,
                timeout=30
            )
        else:
            # Auto-generate ID
            response = requests.post(
                f"{connector.base_url}/{index_name}/_doc",
                auth=connector.auth,
                json=body,
                verify=False,
                timeout=30
            )
        
        connector.close()
        
        if response.status_code in [200, 201]:
            return HTMLResponse("OK")
        else:
            return HTMLResponse(f"Error: {response.text}", status_code=response.status_code)
        
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}", status_code=500)


@router.post("/{db_id}/elasticsearch/{index_name}/document/{doc_id}/update")
async def update_es_document(
    request: Request,
    db_id: int,
    index_name: str,
    doc_id: str,
    doc_body: str = Form(...)
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse("Invalid database", status_code=400)
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        import json
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Parse document body
        try:
            body = json.loads(doc_body)
        except json.JSONDecodeError:
            return HTMLResponse("Invalid JSON", status_code=400)
        
        # Update document
        response = requests.put(
            f"{connector.base_url}/{index_name}/_doc/{doc_id}",
            auth=connector.auth,
            json=body,
            verify=False,
            timeout=30
        )
        
        connector.close()
        
        if response.status_code == 200:
            return HTMLResponse("OK")
        else:
            return HTMLResponse(f"Error: {response.text}", status_code=response.status_code)
        
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}", status_code=500)


@router.post("/{db_id}/elasticsearch/{index_name}/document/{doc_id}/delete")
async def delete_es_document(
    request: Request,
    db_id: int,
    index_name: str,
    doc_id: str
):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("Unauthorized", status_code=401)
    
    db_session = next(get_db())
    manager = ConnectionManager(db_session)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "Elasticsearch":
        return HTMLResponse("Invalid database", status_code=400)
    
    connection_details = {
        "host": database.host,
        "port": database.port,
        "username": database.username,
        "password": database.password
    }
    
    try:
        from app.connectors.es_connector import ESConnector
        import requests
        
        connector = ESConnector(connection_details)
        connector.connect()
        
        # Delete document
        response = requests.delete(
            f"{connector.base_url}/{index_name}/_doc/{doc_id}",
            auth=connector.auth,
            verify=False,
            timeout=30
        )
        
        connector.close()
        
        if response.status_code == 200:
            return HTMLResponse("OK")
        else:
            return HTMLResponse(f"Error: {response.text}", status_code=response.status_code)
        
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}", status_code=500)


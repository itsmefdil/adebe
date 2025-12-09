from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager
from app.services.elasticsearch_service import ElasticsearchService
import json

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

    service = ElasticsearchService(database)
    stats, indices = service.get_dashboard_stats()
    
    service.close()
    
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
    
    service = ElasticsearchService(database)
    result = service.execute_query(method, endpoint, query)
    service.close()

    if isinstance(result, dict) and "error" in result:
         return HTMLResponse(f'<pre class="text-red-400">Error: {result["error"]}</pre>')

    try:
        formatted_result = json.dumps(result, indent=2)
    except:
        formatted_result = str(result)
        
    return HTMLResponse(f'<pre>{formatted_result}</pre>')

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
    
    service = ElasticsearchService(database)
    data = service.inspect_index(index_name)
    service.close()
    
    if "error" in data:
         print(f"Error inspecting index: {data['error']}")
         return RedirectResponse(url=f"/databases/{db_id}/elasticsearch")

    return templates.TemplateResponse("databases/elasticsearch/inspect.html", {
        "request": request,
        "user": user,
        "database": database,
        "index_name": index_name,
        "index_info": data.get("index_info"),
        "mappings": data.get("mappings"),
        "settings": data.get("settings")
    })

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
    
    service = ElasticsearchService(database)
    result = service.search_index(index_name, query, size)
    service.close()
    
    if isinstance(result, dict) and "error" in result:
         return HTMLResponse(f'<pre class="text-red-400">Error: {result["error"]}</pre>')
         
    hits = result
    if not hits:
        return HTMLResponse('<div class="text-slate-500 text-sm">No documents found</div>')
    
    # Format results as cards (keeping API view logic here for now)
    html = ''
    for hit in hits:
        doc_id = hit['_id']
        source = hit['_source']
        
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
    
    service = ElasticsearchService(database)
    
    mappings_obj = None
    if mappings and mappings.strip():
        try:
            mappings_obj = json.loads(mappings)
        except json.JSONDecodeError:
            return HTMLResponse("Invalid mappings JSON", status_code=400)
            
    settings_obj = None
    if settings and settings.strip():
        try:
            settings_obj = json.loads(settings)
        except json.JSONDecodeError:
            return HTMLResponse("Invalid settings JSON", status_code=400)

    success, msg = service.create_index(index_name, shards, replicas, mappings_obj, settings_obj)
    service.close()
    
    if success:
        return HTMLResponse("OK")
    else:
        return HTMLResponse(f"Error: {msg}", status_code=500)

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
    
    service = ElasticsearchService(database)
    success, msg = service.delete_index(index_name)
    service.close()
    
    if success:
        return HTMLResponse("OK")
    else:
        return HTMLResponse(f"Error: {msg}", status_code=500)

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
    
    try:
        body = json.loads(doc_body)
    except json.JSONDecodeError:
        return HTMLResponse("Invalid JSON", status_code=400)

    service = ElasticsearchService(database)
    success, msg = service.create_document(index_name, body, doc_id)
    service.close()
    
    if success:
        return HTMLResponse("OK")
    else:
        return HTMLResponse(f"Error: {msg}", status_code=500)

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
    
    try:
        body = json.loads(doc_body)
    except json.JSONDecodeError:
        return HTMLResponse("Invalid JSON", status_code=400)
        
    service = ElasticsearchService(database)
    success, msg = service.update_document(index_name, doc_id, body)
    service.close()
    
    if success:
        return HTMLResponse("OK")
    else:
        return HTMLResponse(f"Error: {msg}", status_code=500)

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
    
    service = ElasticsearchService(database)
    success, msg = service.delete_document(index_name, doc_id)
    service.close()
    
    if success:
        return HTMLResponse("OK")
    else:
        return HTMLResponse(f"Error: {msg}", status_code=500)

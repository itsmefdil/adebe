from fastapi import APIRouter, Request, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from app.dependencies import templates, get_current_user
from app.database import get_db, ConnectionManager
from app.services.mongo_service import MongoService
import json

router = APIRouter(prefix="/databases", tags=["mongodb"])

@router.get("/{db_id}/mongodb", response_class=HTMLResponse)
async def mongodb_dashboard(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        service = MongoService(database)
        stats = service.get_dashboard_stats()
        
        # Choose template based on MongoDB user privileges (not app user role)
        is_mongo_admin = stats.get("is_mongo_admin", False)
        template_name = "databases/mongodb/dashboard_admin.html" if is_mongo_admin else "databases/mongodb/dashboard_user.html"
        
        print(f"[DEBUG] MongoDB Admin: {is_mongo_admin}, Template: {template_name}")
        
        return templates.TemplateResponse(template_name, {
            "request": request, 
            "user": user,
            "database": database,
            **stats
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.get("/{db_id}/mongodb/collections/{collection_name}/browse", response_class=HTMLResponse)
async def mongodb_collection_browse(
    request: Request, 
    db_id: int, 
    collection_name: str,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    filter: str = Query(None),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        service = MongoService(database)
        
        filter_query = {}
        if filter:
            try:
                filter_query = json.loads(filter)
            except json.JSONDecodeError:
                pass # Or handle error gracefully
                
        result = service.browse_collection(collection_name, page, limit, filter_query)
        
        return templates.TemplateResponse("databases/mongodb/browse.html", {
            "request": request,
            "user": user,
            "database": database,
            "collection_name": collection_name,
            "filter": filter,
            **result
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.get("/{db_id}/mongodb/collections/{collection_name}/insert", response_class=HTMLResponse)
async def mongodb_insert_document_page(request: Request, db_id: int, collection_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    return templates.TemplateResponse("databases/mongodb/insert.html", {
        "request": request,
        "user": user,
        "database": database,
        "collection_name": collection_name
    })

@router.post("/{db_id}/mongodb/collections/{collection_name}/insert")
async def mongodb_insert_document(request: Request, db_id: int, collection_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        form = await request.form()
        document_json = form.get("document")
        
        try:
            document_data = json.loads(document_json)
        except json.JSONDecodeError:
            return templates.TemplateResponse("databases/mongodb/insert.html", {
                "request": request,
                "user": user,
                "database": database,
                "collection_name": collection_name,
                "error": "Invalid JSON format",
                "document": document_json
            })
            
        service = MongoService(database)
        service.insert_document(collection_name, document_data)
        
        return RedirectResponse(
            url=f"/databases/{db_id}/mongodb/collections/{collection_name}/browse", 
            status_code=303
        )
    except Exception as e:
        return templates.TemplateResponse("databases/mongodb/insert.html", {
            "request": request,
            "user": user,
            "database": database,
            "collection_name": collection_name,
            "error": str(e),
            "document": document_json
        })

@router.get("/{db_id}/mongodb/collections/{collection_name}/documents/{doc_id}/edit", response_class=HTMLResponse)
async def mongodb_edit_document_page(request: Request, db_id: int, collection_name: str, doc_id: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        service = MongoService(database)
        document = service.get_document(collection_name, doc_id)
        
        if not document:
            return HTMLResponse("Document not found")
            
        return templates.TemplateResponse("databases/mongodb/edit.html", {
            "request": request,
            "user": user,
            "database": database,
            "collection_name": collection_name,
            "doc_id": doc_id,
            "document": json.dumps(document, indent=2)
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.post("/{db_id}/mongodb/collections/{collection_name}/documents/{doc_id}/edit")
async def mongodb_update_document(request: Request, db_id: int, collection_name: str, doc_id: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        form = await request.form()
        document_json = form.get("document")
        
        try:
            document_data = json.loads(document_json)
        except json.JSONDecodeError:
            return templates.TemplateResponse("databases/mongodb/edit.html", {
                "request": request,
                "user": user,
                "database": database,
                "collection_name": collection_name,
                "doc_id": doc_id,
                "error": "Invalid JSON format",
                "document": document_json
            })
            
        service = MongoService(database)
        service.update_document(collection_name, doc_id, document_data)
        
        return RedirectResponse(
            url=f"/databases/{db_id}/mongodb/collections/{collection_name}/browse", 
            status_code=303
        )
    except Exception as e:
        return templates.TemplateResponse("databases/mongodb/edit.html", {
            "request": request,
            "user": user,
            "database": database,
            "collection_name": collection_name,
            "doc_id": doc_id,
            "error": str(e),
            "document": document_json
        })

@router.delete("/{db_id}/mongodb/collections/{collection_name}/documents/{doc_id}")
async def mongodb_delete_document(request: Request, db_id: int, collection_name: str, doc_id: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return {"error": "Invalid database type"}
        
    try:
        service = MongoService(database)
        result = service.delete_document(collection_name, doc_id)
        
        if result.deleted_count > 0:
            return {"success": True}
        else:
            return {"success": False, "error": "Document not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.get("/{db_id}/mongodb/create-collection", response_class=HTMLResponse)
async def mongodb_create_collection_page(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    return templates.TemplateResponse("databases/mongodb/create_collection.html", {
        "request": request,
        "user": user,
        "database": database
    })

@router.post("/{db_id}/mongodb/create-collection")
async def mongodb_create_collection(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        form = await request.form()
        collection_name = form.get("collection_name")
        
        if not collection_name:
             return templates.TemplateResponse("databases/mongodb/create_collection.html", {
                "request": request,
                "user": user,
                "database": database,
                "error": "Collection name is required"
            })

        service = MongoService(database)
        service.create_collection(collection_name)
        
        return RedirectResponse(
            url=f"/databases/{db_id}/mongodb", 
            status_code=303
        )
    except Exception as e:
        return templates.TemplateResponse("databases/mongodb/create_collection.html", {
            "request": request,
            "user": user,
            "database": database,
            "error": str(e)
        })

@router.delete("/{db_id}/mongodb/collections/{collection_name}")
async def mongodb_drop_collection(request: Request, db_id: int, collection_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return {"error": "Invalid database type"}
        
    try:
        service = MongoService(database)
        service.drop_collection(collection_name)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.post("/{db_id}/mongodb/command")
async def mongodb_run_command(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return {"error": "Invalid database type"}
        
    try:
        body = await request.json()
        command = body.get("command")
        
        if not command:
            return {"success": False, "error": "Command is required"}
            
        service = MongoService(database)
        result = service.run_command(command)
        
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

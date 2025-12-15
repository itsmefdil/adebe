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

@router.post("/{db_id}/mongodb/collections/{collection_name}/documents/bulk-delete")
async def mongodb_bulk_delete_documents(request: Request, db_id: int, collection_name: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return {"error": "Invalid database type"}
        
    try:
        body = await request.json()
        doc_ids = body.get("doc_ids", [])
        
        if not doc_ids:
            return {"success": False, "error": "No document IDs provided"}
            
        service = MongoService(database)
        result = service.delete_documents(collection_name, doc_ids)
        
        return {
            "success": True, 
            "deleted_count": result.deleted_count
        }
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

@router.get("/{db_id}/mongodb/users", response_class=HTMLResponse)
async def mongodb_users_list(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        service = MongoService(database)
        users, is_partial = service.get_users()
        
        return templates.TemplateResponse("databases/mongodb/users.html", {
            "request": request,
            "user": user,
            "database": database,
            "users": users,
            "is_partial": is_partial
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.get("/{db_id}/mongodb/users/create", response_class=HTMLResponse)
async def mongodb_create_user_page(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    return templates.TemplateResponse("databases/mongodb/create_user.html", {
        "request": request,
        "user": user,
        "database": database
    })

def parse_roles(roles_str):
    if not roles_str:
        return []
    
    roles = []
    # Split by comma
    parts = [p.strip() for p in roles_str.split(",") if p.strip()]
    
    for part in parts:
        if "@" in part:
            r_name, r_db = part.split("@", 1)
            roles.append({"role": r_name.strip(), "db": r_db.strip()})
        else:
            roles.append(part)
    return roles

@router.post("/{db_id}/mongodb/users/create")
async def mongodb_create_user(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        roles_str = form.get("roles") # Expecting comma-separated string
        auth_db = form.get("auth_db") or database.database_name # Default to current DB
        
        if not username or not password:
             return templates.TemplateResponse("databases/mongodb/create_user.html", {
                "request": request,
                "user": user,
                "database": database,
                "error": "Username and Password are required"
            })
            
        roles = parse_roles(roles_str)
        
        service = MongoService(database)
        service.create_user(username, password, roles, auth_db)
        
        # If created in another DB, we might want to redirect there?
        # For now, stay here. Users created elsewhere won't appear in list unless we change query.
        return RedirectResponse(
            url=f"/databases/{db_id}/mongodb/users", 
            status_code=303
        )
    except Exception as e:
        return templates.TemplateResponse("databases/mongodb/create_user.html", {
            "request": request,
            "user": user,
            "database": database,
            "error": str(e)
        })

@router.delete("/{db_id}/mongodb/users/{username}")
async def mongodb_delete_user(request: Request, db_id: int, username: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return {"error": "Invalid database type"}
        
    try:
        # Check if auth_db is provided in query params (optional future enhancement)
        # For now, delete from current DB context implies standard deletion
        service = MongoService(database)
        service.delete_user(username)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.get("/{db_id}/mongodb/users/{username}/edit", response_class=HTMLResponse)
async def mongodb_edit_user_page(request: Request, db_id: int, username: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
    
    try:
        service = MongoService(database)
        users, _ = service.get_users()
        target_user = next((u for u in users if u["user"] == username), None)
        
        if not target_user:
            return HTMLResponse("User not found")
            
        # Transform roles to comma-separated string for simpler editing
        if "roles" in target_user:
            role_names = []
            for role in target_user["roles"]:
                if isinstance(role, dict):
                    # Format as role@db
                    role_names.append(f"{role.get('role')}@{role.get('db')}")
                else:
                    role_names.append(str(role))
            target_user["roles_str"] = ", ".join(role_names)
        else:
             target_user["roles_str"] = ""

        return templates.TemplateResponse("databases/mongodb/edit_user.html", {
            "request": request,
            "user": user,
            "database": database,
            "target_user": target_user
        })
    except Exception as e:
        return HTMLResponse(f"Error: {str(e)}")

@router.post("/{db_id}/mongodb/users/{username}/edit")
async def mongodb_update_user(request: Request, db_id: int, username: str, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    
    manager = ConnectionManager(db)
    database = manager.get_connection(db_id)
    
    if not database or database.type != "MongoDB":
        return RedirectResponse(url="/databases")
        
    try:
        form = await request.form()
        password = form.get("password")
        roles_str = form.get("roles")
        
        roles = None
        if roles_str is not None:
             roles = parse_roles(roles_str)
        
        service = MongoService(database)
        service.update_user(username, password if password else None, roles)
        
        return RedirectResponse(
            url=f"/databases/{db_id}/mongodb/users", 
            status_code=303
        )
    except Exception as e:
        # Fetch user again to re-render form with error
        try:
             service = MongoService(database)
             users, _ = service.get_users()
             target_user = next((u for u in users if u["user"] == username), {})
             if "roles" in target_user:
                role_names = []
                for role in target_user["roles"]:
                    if isinstance(role, dict):
                        role_names.append(f"{role.get('role')}@{role.get('db')}")
                    else:
                        role_names.append(str(role))
                target_user["roles_str"] = ", ".join(role_names)
        except:
            target_user = {"user": username, "roles_str": ""}
            
        return templates.TemplateResponse("databases/mongodb/edit_user.html", {
            "request": request,
            "user": user,
            "database": database,
            "target_user": target_user,
            "error": str(e)
        })

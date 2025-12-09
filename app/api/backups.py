from fastapi import APIRouter, Request, Depends, Form, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from app.database import get_db, ConnectionManager
from app.dependencies import get_current_user
from app.worker.tasks import backup_database, restore_database, export_table_task, import_table_task
from app.core.celery_app import celery_app
from app.core.storage import get_storage_backend

router = APIRouter(prefix="/backups")

@router.post("/database/{db_id}/backup")
def trigger_backup(request: Request, db_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
        
    task = backup_database.delay(db_id)
    return JSONResponse({"task_id": task.id, "status": "processing"})

@router.get("/database/{db_id}/list")
def list_backups(request: Request, db_id: int):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
        
    storage = get_storage_backend()
    files = storage.list_backups()
    
    # Filter files related to this DB if possible, or usually backups are global or prefixed.
    # Our naming convention: {type}_{name}_{timestamp}.sql
    # We might need to fetch DB name to filter.
    # For now, return all or implement better filtering.
    return JSONResponse({"files": files})

@router.post("/database/{db_id}/restore")
def trigger_restore(request: Request, db_id: int, filename: str = Form(...), db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
        
    task = restore_database.delay(db_id, filename)
    return JSONResponse({"task_id": task.id, "status": "processing"})

@router.post("/database/{db_id}/tables/{table_name}/export")
def trigger_export(
    request: Request, 
    db_id: int, 
    table_name: str, 
    format: str = Form("csv"),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
        
    task = export_table_task.delay(db_id, table_name, format)
    return JSONResponse({"task_id": task.id, "status": "processing"})

@router.post("/database/{db_id}/tables/{table_name}/import")
async def trigger_import(
    request: Request, 
    db_id: int, 
    table_name: str, 
    file: UploadFile = File(...),
    format: str = Form("csv"),
    db: Session = Depends(get_db)
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    # Upload file to storage first (synchronously or via utility)
    # Since import task needs to access it.
    storage = get_storage_backend()
    
    # We need to save the UploadFile to a temp location first then upload to storage
    import tempfile
    import os
    import shutil
    
    from datetime import datetime
    timestamp = datetime.now()
    file_identifier = f"import_{db_id}_{table_name}_{timestamp.strftime('%Y%m%d%H%M%S')}.{format}"
    
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
        
    try:
        storage.upload(tmp_path, file_identifier)
    finally:
        os.remove(tmp_path)
        
    task = import_table_task.delay(db_id, table_name, file_identifier, format)
    return JSONResponse({"task_id": task.id, "status": "processing"})

@router.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    task_result = celery_app.AsyncResult(task_id)
    return JSONResponse({
        "task_id": task_id,
        "status": task_result.status,
        "result": task_result.result if task_result.ready() else None
    })

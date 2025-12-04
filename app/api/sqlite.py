from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.dependencies import templates, get_current_user

router = APIRouter(prefix="/databases", tags=["sqlite"])

@router.get("/sqlite", response_class=HTMLResponse)
async def sqlite_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/sqlite/dashboard.html", {"request": request, "user": user})

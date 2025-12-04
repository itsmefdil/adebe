from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.dependencies import templates, get_current_user

router = APIRouter(prefix="/databases", tags=["mongodb"])

@router.get("/mongodb", response_class=HTMLResponse)
async def mongodb_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("databases/mongodb/dashboard.html", {"request": request, "user": user})

from fastapi import Request, status
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")

async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler to catch unhandled exceptions and return a user-friendly error page.
    """
    print(f"Global error: {exc}")
    
    # Check if request expects JSON or is an API call
    if request.headers.get("accept") == "application/json" or request.url.path.startswith(("/api", "/backups")):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal Server Error", "error": str(exc)}
        )
    
    return templates.TemplateResponse("error.html", {
        "request": request,
        "error_code": 500,
        "error_message": "An unexpected error occurred.",
        "error_detail": str(exc)
    }, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

async def not_found_handler(request: Request, exc):
    if request.headers.get("accept") == "application/json":
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": "Not Found"}
        )
        
    return templates.TemplateResponse("error.html", {
        "request": request,
        "error_code": 404,
        "error_message": "Page not found.",
        "error_detail": f"The page '{request.url.path}' does not exist."
    }, status_code=status.HTTP_404_NOT_FOUND)

from fastapi import Request
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")

def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        return None
    return user

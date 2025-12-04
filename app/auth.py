import yaml
from pathlib import Path
from typing import Optional

USERS_FILE = Path(__file__).parent / "data" / "users.yaml"

def load_users():
    if not USERS_FILE.exists():
        return []
    with open(USERS_FILE, "r") as f:
        data = yaml.safe_load(f)
        return data.get("users", [])

def authenticate_user(username: str, password: str) -> Optional[dict]:
    users = load_users()
    for user in users:
        if user["username"] == username and user["password"] == password:
            return user
    return None

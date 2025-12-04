from cryptography.fernet import Fernet
import os
from dotenv import load_dotenv

load_dotenv()

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

if not ENCRYPTION_KEY:
    raise ValueError("ENCRYPTION_KEY environment variable is not set")

cipher_suite = Fernet(ENCRYPTION_KEY)

def encrypt_password(password: str) -> str:
    """Encrypts a password using Fernet symmetric encryption."""
    if not password:
        return None
    return cipher_suite.encrypt(password.encode()).decode()

def decrypt_password(encrypted_password: str) -> str:
    """Decrypts a password using Fernet symmetric encryption."""
    if not encrypted_password:
        return None
    try:
        return cipher_suite.decrypt(encrypted_password.encode()).decode()
    except Exception:
        # Return original if decryption fails (backward compatibility for unencrypted passwords)
        return encrypted_password

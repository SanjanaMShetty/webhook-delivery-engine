"""Encryption helpers for endpoint signing secrets."""
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()

def _fernet() -> Fernet:
    key = os.getenv("WEBHOOK_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("WEBHOOK_ENCRYPTION_KEY is required to use webhook signing")
    return Fernet(key.encode("ascii"))

def encrypt_secret(secret: str) -> str:
    return _fernet().encrypt(secret.encode("utf-8")).decode("ascii")

def decrypt_secret(ciphertext: str) -> str:
    return _fernet().decrypt(ciphertext.encode("ascii")).decode("utf-8")

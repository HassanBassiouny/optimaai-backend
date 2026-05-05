"""
app/services/auth_service.py
Password hashing (bcrypt) + JWT tokens for OptimaAi auth.

v2 — HTTPBearer scheme so Swagger's Authorize button takes a token directly
     instead of the useless OAuth2 username/password form.
"""
import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from app.database import get_db, User

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────
SECRET_KEY  = os.getenv("JWT_SECRET_KEY", "change-this-in-production-please")
ALGORITHM   = "HS256"
TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))

pwd_context    = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme  = HTTPBearer(auto_error=False)


# ── Password helpers ──────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        return False


# ── JWT helpers ───────────────────────────────────────────────────────────
def create_access_token(user_id: int, email: str, role: str,
                        expires_minutes: int = TOKEN_EXPIRE_MINUTES) -> str:
    payload = {
        "sub":   str(user_id),
        "email": email,
        "role":  role,
        "exp":   datetime.utcnow() + timedelta(minutes=expires_minutes),
        "iat":   datetime.utcnow(),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


# ── FastAPI dependencies ──────────────────────────────────────────────────
def get_current_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    db: Session = Depends(get_db),
) -> User:
    """Protects endpoints. Raises 401 if no/bad token."""
    if not creds:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    payload = decode_access_token(creds.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    user = db.query(User).filter(User.id == int(payload["sub"])).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user


def get_current_user_optional(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    db: Session = Depends(get_db),
) -> Optional[User]:
    """For endpoints that attribute data if logged in but don't require it."""
    if not creds:
        return None
    payload = decode_access_token(creds.credentials)
    if not payload:
        return None
    return db.query(User).filter(User.id == int(payload["sub"])).first()


def serialize_user(u: User) -> dict:
    """Match the exact shape the Next.js frontend expects in types/index.ts."""
    return {
        "id":            str(u.id),
        "email":         u.email,
        "name":          u.name,
        "avatarUrl":     u.avatar_url or "",
        "role": {
            "id":          f"role-{u.role}",
            "name":        u.role,
            "permissions": [],
        },
        "departmentId":  u.department_id,
        "createdAt":     u.created_at.isoformat() if u.created_at else None,
        "updatedAt":     u.updated_at.isoformat() if u.updated_at else None,
    }
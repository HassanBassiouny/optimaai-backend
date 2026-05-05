"""
app/routes/auth_routes.py

Endpoints that match your Next.js AuthProvider exactly:

  POST /api/v1/auth/register  →  {data: {token, user}, message}
  POST /api/v1/auth/login     →  {data: {token, user}, message}
  GET  /api/v1/auth/me        →  {data: user}
"""
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from app.database import get_db, User
from app.services.auth_service import (
    hash_password, verify_password,
    create_access_token, get_current_user, serialize_user,
)

router = APIRouter(prefix="/api/v1/auth", tags=["Auth"])


# ── Schemas ────────────────────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    fullName: str = Field(..., min_length=2, max_length=255)
    email:    EmailStr
    password: str = Field(..., min_length=8, max_length=128)

class LoginRequest(BaseModel):
    email:    EmailStr
    password: str


# ── Routes ─────────────────────────────────────────────────────────────────
@router.post("/register", status_code=status.HTTP_201_CREATED)
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == req.email.lower()).first()
    if existing:
        raise HTTPException(status_code=409, detail="An account with this email already exists.")

    user = User(
        email           = req.email.lower(),
        name            = req.fullName,
        hashed_password = hash_password(req.password),
        role            = "viewer",
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(user.id, user.email, user.role)
    return {
        "data":    {"token": token, "user": serialize_user(user)},
        "message": "Account created successfully",
    }


@router.post("/login")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == req.email.lower()).first()
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is disabled")

    token = create_access_token(user.id, user.email, user.role)
    return {
        "data":    {"token": token, "user": serialize_user(user)},
        "message": "Login successful",
    }


@router.get("/me")
def me(current_user: User = Depends(get_current_user)):
    return {"data": serialize_user(current_user)}


@router.post("/logout")
def logout():
    """Stateless JWT → client just drops the token. Endpoint exists for symmetry."""
    return {"message": "Logged out"}

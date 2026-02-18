"""
Authentication API endpoints.

Provides user registration, login, token management, and profile endpoints.
"""

from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel, Field, EmailStr
from typing import Optional

from app.core.database import get_db
from app.users.auth import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


# Request/Response Models


class RegisterRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., min_length=8, description="Password (min 8 characters)")
    name: Optional[str] = Field(None, description="User's full name")


class LoginRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., description="User password")


class UpdateProfileRequest(BaseModel):
    name: Optional[str] = Field(None, description="User's full name")


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(..., description="Current password")
    new_password: str = Field(
        ..., min_length=8, description="New password (min 8 characters)"
    )


class PasswordResetRequest(BaseModel):
    email: EmailStr = Field(..., description="Email address for password reset")


class PasswordResetConfirm(BaseModel):
    token: str = Field(..., description="Password reset token")
    new_password: str = Field(..., min_length=8, description="New password")


class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., description="Refresh token")


# Helper to extract token from header


def get_current_user(authorization: Optional[str] = Header(None)):
    """Extract and validate JWT token from Authorization header."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")

    token = authorization[7:]  # Remove "Bearer " prefix

    db = next(get_db())
    try:
        auth_service = AuthService(db)
        user_info = auth_service.verify_token(token)
        return user_info
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    finally:
        db.close()


# Endpoints


@router.post("/register")
def register(request: RegisterRequest):
    """Register a new user account."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        result = auth_service.register(
            email=request.email, password=request.password, name=request.name
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.post("/login")
def login(request: LoginRequest):
    """Login and receive access token."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        result = auth_service.login(email=request.email, password=request.password)
        return result
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    finally:
        db.close()


@router.post("/logout")
def logout(current_user: dict = Depends(get_current_user)):
    """Logout and invalidate refresh tokens."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        auth_service.logout(current_user["user_id"])
        return {"message": "Successfully logged out"}
    finally:
        db.close()


@router.post("/refresh")
def refresh_token(request: RefreshTokenRequest):
    """Refresh access token using refresh token."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        result = auth_service.refresh_token(request.refresh_token)
        return result
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    finally:
        db.close()


@router.get("/me")
def get_current_user_profile(current_user: dict = Depends(get_current_user)):
    """Get current user's profile."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        user = auth_service.get_user(current_user["user_id"])
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user
    finally:
        db.close()


@router.patch("/me")
def update_profile(
    request: UpdateProfileRequest, current_user: dict = Depends(get_current_user)
):
    """Update current user's profile."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        updates = {}
        if request.name is not None:
            updates["name"] = request.name

        user = auth_service.update_user(current_user["user_id"], updates)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user
    finally:
        db.close()


@router.post("/password/change")
def change_password(
    request: ChangePasswordRequest, current_user: dict = Depends(get_current_user)
):
    """Change current user's password."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        auth_service.change_password(
            user_id=current_user["user_id"],
            old_password=request.old_password,
            new_password=request.new_password,
        )
        return {"message": "Password changed successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.post("/password/reset-request")
def request_password_reset(request: PasswordResetRequest):
    """Request a password reset token."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        token = auth_service.request_password_reset(request.email)

        # Always return success to not reveal if email exists
        # Token is logged server-side only; in production, send via email
        return {"message": "If the email exists, a reset link has been sent"}
    finally:
        db.close()


@router.post("/password/reset")
def reset_password(request: PasswordResetConfirm):
    """Reset password using token."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        auth_service.reset_password(
            token=request.token, new_password=request.new_password
        )
        return {"message": "Password reset successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()

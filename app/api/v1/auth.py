"""
Authentication API endpoints.

Provides user registration, login, token management, and profile endpoints.
"""

import logging

from fastapi import APIRouter, HTTPException, Header, Depends, Request
from pydantic import BaseModel, Field, EmailStr
from typing import Optional

from app.core.database import get_db
from app.users.auth import AuthService

logger = logging.getLogger(__name__)

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


# Passwordless sign-in (PLAN_063 / SPEC_053)


class RequestCodeRequest(BaseModel):
    email: EmailStr = Field(..., description="Email to send a sign-in code/link to")
    signup_source: Optional[str] = Field(
        default="playground", description="Where the signup originated"
    )


class VerifyCodeRequest(BaseModel):
    email: EmailStr = Field(..., description="Email the code was sent to")
    code: str = Field(..., min_length=4, max_length=12, description="6-digit sign-in code")


class VerifyTokenRequest(BaseModel):
    token: str = Field(..., description="Magic-link token from the email")


# Helper to extract token from header


def get_current_user(authorization: Optional[str] = Header(None)):
    """Extract and validate a platform JWT from the Authorization header.

    Always strict, whatever REQUIRE_AUTH says: endpoints using this need a
    real user id. Playground and stream tokens are rejected (SPEC_127).
    """
    from app.core.authz import authenticate_bearer

    return authenticate_bearer(authorization)


# Endpoints


@router.post("/register")
def register(request: RegisterRequest):
    """Register a new user account.

    Closed unless ALLOW_SIGNUP=true (SPEC_127). While closed, accounts are
    created by an admin with `python scripts/create_user.py`.
    """
    from app.core.config import get_settings

    if not get_settings().allow_signup:
        raise HTTPException(
            status_code=403,
            detail="Self-registration is disabled. Ask an administrator for an account.",
        )
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


@router.post("/stream-token")
def issue_stream_token(current_user: dict = Depends(get_current_user)):
    """Issue a short-lived token for EventSource streams (SPEC_127).

    EventSource cannot send an Authorization header, so SSE routes accept
    `?stream_token=<token>` instead. The token expires after a few minutes and
    is only checked when the stream connects; request a new one per connect.
    """
    from app.users.auth import STREAM_TOKEN_EXPIRE_SECONDS

    db = next(get_db())
    try:
        token = AuthService(db).create_stream_token(
            current_user["user_id"], current_user["email"], current_user["role"]
        )
    finally:
        db.close()
    return {"stream_token": token, "expires_in": STREAM_TOKEN_EXPIRE_SECONDS}


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
        auth_service.request_password_reset(request.email)

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


# Passwordless sign-in endpoints (PLAN_063 / SPEC_053)


@router.post("/request-code")
async def request_login_code(request: RequestCodeRequest, http_request: Request):
    """Email a passwordless sign-in code + magic link. Always returns a neutral
    message — never reveals whether the email exists, never returns the code."""
    request_ip = http_request.client.host if http_request.client else None
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        result = auth_service.request_login_code(
            email=request.email,
            request_ip=request_ip,
            signup_source=request.signup_source or "playground",
        )
        from app.services.email import send_login_code

        try:
            await send_login_code(result["email"], result["code"], result["link"])
        except Exception as exc:  # noqa: BLE001 — email failure shouldn't 500 the client
            logger.error("Failed to send login code email: %s", exc)
    except ValueError as e:
        # Rate-limit rejections surface as 429; everything else stays neutral.
        raise HTTPException(status_code=429, detail=str(e))
    finally:
        db.close()
    return {"message": "If that email is valid, a sign-in code is on its way."}


@router.post("/verify-code")
async def verify_login_code(request: VerifyCodeRequest):
    """Verify a 6-digit sign-in code and return the JWT token bundle."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        return auth_service.verify_login_code(email=request.email, code=request.code)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    finally:
        db.close()


@router.post("/verify-token")
async def verify_login_token(request: VerifyTokenRequest):
    """Verify a magic-link token and return the JWT token bundle."""
    db = next(get_db())
    try:
        auth_service = AuthService(db)
        return auth_service.verify_login_token(token=request.token)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    finally:
        db.close()

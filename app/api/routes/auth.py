"""ODIN V2 Backend — Auth Routes"""

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_token,
    revoke_all_user_sessions,
    revoke_session,
    validate_refresh_token,
    get_current_user_id,
    get_current_user,
)
from app.models import User
from app.schemas import LoginRequest, LoginResponse, RefreshRequest, UserResponse

router = APIRouter()
settings = get_settings()


@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Login with email and password."""
    from app.core.security import authenticate_user

    user = await authenticate_user(db, request.email, request.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    access_token = create_access_token({"sub": user.id})
    refresh_token = create_refresh_token({"sub": user.id})

    # Store refresh token hash
    from app.models import UserSession

    session = UserSession(
        user_id=user.id,
        token_hash=hash_token(refresh_token),
        expires_at=__import__("datetime").datetime.utcnow() + timedelta(days=settings.refresh_token_expire_days),
    )
    db.add(session)
    await db.commit()

    # Set refresh token as HttpOnly cookie
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=False,  # Set True in production with HTTPS
        samesite="lax",
        max_age=settings.refresh_token_expire_days * 24 * 60 * 60,
        path="/api/auth",
    )

    return LoginResponse(
        access_token=access_token,
        expires_in=settings.access_token_expire_minutes * 60,
    )


@router.post("/refresh", response_model=LoginResponse)
async def refresh_token(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Refresh access token using refresh token."""
    # Get refresh token from cookie or body
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        body = await request.json()
        refresh_token = body.get("refresh_token")

    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token required")

    session = await validate_refresh_token(db, refresh_token)
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")

    # Rotate refresh token
    await revoke_session(db, session.id)
    new_access = create_access_token({"sub": session.user_id})
    new_refresh = create_refresh_token({"sub": session.user_id})

    new_session = UserSession(
        user_id=session.user_id,
        token_hash=hash_token(new_refresh),
        expires_at=__import__("datetime").datetime.utcnow() + timedelta(days=settings.refresh_token_expire_days),
    )
    db.add(new_session)
    await db.commit()

    response.set_cookie(
        key="refresh_token",
        value=new_refresh,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=settings.refresh_token_expire_days * 24 * 60 * 60,
        path="/api/auth",
    )

    return LoginResponse(access_token=new_access, expires_in=settings.access_token_expire_minutes * 60)


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Logout current session."""
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        await revoke_session(db, hash_token(refresh_token))

    response.delete_cookie("refresh_token", path="/api/auth")
    return {"ok": True}


@router.post("/logout-all")
async def logout_all(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Logout all sessions for current user."""
    count = await revoke_all_user_sessions(db, current_user.id)
    return {"ok": True, "revoked_sessions": count}


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """Get current user info."""
    return UserResponse.model_validate(current_user)
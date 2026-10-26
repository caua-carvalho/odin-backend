"""ODIN V2 Backend — Security & Authentication"""

import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.models import User, UserSession

settings = get_settings()

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    """Hash a password."""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.access_token_expire_minutes))
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def create_refresh_token(data: dict) -> str:
    """Create a JWT refresh token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=settings.refresh_token_expire_days)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def decode_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT token."""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        return payload
    except JWTError:
        return None


def hash_token(token: str) -> str:
    """Hash a token for storage (SHA-256)."""
    return hashlib.sha256(token.encode()).hexdigest()


def _is_stream_path(path: str) -> bool:
    """Only the SSE stream may authenticate via query string.

    ``EventSource`` cannot set an Authorization header. WebSockets use a
    separate validator in the runs route. Keeping the fallback this narrow
    avoids leaking JWTs in URLs/logs for ordinary REST calls.
    """
    return path.endswith("/events")


def get_current_user_id(request: Request) -> str:
    """Extract user ID from the Authorization header.

    ``EventSource`` cannot set custom headers, so ``?token=`` is accepted as a
    fallback on the SSE stream endpoint only.
    """
    auth_header = request.headers.get("Authorization")
    token: Optional[str] = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:]  # Remove "Bearer "
    if not token and _is_stream_path(request.url.path):
        token = request.query_params.get("token")

    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid token")

    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    return user_id


async def get_current_user(db: AsyncSession = Depends(get_db), user_id: str = Depends(get_current_user_id)) -> User:
    """Get current authenticated user."""
    result = await db.execute(select(User).where(User.id == user_id, User.is_active == True))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return user


async def authenticate_user(db: AsyncSession, email: str, password: str) -> Optional[User]:
    """Authenticate a user by email and password."""
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


async def create_user_session(
    db: AsyncSession,
    user_id: str,
    user_agent: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> tuple[str, str]:
    """
    Create a new user session.

    Returns:
        Tuple of (access_token, refresh_token)
    """
    # Create access token
    access_token = create_access_token({"sub": user_id})
    # Create refresh token
    refresh_token = create_refresh_token({"sub": user_id})

    # Store hashed refresh token
    session = UserSession(
        user_id=user_id,
        token_hash=hash_token(refresh_token),
        expires_at=datetime.utcnow() + timedelta(days=settings.refresh_token_expire_days),
        user_agent=user_agent,
        ip_address=ip_address,
    )
    db.add(session)
    await db.flush()

    # Update user last_login
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one()
    user.last_login = datetime.utcnow()

    return access_token, refresh_token


async def validate_refresh_token(db: AsyncSession, refresh_token: str) -> Optional[UserSession]:
    """Validate a refresh token and return the session."""
    token_hash = hash_token(refresh_token)
    result = await db.execute(
        select(UserSession).where(
            UserSession.token_hash == token_hash,
            UserSession.expires_at > datetime.utcnow(),
        )
    )
    return result.scalar_one_or_none()


async def revoke_session(db: AsyncSession, session_id: str) -> bool:
    """Revoke a user session."""
    result = await db.execute(select(UserSession).where(UserSession.id == session_id))
    session = result.scalar_one_or_none()
    if session:
        await db.delete(session)
        return True
    return False


async def revoke_all_user_sessions(db: AsyncSession, user_id: str) -> int:
    """Revoke all sessions for a user."""
    result = await db.execute(select(UserSession).where(UserSession.user_id == user_id))
    sessions = result.scalars().all()
    count = 0
    for session in sessions:
        await db.delete(session)
        count += 1
    return count


def generate_secure_token(length: int = 32) -> str:
    """Generate a cryptographically secure random token."""
    return secrets.token_urlsafe(length)
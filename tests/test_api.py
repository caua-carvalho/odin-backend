"""ODIN V2 Backend — API smoke tests.

These exercise the routes end-to-end against an in-memory database without
starting the application lifespan (Hermes is not required).
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.database import get_db
from app.core.security import hash_password
from app.main import app
from app.models import User

PASSWORD = "pass1234"


@pytest.fixture
async def client(test_engine):
    """HTTP client wired to the same in-memory database as the test fixtures."""
    session_maker = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client
    app.dependency_overrides.pop(get_db, None)


async def _create_user(db_session: AsyncSession, email: str) -> User:
    user = User(email=email, hashed_password=hash_password(PASSWORD))
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


async def _login(client: AsyncClient, email: str, mobile: bool = False) -> dict:
    headers = {"X-Odin-Client": "mobile"} if mobile else {}
    response = await client.post(
        "/api/auth/login",
        json={"email": email, "password": PASSWORD},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_login_me_and_refresh(client: AsyncClient, db_session: AsyncSession):
    """Login must return an access token and refresh must rotate it."""
    email = f"{uuid.uuid4().hex}@example.com"
    await _create_user(db_session, email)

    login = await _login(client, email)
    assert login.get("refresh_token") is None  # web clients use the HttpOnly cookie

    me = await client.get(
        "/api/auth/me", headers={"Authorization": f"Bearer {login['access_token']}"}
    )
    assert me.status_code == 200, me.text
    assert me.json()["email"] == email

    refreshed = await client.post("/api/auth/refresh", json={})
    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["access_token"] != login["access_token"]


@pytest.mark.asyncio
async def test_mobile_login_returns_refresh_token(client: AsyncClient, db_session: AsyncSession):
    email = f"{uuid.uuid4().hex}@example.com"
    await _create_user(db_session, email)

    login = await _login(client, email, mobile=True)
    assert login["refresh_token"]

    # Concurrent logins must not mint identical refresh tokens (unique hash index).
    second = await _login(client, email, mobile=True)
    assert second["refresh_token"] != login["refresh_token"]


@pytest.mark.asyncio
async def test_mobile_logout_revokes_body_refresh_token(
    client: AsyncClient, db_session: AsyncSession
):
    email = f"{uuid.uuid4().hex}@example.com"
    await _create_user(db_session, email)
    login = await _login(client, email, mobile=True)

    # Native clients have no cookie: clear it and send the token in the body.
    client.cookies.clear()
    logout = await client.post(
        "/api/auth/logout",
        json={"refresh_token": login["refresh_token"]},
        headers={"Authorization": f"Bearer {login['access_token']}"},
    )
    assert logout.status_code == 200, logout.text

    reused = await client.post(
        "/api/auth/refresh",
        json={"refresh_token": login["refresh_token"]},
        headers={"X-Odin-Client": "mobile"},
    )
    assert reused.status_code == 401, reused.text


@pytest.mark.asyncio
async def test_knowledge_tag_filter(client: AsyncClient, db_session: AsyncSession):
    """Tag filtering must match individual tags inside the JSON array."""
    email = f"{uuid.uuid4().hex}@example.com"
    await _create_user(db_session, email)
    login = await _login(client, email)
    headers = {"Authorization": f"Bearer {login['access_token']}"}

    created = await client.post(
        "/api/knowledge",
        json={"type": "note", "title": "Tagged", "content": "body", "tags": ["alpha", "beta"]},
        headers=headers,
    )
    assert created.status_code == 201, created.text

    matching = await client.get("/api/knowledge?tags=alpha", headers=headers)
    assert matching.status_code == 200, matching.text
    assert matching.json()["total"] == 1

    missing = await client.get("/api/knowledge?tags=gamma", headers=headers)
    assert missing.status_code == 200, missing.text
    assert missing.json()["total"] == 0

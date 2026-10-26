"""ODIN V2 Backend — Tests"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.models import (
    Approval,
    ApprovalStatus,
    Conversation,
    KnowledgeItem,
    KnowledgeType,
    Message,
    MessageRole,
    Run,
    RunStatus,
    User,
    UserSession,
)


def test_settings_load():
    """Test settings can be loaded."""
    settings = Settings(
        secret_key="test-secret-key-minimum-32-chars-long",
        database_url="sqlite+aiosqlite:///:memory:",
    )
    assert settings.secret_key == "test-secret-key-minimum-32-chars-long"
    assert settings.api_port == 8000


def test_models_import():
    """Test all models can be imported."""
    # Just verify imports work
    assert Conversation
    assert Message
    assert KnowledgeItem
    assert Run
    assert Approval
    assert User
    assert UserSession
    assert MessageRole
    assert KnowledgeType
    assert RunStatus
    assert ApprovalStatus


@pytest.mark.asyncio
async def test_database_init(db_session: AsyncSession):
    """Test database session works."""
    from sqlalchemy import text
    result = await db_session.execute(text("SELECT 1"))
    assert result.scalar() == 1


@pytest.mark.asyncio
async def test_user_model(db_session: AsyncSession):
    """Test User model CRUD."""
    from app.core.security import hash_password

    user = User(
        email="test@example.com",
        hashed_password=hash_password("pass123"),
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    assert user.id is not None
    assert user.email == "test@example.com"
    assert user.is_active is True


@pytest.mark.asyncio
async def test_conversation_model(db_session: AsyncSession):
    """Test Conversation model."""
    conv = Conversation(title="Test Conversation")
    db_session.add(conv)
    await db_session.commit()
    await db_session.refresh(conv)

    assert conv.id.startswith("conv_")
    assert conv.title == "Test Conversation"


@pytest.mark.asyncio
async def test_knowledge_model(db_session: AsyncSession):
    """Test KnowledgeItem model."""
    item = KnowledgeItem(
        type=KnowledgeType.NOTE,
        title="Test Note",
        content="This is a test note",
        tags=["test", "note"],
    )
    db_session.add(item)
    await db_session.commit()
    await db_session.refresh(item)

    assert item.id.startswith("knw_")
    assert item.type == KnowledgeType.NOTE
    assert item.tags == ["test", "note"]


@pytest.mark.asyncio
async def test_run_model(db_session: AsyncSession):
    """Test Run model."""
    conv = Conversation(title="Test")
    db_session.add(conv)
    await db_session.flush()

    run = Run(conversation_id=conv.id, status=RunStatus.QUEUED)
    db_session.add(run)
    await db_session.commit()
    await db_session.refresh(run)

    assert run.id.startswith("run_")
    assert run.status == RunStatus.QUEUED
    assert run.conversation_id == conv.id


@pytest.mark.asyncio
async def test_approval_model(db_session: AsyncSession):
    """Test Approval model."""
    conv = Conversation(title="Test")
    db_session.add(conv)
    await db_session.flush()

    run = Run(conversation_id=conv.id, status=RunStatus.RUNNING)
    db_session.add(run)
    await db_session.flush()

    approval = Approval(
        run_id=run.id,
        hermes_approval_id="hermes_apr_123",
        operation="terminal",
        details={"command": "ls -la"},
        status=ApprovalStatus.PENDING,
    )
    db_session.add(approval)
    await db_session.commit()
    await db_session.refresh(approval)

    assert approval.id.startswith("apr_")
    assert approval.status == ApprovalStatus.PENDING
    assert approval.operation == "terminal"
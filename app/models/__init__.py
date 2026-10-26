"""ODIN V2 Backend — Database Models"""

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


def gen_id(prefix: str) -> str:
    """Generate a prefixed UUID."""
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


class Conversation(Base):
    """Chat conversation."""

    __tablename__ = "conversations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("conv"))
    title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    hermes_session_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    messages: Mapped[list["Message"]] = relationship(back_populates="conversation", cascade="all, delete-orphan", lazy="selectin")
    runs: Mapped[list["Run"]] = relationship(back_populates="conversation", cascade="all, delete-orphan", lazy="selectin")

    __table_args__ = (Index("ix_conversations_updated_at", "updated_at"),)


class MessageRole(str, enum.Enum):
    """Message role in conversation."""

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class Message(Base):
    """Message in a conversation."""

    __tablename__ = "messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("msg"))
    conversation_id: Mapped[str] = mapped_column(String(36), ForeignKey("conversations.id", ondelete="CASCADE"), index=True)
    role: Mapped[MessageRole] = mapped_column(Enum(MessageRole), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    tool_calls: Mapped[Optional[list]] = mapped_column(SQLiteJSON, nullable=True)
    tool_results: Mapped[Optional[list]] = mapped_column(SQLiteJSON, nullable=True)
    run_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("runs.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    conversation: Mapped["Conversation"] = relationship(back_populates="messages")
    run: Mapped[Optional["Run"]] = relationship(back_populates="messages")

    __table_args__ = (Index("ix_messages_conversation_created", "conversation_id", "created_at"),)


class KnowledgeType(str, enum.Enum):
    """Type of knowledge item."""

    NOTE = "note"
    LINK = "link"
    SNIPPET = "snippet"
    PERSONAL = "personal"


class KnowledgeItem(Base):
    """Personal knowledge library item."""

    __tablename__ = "knowledge"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("knw"))
    type: Mapped[KnowledgeType] = mapped_column(Enum(KnowledgeType), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    tags: Mapped[list] = mapped_column(SQLiteJSON, default=list, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_knowledge_type_created", "type", "created_at"),
        Index("ix_knowledge_tags", "tags", sqlite_where=None),  # GIN-like via JSON
    )


class RunStatus(str, enum.Enum):
    """Execution run status (local Odin states)."""

    QUEUED = "queued"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class Run(Base):
    """Agent execution run."""

    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("run"))
    conversation_id: Mapped[str] = mapped_column(String(36), ForeignKey("conversations.id", ondelete="CASCADE"), index=True)
    hermes_run_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    hermes_session_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), default=RunStatus.QUEUED, nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    result_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    events: Mapped[list] = mapped_column(SQLiteJSON, default=list, nullable=False)

    # Relationships
    conversation: Mapped["Conversation"] = relationship(back_populates="runs")
    messages: Mapped[list["Message"]] = relationship(back_populates="run")
    approvals: Mapped[list["Approval"]] = relationship(back_populates="run", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_runs_conversation_status", "conversation_id", "status"),
        Index("ix_runs_started_at", "started_at"),
    )


class ApprovalStatus(str, enum.Enum):
    """Approval request status."""

    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"


class Approval(Base):
    """Pending approval request from Hermes."""

    __tablename__ = "approvals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("apr"))
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    hermes_approval_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, unique=True)
    operation: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "terminal", "file_write"
    details: Mapped[dict] = mapped_column(SQLiteJSON, default=dict, nullable=False)
    status: Mapped[ApprovalStatus] = mapped_column(Enum(ApprovalStatus), default=ApprovalStatus.PENDING, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    run: Mapped["Run"] = relationship(back_populates="approvals")

    __table_args__ = (Index("ix_approvals_status_created", "status", "created_at"),)


class User(Base):
    """Application user (for authentication)."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("usr"))
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    is_superuser: Mapped[bool] = mapped_column(default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    sessions: Mapped[list["UserSession"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class UserSession(Base):
    """User session / refresh token."""

    __tablename__ = "user_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: gen_id("ses"))
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    token_hash: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)

    # Relationships
    user: Mapped["User"] = relationship(back_populates="sessions")

    __table_args__ = (Index("ix_user_sessions_expires", "expires_at"),)
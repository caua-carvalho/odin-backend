"""ODIN V2 Backend — Pydantic Schemas"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl


# =============================================================================
# Base Schemas
# =============================================================================

class BaseSchema(BaseModel):
    """Base schema with common config."""

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        json_schema_extra={"examples": []},
    )


class TimestampMixin(BaseModel):
    """Mixin for created/updated timestamps."""

    created_at: datetime
    updated_at: Optional[datetime] = None


# =============================================================================
# Auth Schemas
# =============================================================================

class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseSchema):
    id: str
    email: EmailStr
    is_active: bool
    is_superuser: bool
    created_at: datetime
    last_login: Optional[datetime] = None


# =============================================================================
# Conversation Schemas
# =============================================================================

class ConversationCreate(BaseModel):
    title: Optional[str] = Field(None, max_length=500)
    initial_message: Optional[str] = None


class ConversationUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=500)


class ConversationResponse(BaseSchema, TimestampMixin):
    id: str
    title: Optional[str] = None
    hermes_session_id: Optional[str] = None
    message_count: int = 0


class ConversationListResponse(BaseModel):
    items: List[ConversationResponse]
    total: int
    page: int
    page_size: int


class MessageBase(BaseModel):
    role: str
    content: str
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_results: Optional[List[Dict[str, Any]]] = None


class MessageResponse(BaseSchema, TimestampMixin, MessageBase):
    id: str
    conversation_id: str
    run_id: Optional[str] = None


class MessageListResponse(BaseModel):
    items: List[MessageResponse]
    total: int
    page: int
    page_size: int


# =============================================================================
# Knowledge Schemas
# =============================================================================

class KnowledgeType(str):
    NOTE = "note"
    LINK = "link"
    SNIPPET = "snippet"
    PERSONAL = "personal"


class KnowledgeCreate(BaseModel):
    type: str = Field(..., pattern="^(note|link|snippet|personal)$")
    title: str = Field(..., min_length=1, max_length=500)
    content: str = Field(..., min_length=1)
    url: Optional[HttpUrl] = None
    tags: List[str] = Field(default_factory=list)


class KnowledgeUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=500)
    content: Optional[str] = None
    url: Optional[HttpUrl] = None
    tags: Optional[List[str]] = None


class KnowledgeResponse(BaseSchema, TimestampMixin):
    id: str
    type: str
    title: str
    content: str
    url: Optional[str] = None
    tags: List[str]


class KnowledgeListResponse(BaseModel):
    items: List[KnowledgeResponse]
    total: int
    page: int
    page_size: int


# =============================================================================
# Run Schemas
# =============================================================================

class RunStatus(str):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class RunCreate(BaseModel):
    message: str = Field(..., min_length=1)
    stream: bool = True


class RunResponse(BaseSchema):
    id: str
    conversation_id: str
    hermes_run_id: Optional[str] = None
    hermes_session_id: Optional[str] = None
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    result_summary: Optional[str] = None
    error: Optional[str] = None
    events: List[Dict[str, Any]] = Field(default_factory=list)
    queue_position: int = 0


class RunEvent(BaseModel):
    """Server-sent event from a run."""

    event: str
    data: Dict[str, Any]
    timestamp: datetime


# =============================================================================
# Approval Schemas
# =============================================================================

class ApprovalStatus(str):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"


class ApprovalResponse(BaseSchema):
    id: str
    run_id: str
    hermes_approval_id: Optional[str] = None
    operation: str
    details: Dict[str, Any]
    status: str
    created_at: datetime
    resolved_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None


class ApprovalListResponse(BaseModel):
    items: List[ApprovalResponse]


class ApprovalActionRequest(BaseModel):
    pass  # No body needed for approve/deny


class ApprovalActionResponse(BaseModel):
    ok: bool
    approval_id: str
    status: str


# =============================================================================
# Health / System Schemas
# =============================================================================

class HealthResponse(BaseModel):
    ok: bool
    version: str
    timestamp: datetime


class SystemStatusComponent(BaseModel):
    status: str  # ok, degraded, unknown
    details: Optional[Dict[str, Any]] = None


class SystemStatusResponse(BaseModel):
    version: str
    overall: str  # ok, degraded
    components: Dict[str, SystemStatusComponent]
    hermes_connected: bool
    database_connected: bool
    timestamp: datetime


# =============================================================================
# Error Schemas
# =============================================================================

class ErrorDetail(BaseModel):
    field: Optional[str] = None
    message: str
    code: Optional[str] = None


class ErrorResponse(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: Optional[str] = None
    errors: Optional[List[ErrorDetail]] = None
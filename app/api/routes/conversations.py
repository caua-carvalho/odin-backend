"""ODIN V2 Backend — Conversation Routes"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.models import Conversation, Message
from app.schemas import (
    ConversationCreate,
    ConversationListResponse,
    ConversationResponse,
    ConversationUpdate,
    MessageListResponse,
    MessageResponse,
)

router = APIRouter()


async def get_user_conversation(
    db: AsyncSession,
    conversation_id: str,
    user_id: str,
) -> Conversation:
    """Get a conversation owned by the user."""
    result = await db.execute(
        select(Conversation).where(Conversation.id == conversation_id)
    )
    conversation = result.scalar_one_or_none()
    if not conversation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    return conversation


@router.get("", response_model=ConversationListResponse)
async def list_conversations(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """List user's conversations."""
    # Count total
    count_result = await db.execute(select(func.count(Conversation.id)))
    total = count_result.scalar()

    # Get paginated conversations with message counts
    offset = (page - 1) * page_size
    result = await db.execute(
        select(Conversation)
        .order_by(desc(Conversation.updated_at))
        .offset(offset)
        .limit(page_size)
    )
    conversations = result.scalars().all()

    # Get message counts for each conversation
    conv_ids = [c.id for c in conversations]
    msg_counts = {}
    if conv_ids:
        msg_result = await db.execute(
            select(Message.conversation_id, func.count(Message.id))
            .where(Message.conversation_id.in_(conv_ids))
            .group_by(Message.conversation_id)
        )
        msg_counts = dict(msg_result.all())

    items = [
        ConversationResponse(
            id=c.id,
            title=c.title,
            hermes_session_id=c.hermes_session_id,
            created_at=c.created_at,
            updated_at=c.updated_at,
            message_count=msg_counts.get(c.id, 0),
        )
        for c in conversations
    ]

    return ConversationListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED)
async def create_conversation(
    data: ConversationCreate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Create a new conversation."""
    conversation = Conversation(title=data.title)
    db.add(conversation)
    await db.flush()

    if data.initial_message:
        # Add initial user message
        message = Message(
            conversation_id=conversation.id,
            role="user",
            content=data.initial_message,
        )
        db.add(message)

    await db.commit()
    await db.refresh(conversation)

    return ConversationResponse(
        id=conversation.id,
        title=conversation.title,
        hermes_session_id=conversation.hermes_session_id,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
        message_count=1 if data.initial_message else 0,
    )


@router.get("/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Get a conversation by ID."""
    conversation = await get_user_conversation(db, conversation_id, user_id)

    # Get message count
    count_result = await db.execute(
        select(func.count(Message.id)).where(Message.conversation_id == conversation_id)
    )
    message_count = count_result.scalar()

    return ConversationResponse(
        id=conversation.id,
        title=conversation.title,
        hermes_session_id=conversation.hermes_session_id,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
        message_count=message_count,
    )


@router.patch("/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
    conversation_id: str,
    data: ConversationUpdate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Update a conversation."""
    conversation = await get_user_conversation(db, conversation_id, user_id)

    if data.title is not None:
        conversation.title = data.title
        conversation.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(conversation)

    return ConversationResponse(
        id=conversation.id,
        title=conversation.title,
        hermes_session_id=conversation.hermes_session_id,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
        message_count=0,  # Will be calculated if needed
    )


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Delete a conversation."""
    conversation = await get_user_conversation(db, conversation_id, user_id)
    await db.delete(conversation)
    await db.commit()


@router.get("/{conversation_id}/messages", response_model=MessageListResponse)
async def list_messages(
    conversation_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """List messages in a conversation."""
    await get_user_conversation(db, conversation_id, user_id)

    count_result = await db.execute(
        select(func.count(Message.id)).where(Message.conversation_id == conversation_id)
    )
    total = count_result.scalar()

    offset = (page - 1) * page_size
    result = await db.execute(
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
        .offset(offset)
        .limit(page_size)
    )
    messages = result.scalars().all()

    items = [
        MessageResponse(
            id=m.id,
            conversation_id=m.conversation_id,
            role=m.role.value,
            content=m.content,
            tool_calls=m.tool_calls,
            tool_results=m.tool_results,
            run_id=m.run_id,
            created_at=m.created_at,
        )
        for m in messages
    ]

    return MessageListResponse(items=items, total=total, page=page, page_size=page_size)
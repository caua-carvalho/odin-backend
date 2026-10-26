"""ODIN V2 Backend — Knowledge Library Routes"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.models import KnowledgeItem, KnowledgeType
from app.schemas import KnowledgeCreate, KnowledgeListResponse, KnowledgeResponse, KnowledgeUpdate

router = APIRouter()


@router.get("", response_model=KnowledgeListResponse)
async def list_knowledge(
    type: Optional[str] = Query(None, description="Filter by type"),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    q: Optional[str] = Query(None, description="Full-text search query"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """List knowledge items with filters."""
    query = select(KnowledgeItem)

    if type:
        query = query.where(KnowledgeItem.type == KnowledgeType(type))

    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        if tag_list:
            # JSON contains any of the tags
            for tag in tag_list:
                query = query.where(KnowledgeItem.tags.contains([tag]))

    if q:
        # Simple text search in title and content
        search_term = f"%{q}%"
        query = query.where(
            or_(
                KnowledgeItem.title.ilike(search_term),
                KnowledgeItem.content.ilike(search_term),
            )
        )

    # Count total
    count_result = await db.execute(select(func.count()).select_from(query.subquery()))
    total = count_result.scalar()

    # Paginate
    offset = (page - 1) * page_size
    result = await db.execute(
        query.order_by(desc(KnowledgeItem.updated_at)).offset(offset).limit(page_size)
    )
    items_db = result.scalars().all()

    items = [KnowledgeResponse.model_validate(item) for item in items_db]
    return KnowledgeListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("", response_model=KnowledgeResponse, status_code=status.HTTP_201_CREATED)
async def create_knowledge(
    data: KnowledgeCreate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Create a knowledge item."""
    item = KnowledgeItem(
        type=KnowledgeType(data.type),
        title=data.title,
        content=data.content,
        url=str(data.url) if data.url else None,
        tags=data.tags,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return KnowledgeResponse.model_validate(item)


@router.get("/{item_id}", response_model=KnowledgeResponse)
async def get_knowledge(
    item_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Get a knowledge item by ID."""
    result = await db.execute(select(KnowledgeItem).where(KnowledgeItem.id == item_id))
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge item not found")
    return KnowledgeResponse.model_validate(item)


@router.patch("/{item_id}", response_model=KnowledgeResponse)
async def update_knowledge(
    item_id: str,
    data: KnowledgeUpdate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Update a knowledge item."""
    result = await db.execute(select(KnowledgeItem).where(KnowledgeItem.id == item_id))
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge item not found")

    if data.title is not None:
        item.title = data.title
    if data.content is not None:
        item.content = data.content
    if data.url is not None:
        item.url = str(data.url) if data.url else None
    if data.tags is not None:
        item.tags = data.tags

    item.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(item)
    return KnowledgeResponse.model_validate(item)


@router.delete("/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_knowledge(
    item_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Delete a knowledge item."""
    result = await db.execute(select(KnowledgeItem).where(KnowledgeItem.id == item_id))
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge item not found")
    await db.delete(item)
    await db.commit()
"""ODIN V2 Backend — Approvals Routes"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.integrations.hermes.gateway import HermesError
from app.models import Approval, ApprovalStatus, Run
from app.schemas import ApprovalActionResponse, ApprovalListResponse, ApprovalResponse
from app.services import run_manager

router = APIRouter()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@router.get("", response_model=ApprovalListResponse)
async def list_approvals(
    status: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """List approvals (pending by default in the web client)."""
    query = select(Approval).join(Run).order_by(Approval.created_at.desc())

    if status:
        try:
            query = query.where(Approval.status == ApprovalStatus(status))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Status inválido: {status}") from exc
    if run_id:
        query = query.where(Approval.run_id == run_id)

    result = await db.execute(query)
    return ApprovalListResponse(items=[ApprovalResponse.model_validate(a) for a in result.scalars().all()])


@router.get("/{approval_id}", response_model=ApprovalResponse)
async def get_approval(
    approval_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Get approval details."""
    approval = await db.get(Approval, approval_id)
    if not approval:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval not found")
    return ApprovalResponse.model_validate(approval)


async def _load_pending(db: AsyncSession, approval_id: str) -> tuple[Approval, Run]:
    result = await db.execute(select(Approval).where(Approval.id == approval_id))
    approval = result.scalar_one_or_none()
    if approval is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval not found")
    if approval.status != ApprovalStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Approval already {approval.status.value}",
        )
    run = await db.get(Run, approval.run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return approval, run


async def _resolve(
    db: AsyncSession,
    approval: Approval,
    run: Run,
    choice: str,
    new_status: ApprovalStatus,
) -> ApprovalActionResponse:
    choices = (approval.details or {}).get("choices") or []
    if choices and choice not in choices:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Escolha '{choice}' inválida. Opções: {', '.join(map(str, choices))}",
        )

    try:
        await run_manager.resolve_approval(approval, run, choice=choice, approval_status=new_status)
    except HermesError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Falha ao responder ao Hermes: {exc}",
        ) from exc

    approval.status = new_status
    approval.resolved_at = _utcnow()
    await db.commit()

    return ApprovalActionResponse(ok=True, approval_id=approval.id, status=new_status.value)


@router.post("/{approval_id}/approve", response_model=ApprovalActionResponse)
async def approve_operation(
    approval_id: str,
    choice: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Approve a pending operation (defaults to the one-shot choice)."""
    approval, run = await _load_pending(db, approval_id)
    return await _resolve(db, approval, run, choice or "once", ApprovalStatus.APPROVED)


@router.post("/{approval_id}/deny", response_model=ApprovalActionResponse)
async def deny_operation(
    approval_id: str,
    choice: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Deny a pending operation."""
    approval, run = await _load_pending(db, approval_id)
    return await _resolve(db, approval, run, choice or "deny", ApprovalStatus.DENIED)

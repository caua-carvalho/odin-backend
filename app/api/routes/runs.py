"""ODIN V2 Backend — Runs Routes (Agent Executions)

Every run is created as a normal JSON resource (HTTP 202) and executed by
:mod:`app.services.run_manager` in the background. Clients then attach to the
replayable event stream over SSE (web) or WebSocket (mobile), which keeps the
HTTP request short and the transport resumable.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import StreamingResponse
from sqlalchemy import case, desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.database import async_session_maker, get_db
from app.core.security import decode_token, get_current_user_id
from app.models import Conversation, Message, MessageRole, Run, RunStatus
from app.schemas import RunCreate, RunResponse
from app.services import run_manager

logger = logging.getLogger(__name__)
router = APIRouter()

_ACTIVE_RUN_STATUSES = (RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.WAITING_APPROVAL)

SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


async def _get_run(db: AsyncSession, run_id: str) -> Run:
    result = await db.execute(
        select(Run).options(selectinload(Run.approvals)).where(Run.id == run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run


def _after_seq(request: Request, after: int) -> int:
    """Prefer the Last-Event-ID sent by EventSource on reconnect."""
    header = request.headers.get("last-event-id")
    if header:
        try:
            return max(0, int(header))
        except ValueError:
            pass
    return max(0, after)


def _serialize(event: dict) -> str:
    return json.dumps(event, ensure_ascii=False, default=str)


def _run_response(run: Run) -> RunResponse:
    response = RunResponse.model_validate(run)
    if run.status == RunStatus.QUEUED:
        response.queue_position = run_manager.queue_position(run.id, run.conversation_id)
    return response


@router.post(
    "/conversations/{conversation_id}/runs",
    response_model=RunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_run(
    conversation_id: str,
    data: RunCreate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Create a user message + run and start it in the background.

    Always returns JSON. ``data.stream`` is accepted for backwards
    compatibility but ignored: the event stream lives at
    ``GET /api/runs/{run_id}/events`` (or ``/ws``).
    """
    message_text = data.message.strip()
    if not message_text:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Mensagem vazia")

    conversation = await db.get(Conversation, conversation_id)
    if conversation is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")

    run = Run(conversation_id=conversation_id, status=RunStatus.QUEUED)
    db.add(run)
    await db.flush()

    user_message = Message(
        conversation_id=conversation_id,
        role=MessageRole.USER,
        content=message_text,
        run_id=run.id,
    )
    assistant_message = Message(
        conversation_id=conversation_id,
        role=MessageRole.ASSISTANT,
        content="",
        run_id=run.id,
    )
    db.add_all([user_message, assistant_message])

    # Give the sidebar an instant, human-readable title; Hermes' generated
    # title replaces it when the `session.title` event arrives.
    if not conversation.title:
        conversation.title = message_text[:80]

    await db.commit()
    await db.refresh(run)

    run_manager.start_run_task(run.id, conversation_id, message_text)
    # A queued run may have advanced to `running` already; report the row as
    # created (202) and let the event stream carry the transitions.
    return _run_response(run)


@router.get("/runs", response_model=list[RunResponse])
async def list_runs(
    conversation_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """List runs with optional filters."""
    query = select(Run).join(Conversation).order_by(desc(Run.started_at))

    if conversation_id:
        query = query.where(Run.conversation_id == conversation_id)

    if status:
        try:
            query = query.where(Run.status == RunStatus(status))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Status inválido: {status}") from exc

    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)

    result = await db.execute(query)
    return [_run_response(r) for r in result.scalars().all()]


@router.get("/conversations/{conversation_id}/runs/active", response_model=Optional[RunResponse])
async def get_active_run(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Latest active run for a conversation (used on page load/reattach)."""
    conversation = await db.get(Conversation, conversation_id)
    if conversation is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")

    # A queued run has a newer started_at than the run it is waiting for, so
    # ordering by timestamp alone reattaches the page to the queue instead of
    # the executing turn (and hides a pending approval). Prefer the run that
    # actually owns the Hermes session.
    active_priority = case(
        (Run.status == RunStatus.WAITING_APPROVAL, 0),
        (Run.status == RunStatus.RUNNING, 1),
        else_=2,
    )
    result = await db.execute(
        select(Run)
        .where(
            Run.conversation_id == conversation_id,
            Run.status.in_(_ACTIVE_RUN_STATUSES),
        )
        .order_by(active_priority, desc(Run.started_at))
        .limit(1)
    )
    run = result.scalar_one_or_none()
    return _run_response(run) if run is not None else None


@router.get("/runs/{run_id}", response_model=RunResponse)
async def get_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Get run status and result."""
    return _run_response(await _get_run(db, run_id))


@router.get("/runs/{run_id}/events")
async def get_run_events(
    run_id: str,
    request: Request,
    after: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Replayable SSE stream of a run's normalized events.

    Supports ``Last-Event-ID`` (EventSource reconnect) and ``?after=<seq>``.
    Heartbeats are sent as SSE comments so proxies keep the connection open
    without waking up the client's event handlers.
    """
    await _get_run(db, run_id)
    last_event_id = _after_seq(request, after)

    async def event_stream():
        async for event in run_manager.stream_run_events(run_id, after_seq=last_event_id):
            if event is None:
                yield ": keep-alive\n\n"
                continue
            yield f"id: {event.get('seq', 0)}\nevent: {event['type']}\ndata: {_serialize(event)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=SSE_HEADERS)


@router.websocket("/runs/{run_id}/ws")
async def run_events_websocket(
    websocket: WebSocket,
    run_id: str,
    after: int = Query(0, ge=0),
):
    """WebSocket variant of the run event stream (mobile clients)."""
    token = websocket.query_params.get("token")
    if not token:
        auth_header = websocket.headers.get("authorization") or ""
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:]
    payload = decode_token(token) if token else None
    if not payload or payload.get("type") != "access" or not payload.get("sub"):
        await websocket.close(code=4401, reason="Unauthorized")
        return

    async with async_session_maker() as db:
        run = await db.get(Run, run_id)
        if run is None:
            await websocket.close(code=4404, reason="Run not found")
            return

    await websocket.accept()
    try:
        async for event in run_manager.stream_run_events(run_id, after_seq=max(0, after)):
            if event is None:
                await websocket.send_json({"type": "heartbeat", "run_id": run_id})
                continue
            await websocket.send_json(event)
            if event["type"] in run_manager.TERMINAL_EVENTS:
                break
    except WebSocketDisconnect:
        pass
    except RuntimeError:
        # Socket closed between the generator step and send().
        pass
    finally:
        try:
            await websocket.close()
        except RuntimeError:
            pass


@router.post("/runs/{run_id}/stop")
async def stop_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Request cancellation of a run."""
    run = await _get_run(db, run_id)
    if run.status not in _ACTIVE_RUN_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot stop run in status {run.status.value}",
        )

    effective = await run_manager.request_stop(run_id)
    if effective is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    return {"ok": True, "run_id": run_id, "status": effective}

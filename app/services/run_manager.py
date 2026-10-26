"""ODIN V2 — Run orchestration and live event fan-out.

Owns the bridge between a persisted ``Run`` and a live Hermes session:

* subscribes to Hermes gateway events before submitting the prompt (so no
  first tokens are lost),
* maps them to Odin's UI vocabulary,
* persists the assistant message, tool calls and terminal status,
* fans events out to SSE/WebSocket clients with replay and heartbeats.

The old implementation slept five seconds and returned whatever the PTY
happened to print. This one follows the actual turn lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List, Optional

from sqlalchemy import select

from app.core.config import get_settings
from app.core.database import async_session_maker
from app.integrations.hermes.events import map_event, truncate
from app.integrations.hermes.gateway import HermesError, HermesGateway, get_hermes_adapter
from app.models import Approval, ApprovalStatus, Conversation, Message, MessageRole, Run, RunStatus

logger = logging.getLogger(__name__)
settings = get_settings()

TERMINAL_EVENTS = frozenset({"run_completed", "run_failed", "run_cancelled"})
PERSISTED_EVENTS = frozenset(
    {
        "tool_call",
        "tool_result",
        "approval_requested",
        "approval_resolved",
        "approval_cancelled",
        "run_completed",
        "run_failed",
        "run_cancelled",
    }
)
_ACTIVE_STATUSES = (RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.WAITING_APPROVAL)
_BUS_TTL_SECONDS = 15 * 60


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class RunEventBus:
    """Replayable event fan-out for one run."""

    def __init__(self, run_id: str, max_events: int = 3000):
        self.run_id = run_id
        self._events: deque[dict] = deque(maxlen=max_events)
        self._subscribers: set[asyncio.Queue] = set()
        self._seq = 0
        self.closed = False
        self._done = asyncio.Event()
        self._cleanup_handle: Optional[asyncio.TimerHandle] = None

    def publish(self, event_type: str, data: dict) -> dict:
        self._seq += 1
        event = {
            "type": event_type,
            "data": data,
            "run_id": self.run_id,
            "timestamp": _iso_now(),
            "seq": self._seq,
        }
        self._events.append(event)
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Run %s subscriber queue full; dropping %s", self.run_id, event_type)

        if event_type in TERMINAL_EVENTS:
            self.closed = True
            self._done.set()
            self._schedule_cleanup()
        return event

    async def wait_closed(self) -> None:
        await self._done.wait()

    def open(self, after_seq: int = 0) -> tuple[asyncio.Queue, List[dict]]:
        queue: asyncio.Queue = asyncio.Queue(maxsize=3000)
        self._subscribers.add(queue)
        backlog = [event for event in self._events if event["seq"] > after_seq]
        return queue, backlog

    def close_queue(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def _schedule_cleanup(self) -> None:
        if self._cleanup_handle is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._cleanup_handle = loop.call_later(_BUS_TTL_SECONDS, self._dispose)

    def _dispose(self) -> None:
        if _buses.get(self.run_id) is self:
            _buses.pop(self.run_id, None)


_buses: Dict[str, RunEventBus] = {}
_run_tasks: set[asyncio.Task] = set()
_run_jobs: Dict[str, tuple[str, str]] = {}
_conversation_active: Dict[str, str] = {}
_pending_runs: Dict[str, deque[str]] = {}
_stop_requested: set[str] = set()


def create_bus(run_id: str) -> RunEventBus:
    bus = RunEventBus(run_id)
    _buses[run_id] = bus
    return bus


def get_bus(run_id: str) -> Optional[RunEventBus]:
    return _buses.get(run_id)


def start_run_task(run_id: str, conversation_id: str, message: str) -> Optional[asyncio.Task]:
    """Start a run, or queue it behind the conversation's active turn.

    Hermes supports its own prompt queue, but running one Odin turn at a time
    per conversation keeps the event → message mapping unambiguous: two
    subscribers on the same Hermes session would otherwise consume each
    other's deltas and tool events.
    """
    bus = get_bus(run_id) or create_bus(run_id)
    _run_jobs[run_id] = (conversation_id, message)

    active = _conversation_active.get(conversation_id)
    if active and active != run_id:
        queue = _pending_runs.setdefault(conversation_id, deque())
        queue.append(run_id)
        bus.publish(
            "run_queued",
            {"run_id": run_id, "position": len(queue), "conversation_id": conversation_id},
        )
        return None

    return _launch_run(run_id)


def _launch_run(run_id: str) -> asyncio.Task:
    conversation_id, message = _run_jobs.pop(run_id)
    _conversation_active[conversation_id] = run_id
    task = asyncio.create_task(
        execute_run(run_id, conversation_id, message),
        name=f"odin-run-{run_id}",
    )
    _run_tasks.add(task)
    task.add_done_callback(lambda done: _on_run_finished(done, conversation_id, run_id))
    return task


def _on_run_finished(task: asyncio.Task, conversation_id: str, run_id: str) -> None:
    _run_tasks.discard(task)
    if _conversation_active.get(conversation_id) == run_id:
        _conversation_active.pop(conversation_id, None)
    _stop_requested.discard(run_id)

    pending = _pending_runs.get(conversation_id)
    if pending:
        next_run_id = pending.popleft()
        if not pending:
            _pending_runs.pop(conversation_id, None)
        try:
            _launch_run(next_run_id)
        except Exception:
            logger.exception("Failed to launch queued run %s", next_run_id)


def is_conversation_busy(conversation_id: str) -> bool:
    return conversation_id in _conversation_active


def queue_position(run_id: str, conversation_id: str) -> int:
    pending = _pending_runs.get(conversation_id)
    if not pending:
        return 0
    try:
        return list(pending).index(run_id) + 1
    except ValueError:
        return 0


@dataclass
class _RunState:
    text: List[str] = field(default_factory=list)
    tool_calls: List[dict] = field(default_factory=list)
    tool_results: List[dict] = field(default_factory=list)
    approval_ids: List[str] = field(default_factory=list)
    started: bool = False

    @property
    def full_text(self) -> str:
        return "".join(self.text)


_run_states: Dict[str, _RunState] = {}


def _compact_event(event: dict) -> dict:
    """Bound persisted payloads so one tool cannot bloat the run row."""
    data = dict(event.get("data") or {})
    for key in ("result", "error", "args"):
        if key in data and data[key] is not None:
            if key == "args" and isinstance(data[key], dict):
                encoded = json.dumps(data[key], ensure_ascii=False, default=str)
                if len(encoded) > 4000:
                    data[key] = {"_truncated": True, "preview": encoded[:4000]}
            else:
                data[key] = truncate(data[key], 4000)
    return {**event, "data": data}


async def _append_run_event(db, run: Run, event: dict) -> None:
    if event["type"] not in PERSISTED_EVENTS:
        return
    run.events = [*(run.events or []), _compact_event(event)]


async def execute_run(run_id: str, conversation_id: str, message: str) -> None:
    gateway = get_hermes_adapter()
    bus = get_bus(run_id) or create_bus(run_id)
    state = _RunState()
    _run_states[run_id] = state
    subscription: Optional[asyncio.Queue] = None
    live_session_id: Optional[str] = None
    run: Optional[Run] = None
    conversation: Optional[Conversation] = None

    try:
        async with async_session_maker() as db:
            run = await db.get(Run, run_id)
            conversation = await db.get(Conversation, conversation_id)
            if run is None or conversation is None:
                logger.error("Run %s or conversation %s vanished before execution", run_id, conversation_id)
                bus.publish(
                    "run_failed",
                    {"error": "Execução ou conversa não encontrada.", "result": ""},
                )
                return

            if run.status in (
                RunStatus.CANCELLED,
                RunStatus.INTERRUPTED,
                RunStatus.COMPLETED,
                RunStatus.FAILED,
            ):
                if not bus.closed:
                    if run.status in (RunStatus.CANCELLED, RunStatus.INTERRUPTED):
                        bus.publish(
                            "run_cancelled",
                            {"reason": run.error or "Execução cancelada.", "result": run.result_summary or ""},
                        )
                    elif run.status == RunStatus.COMPLETED:
                        bus.publish(
                            "run_completed",
                            {"result": run.result_summary or "", "usage": None},
                        )
                    else:
                        bus.publish(
                            "run_failed",
                            {"error": run.error or "A execução falhou.", "result": run.result_summary or ""},
                        )
                return

            if run_id in _stop_requested:
                await _finalize(
                    db,
                    run,
                    conversation,
                    bus,
                    state,
                    RunStatus.CANCELLED,
                    error="Cancelada antes de iniciar.",
                )
                return

            run.status = RunStatus.RUNNING
            run.started_at = _utcnow()
            bus.publish("run_started", {"run_id": run_id, "started_at": run.started_at.isoformat() + "Z"})
            await db.commit()

            try:
                ref = await gateway.get_or_create_session(
                    conversation_id=conversation_id,
                    stored_session_id=conversation.hermes_session_id,
                    title=conversation.title,
                )
            except HermesError as exc:
                await _finalize(db, run, conversation, bus, state, RunStatus.FAILED, error=str(exc))
                return

            live_session_id = ref.live_id
            run.hermes_session_id = ref.live_id
            conversation.hermes_session_id = ref.stored_id
            await db.commit()

            bus.publish(
                "session_started",
                {
                    "hermes_session_id": ref.live_id,
                    "stored_session_id": ref.stored_id,
                },
            )

            # Subscribe BEFORE submitting so first deltas/tools cannot race us.
            subscription = gateway.subscribe(ref.live_id)

            if ref.pending_approval:
                await _on_approval_requested(
                    db, run, conversation, bus, state, ref.pending_approval, live_session_id
                )
                await db.commit()

            try:
                submit_result = await gateway.prompt(ref.live_id, message)
            except HermesError as exc:
                await _finalize(db, run, conversation, bus, state, RunStatus.FAILED, error=str(exc))
                return

            if submit_result.get("status") == "queued":
                bus.publish("status", {"kind": "queue", "text": "Mensagem na fila do agente…"})
            else:
                bus.publish("status", {"kind": "thinking", "text": "Processando…"})

            loop = asyncio.get_running_loop()
            deadline = loop.time() + settings.hermes_run_timeout

            while True:
                if bus.closed:
                    break
                if run.status in (RunStatus.CANCELLED, RunStatus.INTERRUPTED, RunStatus.COMPLETED, RunStatus.FAILED):
                    break
                try:
                    # Short poll so a server-side finalization (stop watchdog,
                    # approval cancellation) releases this task promptly.
                    raw = await asyncio.wait_for(subscription.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    if bus.closed:
                        break
                    if loop.time() >= deadline:
                        await _safe_interrupt(gateway, live_session_id)
                        await _finalize(
                            db,
                            run,
                            conversation,
                            bus,
                            state,
                            RunStatus.FAILED,
                            error="Tempo limite da execução excedido. O agente foi interrompido.",
                        )
                        break
                    continue

                for event in map_event(raw):
                    await _apply_event(db, run, conversation, bus, state, event)
                    if bus.closed:
                        break
                if bus.closed:
                    break

    except asyncio.CancelledError:
        if run is not None and conversation is not None:
            await _finalize(
                db=None,
                run=run,
                conversation=conversation,
                bus=bus,
                state=state,
                status=RunStatus.CANCELLED,
                error="Execução cancelada pelo servidor.",
            )
        raise
    except Exception:
        logger.exception("Unexpected error while executing run %s", run_id)
        if run is not None and conversation is not None:
            await _finalize(
                db=None,
                run=run,
                conversation=conversation,
                bus=bus,
                state=state,
                status=RunStatus.FAILED,
                error="Erro interno ao executar o agente.",
            )
    finally:
        if subscription is not None and live_session_id:
            gateway.unsubscribe(live_session_id, subscription)
        _run_states.pop(run_id, None)


async def _apply_event(
    db,
    run: Run,
    conversation: Conversation,
    bus: RunEventBus,
    state: _RunState,
    event: dict,
) -> None:
    event_type = event["type"]
    data = event["data"]

    if event_type == "message_start":
        state.started = True
        bus.publish("message_start", data)
        return

    if event_type == "token":
        delta = str(data.get("delta") or "")
        if not delta:
            return
        if not state.started:
            state.started = True
            bus.publish("message_start", {})
        state.text.append(delta)
        bus.publish("token", {"delta": delta})
        return

    if event_type == "reasoning":
        bus.publish("reasoning", data)
        return

    if event_type == "status":
        bus.publish("status", data)
        return

    if event_type == "session_info":
        bus.publish("session_info", data)
        return

    if event_type == "title":
        title = str(data.get("title") or "").strip()
        if title:
            conversation.title = title
            conversation.updated_at = _utcnow()
            bus.publish("title", {"title": title, "conversation_id": conversation.id})
            await db.commit()
        return

    if event_type == "tool_call":
        call_id = str(data.get("call_id") or "")
        stored = {
            "call_id": call_id,
            "tool": data.get("tool"),
            "args": data.get("args"),
            "context": data.get("context"),
            "started_at": _iso_now(),
        }
        state.tool_calls.append(stored)
        bus.publish("tool_call", stored)
        await _append_run_event(db, run, event)
        await db.commit()
        return

    if event_type == "tool_result":
        call_id = str(data.get("call_id") or "")
        stored = {
            "call_id": call_id,
            "tool": data.get("tool"),
            "result": truncate(data.get("result"), 8000),
            "is_error": bool(data.get("is_error")),
            "error": data.get("error"),
            "duration_s": data.get("duration_s"),
            "completed_at": _iso_now(),
        }
        state.tool_results = [r for r in state.tool_results if r.get("call_id") != call_id]
        state.tool_results.append(stored)
        bus.publish("tool_result", stored)
        await _append_run_event(db, run, event)
        await db.commit()
        return

    if event_type == "approval_requested":
        await _on_approval_requested(db, run, conversation, bus, state, data, run.hermes_session_id or "")
        await db.commit()
        return

    if event_type == "approval_cancelled":
        request_ids = [str(x) for x in (data.get("request_ids") or []) if x]
        approval_ids = [str(x) for x in (data.get("approval_ids") or []) if x]
        if request_ids:
            result = await db.execute(
                select(Approval).where(
                    Approval.run_id == run.id,
                    Approval.status == ApprovalStatus.PENDING,
                    Approval.hermes_approval_id.in_(request_ids),
                )
            )
            for approval in result.scalars().all():
                approval.status = ApprovalStatus.EXPIRED
                approval.resolved_at = _utcnow()
                approval_ids.append(approval.id)
        if run.status == RunStatus.WAITING_APPROVAL:
            run.status = RunStatus.RUNNING
        bus.publish("approval_cancelled", {"approval_ids": approval_ids, "reason": data.get("reason")})
        await _append_run_event(db, run, event)
        await db.commit()
        return

    if event_type == "message_complete":
        status = str(data.get("status") or "complete")
        final_text = str(data.get("text") or "").strip()
        if final_text:
            # message.complete carries the authoritative full text. If no
            # deltas were streamed (fast/tool-only turns) surface it once.
            if not state.text:
                state.text.append(final_text)
                bus.publish("token", {"delta": final_text})
            elif final_text != state.full_text:
                state.text = [final_text]
        if status == "interrupted":
            await _finalize(db, run, conversation, bus, state, RunStatus.CANCELLED, error=None)
        elif status in ("error", "failed"):
            await _finalize(
                db,
                run,
                conversation,
                bus,
                state,
                RunStatus.FAILED,
                error=str(data.get("error") or "O agente falhou ao concluir a execução."),
            )
        else:
            await _finalize(
                db,
                run,
                conversation,
                bus,
                state,
                RunStatus.COMPLETED,
                result=state.full_text,
                usage=data.get("usage"),
            )
        return

    if event_type == "error":
        await _finalize(
            db,
            run,
            conversation,
            bus,
            state,
            RunStatus.FAILED,
            error=str(data.get("error") or "Erro desconhecido no Hermes."),
        )
        return

    if event_type == "interrupted":
        await _finalize(db, run, conversation, bus, state, RunStatus.CANCELLED, error=None)
        return

    if event_type == "connection_lost":
        await _finalize(
            db,
            run,
            conversation,
            bus,
            state,
            RunStatus.FAILED,
            error="A conexão com o Hermes foi interrompida durante a execução. Tente novamente.",
        )
        return

    if event_type == "approval_resolved":
        if run.status == RunStatus.WAITING_APPROVAL:
            run.status = RunStatus.RUNNING
        bus.publish("approval_resolved", data)
        await db.commit()
        return


async def _on_approval_requested(
    db,
    run: Run,
    conversation: Conversation,
    bus: RunEventBus,
    state: _RunState,
    payload: dict,
    live_session_id: str,
) -> None:
    request_id = str(payload.get("request_id") or "")
    command = payload.get("command") or ""
    description = payload.get("description") or ""
    tool_name = payload.get("tool_name") or "approval"
    choices = payload.get("choices") or ["once", "deny"]

    if request_id:
        existing_result = await db.execute(
            select(Approval).where(Approval.hermes_approval_id == request_id)
        )
        existing = existing_result.scalar_one_or_none()
        if existing is not None:
            run.status = RunStatus.WAITING_APPROVAL
            bus.publish(
                "approval_requested",
                {
                    "approval_id": existing.id,
                    "request_id": request_id,
                    "operation": existing.operation,
                    "command": (existing.details or {}).get("command", command),
                    "description": (existing.details or {}).get("description", description),
                    "details": existing.details,
                    "choices": (existing.details or {}).get("choices") or choices,
                },
            )
            return

    approval = Approval(
        run_id=run.id,
        hermes_approval_id=request_id or None,
        operation=str(tool_name),
        details={
            "command": command,
            "description": description,
            "choices": choices,
            "allow_permanent": payload.get("allow_permanent"),
            "allow_session": payload.get("allow_session"),
            "session_id": live_session_id,
            "server_request_id": payload.get("_server_request_id"),
        },
        status=ApprovalStatus.PENDING,
    )
    db.add(approval)
    await db.flush()
    state.approval_ids.append(approval.id)
    run.status = RunStatus.WAITING_APPROVAL

    bus.publish(
        "approval_requested",
        {
            "approval_id": approval.id,
            "request_id": request_id,
            "operation": approval.operation,
            "command": command,
            "description": description,
            "details": approval.details,
            "choices": choices,
        },
    )
    await _append_run_event(
        db,
        run,
        {
            "type": "approval_requested",
            "data": {"approval_id": approval.id, "operation": approval.operation, "details": approval.details},
            "timestamp": _iso_now(),
        },
    )


async def _expire_pending_approvals(db, run_id: str) -> None:
    """A terminal run can no longer accept a decision, so clear its prompts."""
    result = await db.execute(
        select(Approval).where(
            Approval.run_id == run_id,
            Approval.status == ApprovalStatus.PENDING,
        )
    )
    now = _utcnow()
    for approval in result.scalars().all():
        approval.status = ApprovalStatus.EXPIRED
        approval.resolved_at = now


async def _finalize(
    db,
    run: Run,
    conversation: Conversation,
    bus: RunEventBus,
    state: _RunState,
    status: RunStatus,
    result: Optional[str] = None,
    error: Optional[str] = None,
    usage: Optional[dict] = None,
) -> None:
    if bus.closed and run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.INTERRUPTED):
        return

    owned_session = db is None
    if owned_session:
        db = async_session_maker()
        await db.__aenter__()
        # The run may have been finalized from an exception handler after its
        # original session closed, leaving the instances detached. Merge them
        # into this session so the terminal status/events are actually saved.
        run = await db.merge(run)
        conversation = await db.merge(conversation)

    try:
        run.status = status
        run.completed_at = _utcnow()
        text = result if result is not None else state.full_text
        if text:
            run.result_summary = text[:500]
        if error:
            run.error = error
        conversation.updated_at = _utcnow()

        result_db = await db.execute(
            select(Message)
            .where(Message.run_id == run.id, Message.role == MessageRole.ASSISTANT)
            .order_by(Message.created_at)
        )
        assistant = result_db.scalars().first()
        if assistant is not None:
            if text or not assistant.content:
                assistant.content = text or (error or "")
            if state.tool_calls:
                assistant.tool_calls = state.tool_calls
            if state.tool_results:
                assistant.tool_results = state.tool_results

        await _expire_pending_approvals(db, run.id)

        if status == RunStatus.COMPLETED:
            event_type = "run_completed"
            event_data = {
                "result": text,
                "usage": usage,
                "tool_calls": state.tool_calls,
                "tool_results": state.tool_results,
            }
        elif status in (RunStatus.CANCELLED, RunStatus.INTERRUPTED):
            event_type = "run_cancelled"
            event_data = {
                "reason": error or "Execução cancelada.",
                "result": text,
                "tool_calls": state.tool_calls,
                "tool_results": state.tool_results,
            }
        else:
            event_type = "run_failed"
            event_data = {
                "error": error or "A execução falhou.",
                "result": text,
                "tool_calls": state.tool_calls,
                "tool_results": state.tool_results,
            }

        # Persist and commit BEFORE the terminal event reaches subscribers:
        # clients reload the run/messages as soon as it arrives, so publishing
        # first made them read the previous status ("running") and an empty
        # assistant message.
        await _append_run_event(
            db,
            run,
            {
                "type": event_type,
                "data": event_data,
                "run_id": run.id,
                "timestamp": _iso_now(),
            },
        )
        await db.commit()
        bus.publish(event_type, event_data)
    finally:
        if owned_session:
            await db.__aexit__(None, None, None)


async def _safe_interrupt(gateway: HermesGateway, live_session_id: Optional[str]) -> None:
    if not live_session_id:
        return
    try:
        await gateway.interrupt(live_session_id)
    except Exception:
        logger.warning("Failed to interrupt Hermes session %s", live_session_id, exc_info=True)


async def request_stop(run_id: str) -> Optional[str]:
    """Ask Hermes to stop a run. Returns the effective status."""
    async with async_session_maker() as db:
        run = await db.get(Run, run_id)
        if run is None:
            return None
        if run.status not in _ACTIVE_STATUSES:
            return run.status.value

        gateway = get_hermes_adapter()
        bus = get_bus(run_id) or create_bus(run_id)
        state = _run_states.get(run_id) or _RunState()
        live_session_id = run.hermes_session_id or gateway.get_live_session_id(run.conversation_id)

        if run.status == RunStatus.QUEUED:
            _stop_requested.add(run_id)
            _run_jobs.pop(run_id, None)
            pending = _pending_runs.get(run.conversation_id)
            if pending and run_id in pending:
                pending.remove(run_id)
                if not pending:
                    _pending_runs.pop(run.conversation_id, None)
            conversation = await db.get(Conversation, run.conversation_id)
            if conversation is not None:
                await _finalize(
                    db,
                    run,
                    conversation,
                    bus,
                    state,
                    RunStatus.CANCELLED,
                    error="Cancelada antes de iniciar.",
                )
            return RunStatus.CANCELLED.value

        _stop_requested.add(run_id)
        if bus:
            bus.publish("run_stopping", {"reason": "Interrompendo o agente…"})

        if live_session_id:
            try:
                await gateway.interrupt(live_session_id)
            except Exception:
                logger.warning("Failed to interrupt run %s", run_id, exc_info=True)

        asyncio.create_task(_force_cancel_after(run_id, state), name=f"odin-run-stop-{run_id}")
        return "stopping"


async def _force_cancel_after(run_id: str, state: _RunState, seconds: float = 25.0) -> None:
    """Watchdog: never leave the UI stuck on 'stopping'."""
    await asyncio.sleep(seconds)
    async with async_session_maker() as db:
        run = await db.get(Run, run_id)
        if run is None or run.status not in _ACTIVE_STATUSES:
            return
        bus = get_bus(run_id) or create_bus(run_id)
        conversation = await db.get(Conversation, run.conversation_id)
        if conversation is None:
            return
        await _finalize(
            db,
            run,
            conversation,
            bus,
            state,
            RunStatus.CANCELLED,
            error="Interrupção confirmada localmente (o Hermes não respondeu a tempo).",
        )


async def resolve_approval(
    approval: Approval,
    run: Run,
    choice: str,
    approval_status: ApprovalStatus,
) -> None:
    """Send the decision to Hermes and notify live subscribers."""
    gateway = get_hermes_adapter()
    live_session_id = run.hermes_session_id or gateway.get_live_session_id(run.conversation_id)
    if not live_session_id:
        raise HermesError("Sessão do Hermes não encontrada para esta aprovação.")

    request_id = approval.hermes_approval_id or ""
    await gateway.respond_approval(live_session_id, request_id, choice)

    bus = get_bus(run.id)
    if bus is not None:
        bus.publish(
            "approval_resolved",
            {
                "approval_id": approval.id,
                "request_id": request_id,
                "status": approval_status.value,
                "choice": choice,
            },
        )
    if run.status == RunStatus.WAITING_APPROVAL:
        run.status = RunStatus.RUNNING


async def stream_run_events(
    run_id: str,
    after_seq: int = 0,
    heartbeat_seconds: float = 15.0,
) -> AsyncGenerator[Optional[dict], None]:
    """Yield events for SSE. ``None`` means "send a heartbeat comment"."""
    bus = get_bus(run_id)
    if bus is None:
        # The server restarted or the replay window expired: emit one snapshot
        # so the client can settle instead of waiting forever.
        async with async_session_maker() as db:
            run = await db.get(Run, run_id)
            if run is None:
                yield {
                    "type": "run_failed",
                    "run_id": run_id,
                    "data": {"error": "Execução não encontrada."},
                    "timestamp": _iso_now(),
                    "seq": 0,
                }
                return
            yield {
                "type": "run_snapshot",
                "run_id": run_id,
                "data": {
                    "status": run.status.value,
                    "result_summary": run.result_summary,
                    "error": run.error,
                    "active": run.status in _ACTIVE_STATUSES,
                },
                "timestamp": _iso_now(),
                "seq": 0,
            }
        return

    queue, backlog = bus.open(after_seq)
    last_seq = after_seq
    try:
        for event in backlog:
            if event["seq"] <= last_seq:
                continue
            last_seq = event["seq"]
            yield event
        if bus.closed:
            return

        while True:
            if bus.closed and queue.empty():
                return
            try:
                event = await asyncio.wait_for(queue.get(), timeout=heartbeat_seconds)
            except asyncio.TimeoutError:
                yield None
                continue
            if event["seq"] <= last_seq:
                continue
            last_seq = event["seq"]
            yield event
            if event["type"] in TERMINAL_EVENTS:
                return
    finally:
        bus.close_queue(queue)


async def recover_stale_runs() -> int:
    """Mark runs left behind by a server restart as failed.

    In-memory live sessions do not survive a process restart, so leaving these
    rows ``running`` would make the UI spin forever on reattach.
    """
    recovered = 0
    async with async_session_maker() as db:
        result = await db.execute(select(Run).where(Run.status.in_(_ACTIVE_STATUSES)))
        pending_notifications = []
        for run in result.scalars().all():
            run.status = RunStatus.FAILED
            run.completed_at = _utcnow()
            run.error = "Execução interrompida por reinício do ODIN. Envie a mensagem novamente."
            await _expire_pending_approvals(db, run.id)
            bus = get_bus(run.id) or create_bus(run.id)
            pending_notifications.append((bus, {"error": run.error, "result": ""}))
            recovered += 1
        await db.commit()
    for bus, payload in pending_notifications:
        bus.publish("run_failed", payload)
    if recovered:
        logger.info("Recovered %s stale run(s) after restart", recovered)
    return recovered

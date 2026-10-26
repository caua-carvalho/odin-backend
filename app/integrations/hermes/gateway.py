"""ODIN V2 — Hermes Gateway Client.

Speaks Hermes' structured JSON-RPC protocol over ``/api/ws`` (the same
protocol used by the Hermes TUI and dashboard sidebar). This replaces the raw
PTY terminal stream, which could not provide token streaming, tool lifecycle
events, approvals, or reliable completion detection.

Protocol notes (Hermes Agent v0.22):

* Frames are newline-delimited JSON-RPC 2.0 over WebSocket.
* Server -> client events use ``{"method": "event", "params": {...}}`` where
  ``params.type`` is e.g. ``message.delta`` / ``tool.start`` / ``message.complete``.
* Server -> client requests (approvals) carry an ``srq-*`` id and expect a
  JSON-RPC response: ``{"id": "srq-...", "result": {"choice": "once"}}``.
* One socket can host many sessions. Clients must send
  ``client.capabilities {"server_requests": true}`` after ``gateway.ready``.

The gateway auto-resumes a stored Hermes session when a new Odin run starts in
a conversation that already has a Hermes transcript.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import re
from urllib.parse import urlencode

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class HermesError(Exception):
    """Base error for the Hermes gateway."""


class HermesUnavailable(HermesError):
    """Hermes cannot be reached."""


class HermesRpcError(HermesError):
    """Hermes answered a JSON-RPC call with an error."""

    def __init__(self, code: int, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"Hermes RPC {code}: {message}")


@dataclass
class HermesSessionRef:
    """A live Hermes session plus its durable (resumable) identity."""

    live_id: str
    stored_id: str
    conversation_id: Optional[str] = None
    pending_approval: Optional[dict] = None


class HermesGateway:
    """Single multiplexed JSON-RPC connection to Hermes."""

    def __init__(self, settings=None):
        self.settings = settings or get_settings()
        self.base_url = self.settings.hermes_api_url.rstrip("/")
        self.session_token = self.settings.hermes_session_token

        self._ws: Optional[Any] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._next_id = 0
        self._pending: Dict[int, asyncio.Future] = {}
        self._subscriptions: Dict[str, set[asyncio.Queue]] = {}
        self._open_server_requests: Dict[str, dict] = {}
        self._send_tasks: set[asyncio.Task] = set()
        self._ready = asyncio.Event()
        self._closed = True
        self._last_disconnect: Optional[str] = None

        # conversation_id -> live/server session mapping, preserved across
        # WebSocket reconnects so a run can resume the durable transcript.
        self._conversations: Dict[str, HermesSessionRef] = {}
        self._sessions_by_live: Dict[str, HermesSessionRef] = {}

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return not self._closed and self._ws is not None

    
    async def _refresh_session_token(self) -> str:
        """Fetch the current session token from the Hermes dashboard."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/")
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HermesUnavailable(
                "Não foi possível obter o token atual do Hermes."
            ) from exc

        match = re.search(
            r"""__HERMES_SESSION_TOKEN__\s*=\s*["']([^"']+)["']""",
            response.text,
        )

        if not match:
            raise HermesUnavailable(
                "Token de sessão não encontrado na página do Hermes."
            )

        self.session_token = match.group(1)
        return self.session_token

    def _ws_url(self) -> str:
        ws_base = (
            self.base_url
            .replace("http://", "ws://", 1)
            .replace("https://", "wss://", 1)
        )
        url = f"{ws_base}/api/ws"

        if self.session_token:
            url += "?" + urlencode({"token": self.session_token})

        return url

        ws_base = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        url = f"{ws_base}/api/ws"
        if self.session_token:
            url += "?token=" + self.session_token
        return url

    async def connect(self) -> None:
        """Open the WebSocket and complete the ``gateway.ready`` handshake."""
        async with self._connect_lock:
            if self.connected:
                return

            await self._refresh_session_token()
            url = self._ws_url()

            logger.info(
                "Connecting to Hermes gateway at %s",
                url.split("?", 1)[0],
            )

            try:
                ws = await asyncio.wait_for(
                    websockets.connect(
                        url,
                        open_timeout=self.settings.hermes_connect_timeout,
                        ping_interval=20,
                        ping_timeout=20,
                        max_size=None,
                    ),
                    timeout=self.settings.hermes_connect_timeout + 2,
                )
            except asyncio.TimeoutError as exc:
                raise HermesUnavailable("Tempo esgotado ao conectar no Hermes.") from exc
            except WebSocketException as exc:
                raise HermesUnavailable(f"Falha ao conectar no Hermes: {exc}") from exc
            except OSError as exc:
                raise HermesUnavailable(f"Hermes indisponível: {exc}") from exc

            self._ws = ws
            self._closed = False
            self._last_disconnect = None
            self._ready.clear()
            self._reader_task = asyncio.create_task(self._reader_loop(ws))

            try:
                await asyncio.wait_for(self._ready.wait(), timeout=self.settings.hermes_connect_timeout)
            except asyncio.TimeoutError as exc:
                await self._teardown("gateway.ready timeout")
                raise HermesUnavailable("Hermes não enviou gateway.ready.") from exc

            # Advertise that we can answer server->client requests. Without
            # this Hermes will not send approvals to this client.
            try:
                await self.call("client.capabilities", {"server_requests": True}, timeout=10)
            except HermesError as exc:
                logger.warning("client.capabilities failed: %s", exc)

            logger.info("Hermes gateway connected")

    async def ensure_connected(self) -> None:
        if not self.connected:
            await self.connect()

    async def close(self) -> None:
        await self._teardown("shutdown")

    async def _teardown(self, reason: str) -> None:
        self._closed = True
        self._last_disconnect = reason
        ws, self._ws = self._ws, None

        if self._reader_task:
            current = asyncio.current_task()
            if self._reader_task is not current:
                self._reader_task.cancel()
            self._reader_task = None

        for task in list(self._send_tasks):
            task.cancel()
        self._send_tasks.clear()

        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(HermesUnavailable(f"Conexão com Hermes encerrada ({reason})."))
        self._pending.clear()
        self._open_server_requests.clear()

        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

        # Active runs must not hang forever waiting for events.
        for session_id in list(self._subscriptions):
            self._publish_local(session_id, {"type": "connection_lost", "session_id": session_id, "payload": {"reason": reason}})

    # ------------------------------------------------------------------
    # Reader / frame dispatch
    # ------------------------------------------------------------------

    async def _reader_loop(self, ws) -> None:
        try:
            async for raw in ws:
                try:
                    frame = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    logger.debug("Ignoring non-JSON Hermes frame: %.120r", raw)
                    continue
                self._handle_frame(frame)
        except ConnectionClosed as exc:
            logger.info("Hermes gateway disconnected: %s", exc)
            await self._teardown("connection closed")
        except WebSocketException as exc:
            logger.warning("Hermes gateway WebSocket error: %s", exc)
            await self._teardown(f"websocket error: {exc}")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Hermes gateway reader crashed")
            await self._teardown("reader crashed")

    def _handle_frame(self, frame: dict) -> None:
        # Response to one of our calls.
        if "id" in frame and ("result" in frame or "error" in frame):
            self._resolve_response(frame)
            return

        method = frame.get("method")
        params = frame.get("params") or {}

        if method == "event":
            event_type = params.get("type")
            if event_type == "gateway.ready":
                self._ready.set()
                return
            self._dispatch_event(params)
            return

        # Server -> client request (approval, clarify, sudo, ...).
        if "id" in frame and method:
            self._handle_server_request(frame)
            return

        logger.debug("Unhandled Hermes frame: %s", frame)

    def _resolve_response(self, frame: dict) -> None:
        raw_id = frame.get("id")
        future = self._pending.pop(raw_id, None)
        if future is None or future.done():
            return
        if "error" in frame:
            error = frame.get("error") or {}
            future.set_exception(
                HermesRpcError(
                    int(error.get("code", -32000)),
                    str(error.get("message", "Erro desconhecido")),
                    error.get("data"),
                )
            )
        else:
            future.set_result(frame.get("result") or {})

    def _dispatch_event(self, params: dict) -> None:
        session_id = str(params.get("session_id") or "")
        event_type = str(params.get("type") or "")

        if event_type == "request.cancel":
            # A server request stopped waiting (interrupt/reap/timeout). The
            # payload identifies the original srq-* request.
            payload = params.get("payload") or {}
            request_id = str(payload.get("id") or "")
            if request_id:
                self._drop_server_request(request_id=request_id)
            self._publish_local(
                session_id,
                {
                    "type": "approval_cancelled",
                    "session_id": session_id,
                    "payload": {
                        "request_ids": [request_id] if request_id else [],
                        "reason": payload.get("reason") or "cancelled",
                    },
                },
            )
            return

        if event_type == "approval.cancelled":
            payload = params.get("payload") or {}
            for request_id in payload.get("request_ids") or []:
                self._drop_server_request(request_id=str(request_id))
            self._publish_local(session_id, {"type": "approval_cancelled", "session_id": session_id, "payload": payload})
            return

        self._publish_local(session_id, {"type": event_type, "session_id": session_id, "payload": params.get("payload") or {}, "seq": params.get("seq")})

    def _drop_server_request(self, *, request_id: str = "", srq_id: str = "") -> dict | None:
        """Remove an open approval from the lookup table (both id keys)."""
        frame = None
        for key, value in list(self._open_server_requests.items()):
            frame_id = str(value.get("id") or "")
            payload = value.get("params") or {}
            approval_id = str(payload.get("request_id") or "")
            if (request_id and request_id in (key, approval_id)) or (srq_id and srq_id in (key, frame_id)):
                frame = value
                self._open_server_requests.pop(key, None)
        return frame

    def _handle_server_request(self, frame: dict) -> None:
        method = str(frame.get("method") or "")
        params = frame.get("params") or {}
        request_id = str(frame.get("id") or "")
        session_id = str(params.get("session_id") or "")

        if method == "approval":
            # Index by both the srq frame id and the approval id so a decision
            # can be routed either way after a reconnect.
            self._open_server_requests[request_id] = frame
            approval_id = str(params.get("request_id") or "")
            if approval_id:
                self._open_server_requests[approval_id] = frame
            payload = {**params, "_server_request_id": request_id}
            self._publish_local(
                session_id,
                {
                    "type": "approval_requested",
                    "session_id": session_id,
                    "request_id": request_id,
                    "payload": payload,
                },
            )
            return

        # Unsupported interactive request: answer immediately so the agent does
        # not sit waiting for a human that Odin cannot present yet.
        logger.warning("Declining unsupported Hermes request: %s", method)
        self._send_frame(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"ODIN não suporta a solicitação interativa '{method}'.",
                },
            }
        )

    # ------------------------------------------------------------------
    # Subscriptions (per Hermes live session)
    # ------------------------------------------------------------------

    def subscribe(self, session_id: str, maxsize: int = 4000) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._subscriptions.setdefault(session_id, set()).add(queue)
        return queue

    def unsubscribe(self, session_id: str, queue: asyncio.Queue) -> None:
        subscribers = self._subscriptions.get(session_id)
        if not subscribers:
            return
        subscribers.discard(queue)
        if not subscribers:
            self._subscriptions.pop(session_id, None)

    def _publish_local(self, session_id: str, event: dict) -> None:
        subscribers = self._subscriptions.get(session_id)
        if not subscribers:
            return
        for queue in list(subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("Hermes event queue full for session %s; dropping %s", session_id, event.get("type"))
            except Exception:
                logger.exception("Failed to publish Hermes event")

    # ------------------------------------------------------------------
    # JSON-RPC calls
    # ------------------------------------------------------------------

    def _send_frame(self, frame: dict) -> None:
        if not self.connected or self._ws is None:
            raise HermesUnavailable("Não conectado ao Hermes.")

        async def _send() -> None:
            try:
                if self._ws is not None:
                    await self._ws.send(json.dumps(frame, ensure_ascii=False))
            except Exception:
                logger.warning("Failed to send Hermes frame", exc_info=True)

        task = asyncio.create_task(_send())
        self._send_tasks.add(task)
        task.add_done_callback(self._send_tasks.discard)

    async def call(self, method: str, params: dict, timeout: float = 60.0) -> dict:
        if not self.connected or self._ws is None:
            raise HermesUnavailable("Não conectado ao Hermes.")

        self._next_id += 1
        request_id = self._next_id
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[request_id] = future

        frame = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        try:
            await self._ws.send(json.dumps(frame, ensure_ascii=False))
        except Exception as exc:
            self._pending.pop(request_id, None)
            raise HermesUnavailable(f"Falha ao enviar para o Hermes: {exc}") from exc

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            self._pending.pop(request_id, None)
            raise HermesUnavailable(f"Hermes não respondeu a {method} em {timeout:.0f}s.") from exc

    # ------------------------------------------------------------------
    # Session helpers
    # ------------------------------------------------------------------

    async def _create_session(self, title: Optional[str] = None, idempotency_key: Optional[str] = None) -> HermesSessionRef:
        params: Dict[str, Any] = {
            "source": "odin",
            "close_on_disconnect": False,
        }
        if title:
            params["title"] = title
        if idempotency_key:
            params["idempotency_key"] = idempotency_key

        result = await self.call("session.create", params, timeout=90)
        live_id = str(result.get("session_id") or "")
        stored_id = str(result.get("stored_session_id") or live_id)
        if not live_id:
            raise HermesError("Hermes retornou uma sessão inválida.")
        ref = HermesSessionRef(live_id=live_id, stored_id=stored_id)
        self._sessions_by_live[live_id] = ref
        return ref

    async def _resume_session(self, stored_id: str) -> HermesSessionRef:
        result = await self.call(
            "session.resume",
            {
                "session_id": stored_id,
                "lazy": True,
                "defer_history": True,
                "omit_messages": True,
                "close_on_disconnect": False,
            },
            timeout=90,
        )
        live_id = str(result.get("session_id") or "")
        if not live_id:
            raise HermesError("Hermes retornou uma sessão inválida ao retomar.")
        durable = str(result.get("stored_session_id") or result.get("session_key") or stored_id)
        ref = HermesSessionRef(
            live_id=live_id,
            stored_id=durable,
            pending_approval=result.get("pending_approval"),
        )
        self._sessions_by_live[live_id] = ref
        return ref

    async def get_or_create_session(
        self,
        conversation_id: str,
        stored_session_id: Optional[str],
        title: Optional[str] = None,
    ) -> HermesSessionRef:
        """Return the live session for a conversation, resuming if needed."""
        await self.ensure_connected()

        existing = self._conversations.get(conversation_id)
        if existing and existing.live_id in self._sessions_by_live:
            return existing

        ref: Optional[HermesSessionRef] = None
        if stored_session_id:
            try:
                ref = await self._resume_session(stored_session_id)
            except HermesRpcError as exc:
                logger.info("Could not resume Hermes session %s (%s); creating a new one", stored_session_id, exc.message)
            except HermesError:
                raise

        if ref is None:
            ref = await self._create_session(
                title=title,
                idempotency_key=f"odin-conv-{conversation_id}",
            )

        ref.conversation_id = conversation_id
        self._conversations[conversation_id] = ref
        return ref

    def get_live_session_id(self, conversation_id: str) -> Optional[str]:
        ref = self._conversations.get(conversation_id)
        return ref.live_id if ref else None

    def get_session_by_live_id(self, live_id: str) -> Optional[HermesSessionRef]:
        return self._sessions_by_live.get(live_id)

    async def prompt(self, live_id: str, text: str) -> dict:
        return await self.call(
            "prompt.submit",
            {"session_id": live_id, "text": text, "surface": "odin"},
            timeout=90,
        )

    async def interrupt(self, live_id: str) -> dict:
        return await self.call("session.interrupt", {"session_id": live_id}, timeout=30)

    async def respond_approval(self, live_id: str, request_id: str, choice: str) -> dict:
        """Resolve a pending approval.

        Prefer answering the original server request (lowest latency). If the
        original frame is gone (e.g. after a reconnect), fall back to the
        ``approval.respond`` RPC.
        """
        frame = self._drop_server_request(request_id=request_id)
        if frame is not None:
            self._send_frame(
                {
                    "jsonrpc": "2.0",
                    "id": frame.get("id"),
                    "result": {"choice": choice},
                }
            )
            return {"resolved": 1}

        return await self.call(
            "approval.respond",
            {
                "session_id": live_id,
                "request_id": request_id,
                "choice": choice,
            },
            timeout=30,
        )

    async def pending_approvals(self, live_id: str) -> list:
        result = await self.call("approval.pending", {"session_id": live_id}, timeout=15)
        return list(result.get("approvals") or [])

    # ------------------------------------------------------------------
    # HTTP health / status (kept from the old adapter)
    # ------------------------------------------------------------------

    def _headers(self) -> dict:
        if self.session_token:
            return {"X-Hermes-Session-Token": self.session_token}
        return {}

    async def health_check(self) -> bool:
        try:
            await self._refresh_session_token()
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/api/health", headers=self._headers())
                return response.status_code == 200
        except Exception:
            return False

    async def get_system_status(self) -> Optional[dict]:
        try:
            await self._refresh_session_token()
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/api/status", headers=self._headers())
                if response.status_code == 200:
                    return response.json()
        except Exception as exc:
            logger.warning("Failed to get Hermes status: %s", exc)
        return None


_gateway: Optional[HermesGateway] = None


def get_hermes_adapter() -> HermesGateway:
    """Backwards-compatible accessor used by the routes/system probe."""
    global _gateway
    if _gateway is None:
        _gateway = HermesGateway()
    return _gateway


async def init_hermes_adapter(settings=None) -> HermesGateway:
    global _gateway
    _gateway = HermesGateway(settings)
    try:
        await _gateway.connect()
        logger.info("Hermes gateway ready")
    except HermesError as exc:
        # Hermes may be started after Odin; the first run will retry.
        logger.warning("Hermes gateway not available at startup: %s", exc)
    return _gateway


async def close_hermes_adapter() -> None:
    global _gateway
    if _gateway is not None:
        await _gateway.close()
        _gateway = None

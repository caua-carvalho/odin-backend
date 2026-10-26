"""ODIN V2 — Hermes event mapper.

Translates Hermes gateway events (tui_gateway JSON-RPC, v0.22) into the small,
stable event vocabulary consumed by Odin's SSE/WS streams.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_MAX_RESULT_CHARS = 6000


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def truncate(value: Any, limit: int = _MAX_RESULT_CHARS) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        try:
            value = json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            value = str(value)
    if len(value) <= limit:
        return value
    return value[:limit] + f"\n… (truncado, {len(value) - limit} caracteres)"


def _result_text(payload: dict) -> str:
    if payload.get("summary"):
        return str(payload["summary"])
    if payload.get("result_text"):
        return truncate(payload["result_text"])
    result = payload.get("result")
    if isinstance(result, dict):
        # terminal tool results are {output, exit_code, error}
        if result.get("error"):
            return truncate(result["error"])
        if "output" in result:
            return truncate(result.get("output"))
    return truncate(result)


class HermesEventMapper:
    """Map a raw Hermes frame to zero or more normalized Odin events."""

    def map(self, raw: dict) -> List[dict]:
        event_type = str(raw.get("type") or "")
        payload: Dict[str, Any] = raw.get("payload") or {}

        if event_type == "message.start":
            return [self._event("message_start", {})]

        if event_type in ("message.delta", "token"):
            delta = payload.get("text") or payload.get("delta") or ""
            if not delta:
                return []
            return [self._event("token", {"delta": str(delta)})]

        if event_type in ("reasoning.delta", "thinking.delta"):
            delta = payload.get("text") or ""
            # Hermes emits a cosmetic "(¬_¬) reasoning..." placeholder first.
            if not delta or delta.startswith("("):
                return []
            return [self._event("reasoning", {"delta": str(delta)})]

        if event_type in ("tool.generating", "tool.progress"):
            return [
                self._event(
                    "status",
                    {"kind": "tool", "text": f"Preparando {payload.get('name') or 'ferramenta'}…"},
                )
            ]

        if event_type in ("tool.start", "tool_call", "tool_call_start"):
            return [self._event("tool_call", self._tool_start(payload))]

        if event_type in ("tool.complete", "tool_result", "tool_call_end"):
            return [self._event("tool_result", self._tool_result(payload))]

        if event_type in ("status.update", "status"):
            text = payload.get("text") or payload.get("message") or ""
            if not text:
                return []
            return [self._event("status", {"kind": payload.get("kind") or "info", "text": str(text)})]

        if event_type == "session.title":
            title = payload.get("title")
            if not title:
                return []
            return [self._event("title", {"title": str(title)})]

        if event_type == "session.info":
            keep = ("model", "provider", "reasoning_effort", "approval_mode", "cwd", "profile_name")
            return [self._event("session_info", {k: payload.get(k) for k in keep if payload.get(k) is not None})]

        if event_type in ("message.complete", "run_completed", "turn_complete"):
            return [self._event("message_complete", {
                "text": payload.get("text") or payload.get("result") or "",
                "status": payload.get("status") or "complete",
                "usage": payload.get("usage"),
                "error": payload.get("error") or payload.get("failure_reason"),
            })]

        if event_type in ("error", "run_failed"):
            message = payload.get("message") or payload.get("error") or "Erro desconhecido no Hermes."
            return [self._event("error", {"error": str(message), "retryable": payload.get("retryable")})]

        if event_type in ("interrupted", "cancelled", "run_cancelled"):
            return [self._event("interrupted", {"reason": payload.get("reason") or "Execução interrompida."})]

        if event_type in ("approval_requested", "approval"):
            return [self._event("approval_requested", payload)]

        if event_type == "approval_cancelled":
            return [self._event("approval_cancelled", payload)]

        if event_type == "connection_lost":
            return [self._event("connection_lost", payload)]

        if event_type in ("sessions.changed", "session.ready", "thinking.start", "thinking.stop"):
            return []

        return []

    # ------------------------------------------------------------------

    def _tool_start(self, payload: dict) -> dict:
        call_id = str(payload.get("tool_id") or payload.get("call_id") or payload.get("id") or "")
        tool_name = str(payload.get("name") or payload.get("tool") or "ferramenta")
        args = payload.get("args") or payload.get("arguments") or {}
        return {
            "call_id": call_id,
            "tool": tool_name,
            "args": args,
            "context": payload.get("context") or payload.get("preview"),
        }

    def _tool_result(self, payload: dict) -> dict:
        call_id = str(payload.get("tool_id") or payload.get("call_id") or payload.get("id") or "")
        error = payload.get("error")
        result = _result_text(payload)
        is_error = bool(error) or bool(payload.get("is_error"))
        if is_error and not result:
            result = str(error)
        return {
            "call_id": call_id,
            "tool": payload.get("name"),
            "result": result,
            "is_error": is_error,
            "error": str(error) if error else None,
            "duration_s": payload.get("duration_s"),
        }

    def _event(self, event_type: str, data: dict) -> dict:
        return {"type": event_type, "data": data, "timestamp": _utcnow()}


def map_event(raw: dict) -> List[dict]:
    return HermesEventMapper().map(raw)

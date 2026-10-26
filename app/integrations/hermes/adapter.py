"""ODIN V2 — Hermes adapter (compatibility shim).

The integration now speaks Hermes' JSON-RPC gateway over ``/api/ws`` instead
of proxying a raw PTY. See :mod:`app.integrations.hermes.gateway`.
"""

from app.integrations.hermes.gateway import (  # noqa: F401
    HermesError,
    HermesError as HermesAdapterError,
    HermesGateway as HermesAdapter,
    HermesRpcError,
    HermesSessionRef,
    HermesUnavailable as HermesConnectionError,
    close_hermes_adapter,
    get_hermes_adapter,
    init_hermes_adapter,
)

__all__ = [
    "HermesAdapter",
    "HermesAdapterError",
    "HermesConnectionError",
    "HermesError",
    "HermesGateway",
    "HermesRpcError",
    "HermesSessionRef",
    "close_hermes_adapter",
    "get_hermes_adapter",
    "init_hermes_adapter",
]

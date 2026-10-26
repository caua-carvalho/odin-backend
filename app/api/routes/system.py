"""ODIN V2 Backend — System Status Routes"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.core.security import get_current_user_id
from app.integrations.hermes.adapter import get_hermes_adapter
from app.schemas import HealthResponse, SystemStatusComponent, SystemStatusResponse

router = APIRouter()
settings = get_settings()


@router.get("/status", response_model=SystemStatusResponse)
async def system_status(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    """Get full system status."""
    # Check database
    db_status = "ok"
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        db_status = "degraded"

    # Check Hermes
    hermes_status = "ok"
    hermes_details = {}
    adapter = get_hermes_adapter()
    hermes_healthy = await adapter.health_check()
    if not hermes_healthy:
        hermes_status = "degraded"
    else:
        status_data = await adapter.get_system_status()
        if status_data:
            hermes_details = status_data

    components = {
        "database": SystemStatusComponent(status=db_status),
        "hermes": SystemStatusComponent(status=hermes_status, details=hermes_details),
        "api": SystemStatusComponent(status="ok"),
    }

    overall = "ok" if all(c.status == "ok" for c in components.values()) else "degraded"

    return SystemStatusResponse(
        version="0.1.0",
        overall=overall,
        components=components,
        hermes_connected=hermes_healthy,
        database_connected=db_status == "ok",
        timestamp=__import__("datetime").datetime.utcnow(),
    )


@router.get("/health", response_model=HealthResponse)
async def system_health():
    """Simple health check."""
    return HealthResponse(
        ok=True,
        version="0.1.0",
        timestamp=__import__("datetime").datetime.utcnow(),
    )
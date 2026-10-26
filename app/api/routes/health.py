"""ODIN V2 Backend — Health Routes"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/api/health")
async def health_check():
    """Lightweight health check."""
    return {"ok": True, "service": "odin-backend", "version": "0.1.0"}
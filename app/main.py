"""ODIN V2 Backend — Main Application"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from structlog import get_logger

from app.core.config import get_settings
from app.core.database import close_db, init_db
from app.core.security import revoke_all_user_sessions
from app.integrations.hermes.adapter import init_hermes_adapter, close_hermes_adapter
from app.services.run_manager import recover_stale_runs
from app.api.routes import health, auth, conversations, runs, approvals, knowledge, system

logger = get_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    # Startup
    logger.info("Starting ODIN V2 Backend", version="0.1.0")
    await init_db()
    logger.info("Database initialized")
    await recover_stale_runs()
    await init_hermes_adapter()
    logger.info("Hermes adapter initialized")

    yield

    # Shutdown
    logger.info("Shutting down ODIN V2 Backend")
    await close_hermes_adapter()
    logger.info("Hermes adapter closed")
    await close_db()
    logger.info("Database connections closed")


app = FastAPI(
    title="ODIN V2 API",
    description="Personal AI Assistant Backend",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(conversations.router, prefix="/api/conversations", tags=["Conversations"])
app.include_router(runs.router, prefix="/api", tags=["Runs"])
app.include_router(approvals.router, prefix="/api/approvals", tags=["Approvals"])
app.include_router(knowledge.router, prefix="/api/knowledge", tags=["Knowledge"])
app.include_router(system.router, prefix="/api/system", tags=["System"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "ODIN V2 API",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/api/health",
    }
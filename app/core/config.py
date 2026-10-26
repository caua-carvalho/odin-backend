"""ODIN V2 Backend — Application Settings"""

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    api_workers: int = 1

    # Database
    database_url: str = "sqlite+aiosqlite:///./odin.db"

    # Security
    secret_key: str = Field(..., min_length=32)
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24 * 7  # 7 days
    refresh_token_expire_days: int = 30

    # CORS (stored as comma-separated string in env, parsed via property)
    cors_origins_raw: str = "http://localhost:5173,http://127.0.0.1:5173,exp://192.168.1.0/24:8081,exp://localhost:8081"

    @property
    def cors_origins(self) -> List[str]:
        return [origin.strip() for origin in self.cors_origins_raw.split(",") if origin.strip()]

    # Hermes Integration
    hermes_api_url: str = "http://127.0.0.1:9119"
    hermes_session_token: Optional[str] = None
    hermes_ws_timeout: float = 30.0
    hermes_connect_timeout: float = 10.0
    hermes_run_timeout: float = 3600.0  # safety net per turn (seconds)

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"  # json or console

    # Rate limiting
    rate_limit_requests: int = 100
    rate_limit_window_seconds: int = 60
    auth_rate_limit_requests: int = 10
    auth_rate_limit_window_seconds: int = 60

    # Paths
    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent.parent)

    @property
    def database_path(self) -> Path:
        """Extract file path from SQLite URL."""
        if self.database_url.startswith("sqlite"):
            # sqlite+aiosqlite:///./odin.db -> ./odin.db
            path_str = self.database_url.replace("sqlite+aiosqlite:///", "").replace("sqlite:///", "")
            return (self.base_dir / path_str).resolve()
        return self.base_dir / "odin.db"


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
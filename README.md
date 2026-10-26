# ODIN V2 Backend

FastAPI backend for the ODIN V2 personal AI assistant.

## Quick Start

```bash
cd backend
cp ../.env.example .env
# Edit .env with your settings

# Install dependencies
pip install -e ".[dev]"

# Run development server
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## Requirements

- Python 3.12+
- Hermes Agent running on `http://127.0.0.1:9119` (or configured `HERMES_API_URL`)

## Environment Variables

See `.env.example` for all options.

Key variables:
- `SECRET_KEY` — JWT signing key (generate: `python -c "import secrets; print(secrets.token_urlsafe(32))"`)
- `HERMES_API_URL` — Hermes dashboard URL (default: `http://127.0.0.1:9119`)
- `DATABASE_URL` — SQLite by default, can use PostgreSQL

## API Documentation

- Swagger UI: http://127.0.0.1:8000/docs
- ReDoc: http://127.0.0.1:8000/redoc
- Health: http://127.0.0.1:8000/api/health

## Hermes Integration

The backend uses Hermes' structured `tui_gateway` JSON-RPC protocol over
WebSocket (`/api/ws`), multiplexed across conversations. See
`../docs/hermes-communication.md` for handshake, sessions, events, approvals,
queueing and reconnect behavior.

Hermes must be running with `hermes serve --port 9119 --host 127.0.0.1`.

The `HERMES_SESSION_TOKEN` is automatically read from the `HERMES_DASHBOARD_SESSION_TOKEN` environment variable when Hermes starts, or can be set manually.

## Project Structure

```
backend/
├── app/
│   ├── api/routes/          # API endpoints
│   ├── core/                # Config, database, security
│   ├── integrations/hermes/ # Hermes WebSocket adapter
│   ├── models/              # SQLAlchemy models
│   └── schemas/             # Pydantic schemas
├── tests/
└── pyproject.toml
```

## Testing

```bash
pytest -v
```

## Key Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/auth/login` | Login |
| `GET /api/conversations` | List conversations |
| `POST /api/conversations` | Create conversation |
| `POST /api/conversations/{id}/runs` | Start agent run (JSON 202) |
| `GET /api/runs/{id}` | Get run status |
| `GET /api/runs/{id}/events` | Replayable SSE stream |
| `WS /api/runs/{id}/ws` | WebSocket stream (mobile) |
| `GET /api/approvals` | List pending approvals |
| `POST /api/approvals/{id}/approve` | Approve operation |
| `GET /api/knowledge` | List knowledge items |
| `POST /api/knowledge` | Create knowledge item |
| `GET /api/system/status` | System health |
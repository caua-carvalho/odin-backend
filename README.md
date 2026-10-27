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

## Troubleshooting

### `POST /api/auth/login` returns 500

The most common cause on a fresh server is a bcrypt/passlib mismatch. Older
revisions used **passlib 1.7.4**, which is unmaintained and crashes with
**bcrypt >= 4.1/5.x**, raising `ValueError: password cannot be longer than 72
bytes` on every login. The backend now calls `bcrypt` directly and supports
both 4.x and 5.x.

Check the installed version on the server:

```bash
python -c "import bcrypt; print(bcrypt.__version__)"
```

Then reinstall the backend so the pinned dependency is applied:

```bash
pip install -e ".[dev]"
```

Other checks for a 500 on login (GETs work, but login writes to the database):

```bash
# The process running uvicorn must own/be able to write the SQLite file.
ls -l odin.db
sudo -u <service-user> test -w /path/to/backend/odin.db && echo writable

# SECRET_KEY must be at least 32 chars and .env must be in backend/.
grep -E '^(SECRET_KEY|DATABASE_URL|CORS_ORIGINS_RAW)=' .env

# Get the real traceback from the service manager.
journalctl -u odin -n 100 --no-pager   # or: systemctl --user status odin
```

Use an absolute `DATABASE_URL` in production so the database does not depend on
the working directory:

```env
DATABASE_URL=sqlite+aiosqlite:////opt/odin/odin.db
CORS_ORIGINS_RAW=https://odin.example.com
```

## Key Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/auth/login` | Login |
| `POST /api/auth/refresh` | Rotate refresh token / issue access token |
| `POST /api/auth/logout` | Revoke the current session |
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

### Native clients

Native clients send `X-Odin-Client: mobile`. Login and refresh then include the
refresh token in the JSON body (in addition to the HttpOnly cookie used by web),
and logout accepts that token in the request body. Tokens rotate on every
refresh; the previous session is revoked.

# Environment Rules

## Key Environment Variables
- `JOBMON__DB__SQLALCHEMY_DATABASE_URI` - Database connection
- `JOBMON__AUTH__ENABLED` - Set `false` for local development
- `UV_EXTRA_INDEX_URL` - Private package index (if needed)

## macOS Development
Set fork safety for parallel tests:
```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

## Docker
Use `docker compose` (not `docker-compose`) for local stack.

## Docker Development Stack

- Local dev uses **SQLite** at `jobmon_server/jobmon.db`, NOT PostgreSQL
- Do not use MCP Postgres tools for local dev queries
- Container names: `jobmon-jobmon_<service>-1` (e.g., `jobmon-jobmon_backend-1`)
- Backend API: `http://localhost:8070`, Frontend: `http://localhost:3000`
- `sqlite3` CLI is NOT installed in containers; use `python3 -c "import sqlite3; ..."`

## Docker Volume Mounts

- Source code is bind-mounted into containers
- After Python changes: `docker compose restart <service>` (NOT `--build`)
- Rebuild only for dependency or Dockerfile changes
- Frontend only mounts `./jobmon_gui/src:/app/src`; changes to `package.json`, `vite.config.ts` require `docker compose up -d --build jobmon_frontend`

## Alembic Migrations in Docker

Run from `/app` with explicit config:
```bash
docker exec -w /app jobmon-jobmon_backend-1 alembic -c /app/jobmon_server/src/jobmon/server/alembic.ini upgrade head
```

## Dev Workflow Scripts

- Located in `dev/workflows/` (NOT `tests/`)
- Bind-mounted to `/app/test_scripts/` in `jobmon_client` container
- Run: `docker compose exec jobmon_client python /app/test_scripts/<script>.py sequential`

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

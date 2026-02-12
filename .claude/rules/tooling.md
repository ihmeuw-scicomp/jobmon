# Tooling Rules

## Python Commands

- Always use `uv run` for Python commands (pytest, python, nox, etc.)
- Never use bare `python` or `pytest` - always prefix with `uv run`

## Code Style

- Line length: **95 characters** (configured in `.flake8` and `pyproject.toml`)

## Nox Sessions

Available sessions:
- `uv run nox -s tests` - Run test suite
- `uv run nox -s lint` - Lint check (flake8)
- `uv run nox -s format` - Auto-format (black, isort)
- `uv run nox -s typecheck` - Type checking (mypy)
- `uv run nox -s generate_api_types` - Generate TypeScript types from OpenAPI schema

## Frontend Commands

- EVERY `npm` command MUST include the full cd prefix:
  `cd /Users/mlsandar/repos/jobmon/jobmon_gui && npm run build`
- Shell working directory does NOT reliably persist between tool calls
- Run `npm run build` after every set of frontend changes (catches TypeScript errors)

## Generated Files

- `jobmon_gui/src/types/apiSchema.ts` is auto-generated - NEVER manually edit
- Full regeneration workflow: backend schema change → `docker compose restart jobmon_backend` → `uv run nox -s generate_api_types`
- `apiSchema.ts` is very large (~42K tokens) - use Grep to search specific types, never Read in full

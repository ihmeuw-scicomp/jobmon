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

- All npm/bun commands must run from `jobmon_gui/` directory
- Use `cd jobmon_gui && npm <command>` pattern

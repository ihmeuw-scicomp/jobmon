# Jobmon

Jobmon is a distributed workflow management system for scientific computing. It orchestrates task execution across HPC clusters (Slurm, SGE) and cloud environments.

## Project Structure

```
jobmon/
├── jobmon_client/    # Python client library for defining workflows
├── jobmon_core/      # Shared code (serializers, constants, exceptions)
├── jobmon_server/    # FastAPI REST API + SQLAlchemy models
└── jobmon_gui/       # React/TypeScript web interface
```

## Essential Commands

### Python (always use uv)
```bash
uv run pytest -n 10              # Run all tests (including e2e)
uv run python <script>           # Run any Python script
uv run nox -s lint               # Lint check
uv run nox -s format             # Auto-format code
uv run nox -s typecheck          # Type checking
```

### Frontend (run from jobmon_gui/)
```bash
cd jobmon_gui && npm install     # Install dependencies
cd jobmon_gui && npm run dev     # Start dev server
cd jobmon_gui && npm run build   # Production build
```

## Before Implementation

- Read relevant docs in `design/` directory
- Check existing patterns in `.flake8` and `pyproject.toml`
- See `.claude/rules/` for detailed conventions

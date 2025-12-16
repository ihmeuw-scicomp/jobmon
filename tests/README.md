# Jobmon Test Suite

This directory contains the pytest test suite for Jobmon, organized by test type.

## Directory Structure

```
tests/
├── conftest.py              # Root fixtures and pytest configuration
├── fixtures/                # Organized fixture modules
│   ├── database.py          # Database engine and session fixtures
│   ├── server.py            # Web server and client connection fixtures
│   └── workflows.py         # Tool, task template, and workflow fixtures
│
├── unit/                    # Pure unit tests (no server needed)
│   ├── core/                # Configuration, utilities, templates
│   ├── client/              # Client library unit tests
│   ├── logging/             # All logging configuration tests
│   ├── server/              # Server logic unit tests
│   └── swarm/               # Swarm component unit tests (mocked)
│
├── integration/             # Tests requiring server + database
│   ├── client/              # Workflow/task binding, arrays
│   ├── server/              # Database operations, routes
│   ├── distributor/         # Task instantiation, triaging
│   ├── swarm/               # Swarm execution integration
│   ├── reaper/              # Workflow cleanup
│   └── cli/                 # CLI command tests
│
└── e2e/                     # End-to-end workflow tests
```

## Test Categories

| Directory | Server Required | Database Required | Run Time |
|-----------|-----------------|-------------------|----------|
| `unit/` | ❌ No | ❌ No | ~16 seconds |
| `integration/` | ✅ Yes | ✅ Yes | ~3-5 minutes |
| `e2e/` | ✅ Yes | ✅ Yes | ~5+ minutes |

## Running Tests

```bash
# Fast feedback during development (~16 seconds)
pytest tests/unit/ -x

# Just logging tests
pytest tests/unit/logging/

# Integration tests
pytest tests/integration/

# End-to-end tests
pytest tests/e2e/

# Full suite with parallel workers (~8 minutes)
pytest tests/ -n 15

# Full suite sequential
pytest tests/
```

## Key Fixtures

### Database Fixtures (`fixtures/database.py`)

| Fixture | Scope | Description |
|---------|-------|-------------|
| `setup_test_environment` | session | Configures test environment, resets singletons |
| `db_engine` | session | Creates and initializes SQLite database |

### Server Fixtures (`fixtures/server.py`)

| Fixture | Scope | Description |
|---------|-------|-------------|
| `api_prefix` | session | Returns `/api/v3` |
| `web_server_process` | session | Starts Jobmon server in subprocess |
| `client_env` | function | Configures client to connect to test server |
| `requester_no_retry` | function | Client requester with no retry logic |

### Workflow Fixtures (`fixtures/workflows.py`)

| Fixture | Scope | Description |
|---------|-------|-------------|
| `tool` | function | Pre-configured Tool with task templates |
| `task_template` | function | The 'simple_template' from tool |
| `array_template` | function | The 'array_template' from tool |

## Test Counts

| Directory | Test Files | Approx. Tests |
|-----------|------------|---------------|
| `unit/core/` | 4 | ~25 |
| `unit/client/` | 1 | ~10 |
| `unit/logging/` | 14 | ~150 |
| `unit/server/` | 2 | ~15 |
| `unit/swarm/` | 7 | ~235 |
| `integration/client/` | 15 | ~180 |
| `integration/server/` | 7 | ~50 |
| `integration/distributor/` | 5 | ~20 |
| `integration/swarm/` | 3 | ~15 |
| `integration/reaper/` | 3 | ~10 |
| `integration/cli/` | 3 | ~45 |
| `e2e/` | 2 | ~11 |
| **Total** | **~66** | **~770** |

## Parallel Testing (pytest-xdist)

The test suite supports parallel execution with `pytest-xdist`. Each worker gets its own isolated SQLite database.

```bash
# Run with 15 workers
pytest tests/ -n 15

# Run with auto-detected worker count
pytest tests/ -n auto
```

**Note:** Some tests in `unit/swarm/test_builder.py` may be flaky with parallel execution due to mock state. Run them individually if they fail:

```bash
pytest tests/unit/swarm/test_builder.py -v
```

## Writing New Tests

### Unit Tests
- Place in `unit/{component}/`
- Should NOT require `client_env`, `db_engine`, or server fixtures
- Use mocking for external dependencies
- Should run in < 1 second each

### Integration Tests
- Place in `integration/{component}/`
- Use `client_env` and `db_engine` fixtures
- Test real component interactions
- May take several seconds each

### End-to-End Tests
- Place in `e2e/`
- Test complete workflow execution
- Use real distributors (sequential/multiprocess)
- May take minutes to complete

## Logging Tests

All logging tests are consolidated in `unit/logging/`:

| File | Description |
|------|-------------|
| `test_components.py` | Parameterized tests for all components (client, server, distributor, worker) |
| `test_component_logging.py` | Core `configure_component_logging` function |
| `test_logconfig_overrides.py` | Configuration override system |
| `test_otlp.py` | OTLP handlers and managers |
| `test_structlog_context.py` | Structlog context and telemetry isolation |
| `test_structlog_detection.py` | Structlog integration detection |
| `test_cross_component.py` | Cross-component logging scenarios |
| `test_client_logging.py` | Client-specific logging |
| `test_server_logging.py` | Server logging configuration |
| `test_server_otlp.py` | Server OTLP integration |
| `test_output_capture.py` | Stdout/stderr capture tests |
| `test_scheduler.py` | Scheduler logging |

## CI Integration

Recommended CI pipeline configuration:

```yaml
stages:
  - unit
  - integration
  - e2e

unit:
  stage: unit
  script: pytest tests/unit/ -x --tb=short
  # ~20 seconds

integration:
  stage: integration
  needs: [unit]
  script: pytest tests/integration/ -n 4 --tb=short
  # ~5 minutes

e2e:
  stage: e2e
  needs: [integration]
  script: pytest tests/e2e/ -v
  # ~5 minutes
```

---

*Last updated: December 2024*


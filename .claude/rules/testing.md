# Testing Rules

## Running Tests

- Always run the full test suite including e2e tests:
  ```bash
  uv run pytest -n 10
  ```
- Use `-n 10` for parallel execution
- Never exclude e2e tests unless explicitly asked

## Docker E2E Testing

- Six-job test is the definitive E2E validation after schema changes:
  ```bash
  docker compose exec jobmon_client python /app/test_scripts/six_job_test.py sequential
  ```
- Dev workflow scripts are in `dev/workflows/`, NOT in `tests/`
- Test helper scripts are in `tests/_scripts/`

## Test Fixtures

- Before writing new test fixtures, READ existing patterns in `tests/conftest.py`
- SQLAlchemy models have deep FK chains (Task requires Node, TaskTemplateVersion, Dag, etc.)
- Copy and adapt existing fixtures rather than building from scratch

## After Schema Changes

- Run database migrations before testing
- Check Docker backend logs for migration errors when tests fail
- Verify migrations work on clean database

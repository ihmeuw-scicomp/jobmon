# Testing Rules

## Running Tests

- Always run the full test suite including e2e tests:
  ```bash
  uv run pytest -n 10
  ```
- Use `-n 10` for parallel execution

## After Schema Changes

- Run database migrations before testing
- Verify migrations work on clean database

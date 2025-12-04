"""Integration test configuration.

Integration tests in this directory:
- Require server and database (use client_env, db_engine fixtures)
- Test component interactions and database operations
- May take longer to run than unit tests

Run with: pytest tests/pytest/integration/ --tb=short
"""

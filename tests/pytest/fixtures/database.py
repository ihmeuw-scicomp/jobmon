"""Database fixtures for Jobmon tests.

These fixtures handle database initialization and engine creation.
They are session-scoped and work correctly with pytest-xdist for parallel testing.

Key fixtures:
    setup_test_environment: Configures test environment and resets singletons
    db_engine: Creates and initializes the SQLite database engine
"""

import os
import pathlib

import pytest
from sqlalchemy.engine import Engine


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment - runs automatically at session start.

    Database URI and singletons are already configured in pytest_configure.
    This fixture ensures singletons are clean for the session and provides
    the database path for other fixtures.

    Yields:
        pathlib.Path: Path to the SQLite database file
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    db_uri = os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"]

    # Extract SQLite file path from URI (remove 'sqlite:////' prefix)
    sqlite_file = pathlib.Path(db_uri[11:])

    print(f"Worker {worker_id}: Setting up test environment")
    print(f"Worker {worker_id}: SQLite file at {sqlite_file}")

    # Reset singletons for clean test state (may have been set in pytest_configure,
    # but we reset again here to ensure clean state at fixture resolution time)
    import jobmon.server.web.config as config_module

    config_module._jobmon_config = None

    import jobmon.server.web.db.engine as engine_module

    engine_module._engine = None
    engine_module._SessionMaker = None

    print(f"Worker {worker_id}: Environment setup complete")
    yield sqlite_file
    print(f"Worker {worker_id}: Environment teardown")


@pytest.fixture(scope="session")
def db_engine(setup_test_environment) -> Engine:
    """Initialize and return the database engine.

    Creates an SQLite database with all Jobmon tables initialized.
    Each pytest-xdist worker gets its own isolated database.

    CRITICAL: Resets singletons before initialization to prevent stale
    references from earlier fixture resolution or other workers.

    Args:
        setup_test_environment: The test environment fixture (provides db path)

    Returns:
        Engine: SQLAlchemy engine connected to the test database

    Raises:
        AssertionError: If database URI is not SQLite or tables not initialized
    """
    import jobmon.server.web.config as config_module
    import jobmon.server.web.db.engine as engine_module
    from jobmon.server.web.config import get_jobmon_config
    from jobmon.server.web.db import get_engine, init_db

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")

    # CRITICAL: Reset singletons to prevent stale references
    # This is necessary because fixtures might run in unexpected order
    # with pytest-xdist, and singletons could be initialized with wrong values
    config_module._jobmon_config = None
    engine_module._engine = None
    engine_module._SessionMaker = None

    config = get_jobmon_config()
    db_uri = config.get("db", "sqlalchemy_database_uri")
    assert "sqlite" in db_uri, f"Expected SQLite URI but got: {db_uri}"

    # Initialize database - each worker has its own, so no race conditions
    init_db()

    # Create and return engine
    eng = get_engine()  # Use the configured engine with WAL mode

    # Verify database has expected tables
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    with Session(eng) as session:
        res = session.execute(text("SELECT * from workflow_status")).fetchall()
        assert len(res) > 0, f"Worker {worker_id}: Database not properly initialized"

    print(f"Worker {worker_id}: Database ready with {len(res)} workflow statuses")
    return eng

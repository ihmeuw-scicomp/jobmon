"""Database fixtures for Jobmon tests.

These fixtures handle database initialization and engine creation.
They are session-scoped and work correctly with pytest-xdist for parallel testing.

Key fixtures:
    setup_test_environment: Configures test environment
    db_engine: Creates and initializes the SQLite database engine
    dbsession: Provides a transactional session for tests
"""

import os
import pathlib

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment - runs automatically at session start.

    Database URI is already configured in pytest_configure.
    This fixture provides the database path for other fixtures.

    Yields:
        pathlib.Path: Path to the SQLite database file
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    db_uri = os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"]

    # Extract SQLite file path from URI (remove 'sqlite:////' prefix)
    sqlite_file = pathlib.Path(db_uri[11:])

    print(f"Worker {worker_id}: Setting up test environment")
    print(f"Worker {worker_id}: SQLite file at {sqlite_file}")
    print(f"Worker {worker_id}: Environment setup complete")

    yield sqlite_file

    print(f"Worker {worker_id}: Environment teardown")


@pytest.fixture(scope="session")
def db_engine(setup_test_environment) -> Engine:
    """Initialize and return the database engine.

    Creates an SQLite database with all Jobmon tables initialized.
    Each pytest-xdist worker gets its own isolated database.

    Args:
        setup_test_environment: The test environment fixture (provides db path)

    Returns:
        Engine: SQLAlchemy engine connected to the test database

    Raises:
        AssertionError: If database URI is not SQLite or tables not initialized
    """
    from jobmon.core.configuration import JobmonConfig
    from jobmon.server.web.db import init_db

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")

    # Get config and verify it's SQLite
    config = JobmonConfig()
    db_uri = config.get("db", "sqlalchemy_database_uri")
    assert "sqlite" in db_uri, f"Expected SQLite URI but got: {db_uri}"

    # Initialize database - each worker has its own, so no race conditions
    init_db()

    # Create engine directly (no singleton)
    engine = create_engine(db_uri)

    # Verify database has expected tables
    from sqlalchemy import text

    with Session(engine) as session:
        res = session.execute(text("SELECT * from workflow_status")).fetchall()
        assert len(res) > 0, f"Worker {worker_id}: Database not properly initialized"

    print(f"Worker {worker_id}: Database ready with {len(res)} workflow statuses")
    yield engine

    # Cleanup: dispose of engine
    engine.dispose()


@pytest.fixture(scope="function")
def dbsession(db_engine: Engine):
    """Provides a transactional SQLAlchemy Session for tests.

    Each test gets its own session with a transaction that is rolled back
    after the test completes, ensuring test isolation.

    Args:
        db_engine: The database engine fixture

    Yields:
        Session: A SQLAlchemy session bound to a transaction
    """
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()

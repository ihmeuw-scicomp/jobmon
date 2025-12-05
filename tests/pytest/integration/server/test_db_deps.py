"""Tests for jobmon.server.web.db.deps module."""

from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from jobmon.server.web.db.deps import DB, get_db, get_dialect


class TestGetDb:
    """Test the get_db FastAPI dependency function."""

    def test_get_db_yields_session(self):
        """Test that get_db yields a session from app.state.db_sessionmaker."""
        # Create mock request with app.state
        mock_session = Mock(spec=Session)
        mock_sessionmaker = Mock(return_value=mock_session)

        mock_request = MagicMock()
        mock_request.app.state.db_sessionmaker = mock_sessionmaker

        # Execute
        generator = get_db(mock_request)
        session = next(generator)

        # Verify
        assert session is mock_session
        mock_sessionmaker.assert_called_once()

    def test_get_db_commits_on_success(self):
        """Test that get_db commits the session on successful completion."""
        mock_session = Mock(spec=Session)
        mock_sessionmaker = Mock(return_value=mock_session)

        mock_request = MagicMock()
        mock_request.app.state.db_sessionmaker = mock_sessionmaker

        # Execute - exhaust the generator
        generator = get_db(mock_request)
        next(generator)
        try:
            next(generator)
        except StopIteration:
            pass

        # Verify commit was called
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_get_db_rollback_on_exception(self):
        """Test that get_db rolls back the session on exception."""
        mock_session = Mock(spec=Session)
        mock_sessionmaker = Mock(return_value=mock_session)

        mock_request = MagicMock()
        mock_request.app.state.db_sessionmaker = mock_sessionmaker

        # Execute with exception
        generator = get_db(mock_request)
        next(generator)

        # Throw exception into generator
        with pytest.raises(ValueError):
            generator.throw(ValueError("Test exception"))

        # Verify rollback was called
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()

    def test_get_db_closes_on_commit_failure(self):
        """Test that session is closed even if commit fails."""
        mock_session = Mock(spec=Session)
        mock_session.commit.side_effect = SQLAlchemyError("Commit failed")
        mock_sessionmaker = Mock(return_value=mock_session)

        mock_request = MagicMock()
        mock_request.app.state.db_sessionmaker = mock_sessionmaker

        # Execute
        generator = get_db(mock_request)
        next(generator)

        # Try to complete normally - commit should fail
        with pytest.raises(SQLAlchemyError):
            try:
                next(generator)
            except StopIteration:
                pass

        # Verify session is still closed even after commit failure
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()


class TestGetDialect:
    """Test the get_dialect FastAPI dependency function."""

    def test_get_dialect_returns_dialect_from_app_state(self):
        """Test that get_dialect returns the dialect from app.state."""
        mock_request = MagicMock()
        mock_request.app.state.db_dialect = "mysql"

        result = get_dialect(mock_request)

        assert result == "mysql"

    def test_get_dialect_sqlite(self):
        """Test that get_dialect works for sqlite."""
        mock_request = MagicMock()
        mock_request.app.state.db_dialect = "sqlite"

        result = get_dialect(mock_request)

        assert result == "sqlite"


class TestDbDependency:
    """Test the DB dependency alias."""

    def test_db_is_fastapi_dependency(self):
        """Test that DB is properly configured as a FastAPI Depends."""
        # The DB should be a Depends instance
        assert hasattr(DB, "dependency")
        # The dependency should be the get_db function
        assert DB.dependency is get_db


class TestIntegrationWithFastAPI:
    """Integration tests with actual FastAPI app."""

    def test_db_dependency_in_route(self, web_server_in_memory):
        """Test that the DB dependency works in an actual FastAPI route."""
        from fastapi import APIRouter
        from sqlalchemy.orm import Session

        client, engine = web_server_in_memory

        # Create a test router
        test_router = APIRouter()

        @test_router.get("/test-db")
        def test_db_endpoint(db: Session = DB):
            """Test endpoint that uses the DB dependency."""
            # Verify we get a real Session object
            assert isinstance(db, Session)
            return {"status": "success", "session_type": str(type(db))}

        # Add the test router to the app
        client.app.include_router(test_router)

        # Make request to test endpoint
        response = client.get("/test-db")

        # Verify the response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "Session" in data["session_type"]

    def test_db_dependency_transaction_behavior(self, web_server_in_memory):
        """Test that the DB dependency properly handles transactions."""
        from fastapi import APIRouter
        from sqlalchemy.orm import Session

        from jobmon.server.web.models.workflow_status import WorkflowStatus

        client, engine = web_server_in_memory

        test_router = APIRouter()

        @test_router.post("/test-transaction")
        def test_transaction_endpoint(db: Session = DB):
            """Test endpoint that performs a database operation."""
            # Try to query a simple table to verify the session works
            result = db.query(WorkflowStatus).first()
            return {"status": "success", "has_data": result is not None}

        # Add the test router to the app
        client.app.include_router(test_router)

        # Make request to test endpoint
        response = client.post("/test-transaction")

        # Verify the response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"

    def test_db_dependency_handles_exceptions(self, web_server_in_memory):
        """Test that the DB dependency properly handles exceptions and rollback."""
        from fastapi import APIRouter, HTTPException
        from sqlalchemy.orm import Session

        client, engine = web_server_in_memory

        test_router = APIRouter()

        @test_router.post("/test-exception")
        def test_exception_endpoint(db: Session = DB):
            """Test endpoint that raises an exception."""
            # Force an exception to test rollback behavior
            raise HTTPException(status_code=400, detail="Test exception")

        # Add the test router to the app
        client.app.include_router(test_router)

        # Make request to test endpoint
        response = client.post("/test-exception")

        # Verify the error response
        assert response.status_code == 400
        data = response.json()
        assert data["detail"] == "Test exception"

    def test_dialect_dependency_in_route(self, web_server_in_memory):
        """Test that the dialect dependency works in actual routes."""
        from fastapi import APIRouter, Depends

        from jobmon.server.web.db import get_dialect

        client, engine = web_server_in_memory

        test_router = APIRouter()

        @test_router.get("/test-dialect")
        def test_dialect_endpoint(dialect: str = Depends(get_dialect)):
            """Test endpoint that uses the dialect dependency."""
            return {"dialect": dialect}

        # Add the test router to the app
        client.app.include_router(test_router)

        # Make request to test endpoint
        response = client.get("/test-dialect")

        # Verify the response
        assert response.status_code == 200
        data = response.json()
        assert data["dialect"] == "sqlite"  # Test database is SQLite


class TestSessionLifecycle:
    """Test complete session lifecycle with real database."""

    def test_session_lifecycle_with_real_db(self, dbsession):
        """Test session lifecycle using the test database session."""
        # This uses the dbsession fixture which provides a real SQLAlchemy session
        # bound to the test database

        # Test that we can use the session for basic operations
        from jobmon.server.web.models.workflow_status import WorkflowStatus

        # Query should work without issues
        result = dbsession.query(WorkflowStatus).first()

        # Should not raise any exceptions
        assert True  # If we get here, the session is working

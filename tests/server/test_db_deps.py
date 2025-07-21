"""Tests for jobmon.server.web.db.deps module."""

from unittest.mock import Mock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from jobmon.server.web.db.deps import DB, _session_scope, get_db


class TestSessionScope:
    """Test the _session_scope context manager."""

    def test_session_scope_success(self, db_engine):
        """Test that session_scope properly commits and closes on success."""
        # Setup
        with patch(
            "jobmon.server.web.db.deps.get_sessionmaker"
        ) as mock_get_sessionmaker:
            mock_session = Mock(spec=Session)
            mock_sessionmaker = Mock()
            mock_sessionmaker.return_value = mock_session
            mock_get_sessionmaker.return_value = mock_sessionmaker

            # Execute
            with _session_scope() as session:
                assert session is mock_session
                # Simulate some database operation

            # Verify
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()
            mock_session.rollback.assert_not_called()

    def test_session_scope_exception_rollback(self, db_engine):
        """Test that session_scope properly rolls back on exception."""
        with patch(
            "jobmon.server.web.db.deps.get_sessionmaker"
        ) as mock_get_sessionmaker:
            mock_session = Mock(spec=Session)
            mock_sessionmaker = Mock()
            mock_sessionmaker.return_value = mock_session
            mock_get_sessionmaker.return_value = mock_sessionmaker

            # Execute with exception
            with pytest.raises(ValueError):
                with _session_scope() as session:
                    assert session is mock_session
                    raise ValueError("Test exception")

            # Verify
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()
            mock_session.commit.assert_not_called()

    def test_session_scope_commit_exception_still_closes(self, db_engine):
        """Test that session is closed even if commit fails."""
        with patch(
            "jobmon.server.web.db.deps.get_sessionmaker"
        ) as mock_get_sessionmaker:
            mock_session = Mock(spec=Session)
            mock_session.commit.side_effect = SQLAlchemyError("Commit failed")
            mock_sessionmaker = Mock()
            mock_sessionmaker.return_value = mock_session
            mock_get_sessionmaker.return_value = mock_sessionmaker

            # Execute - should raise the commit exception
            with pytest.raises(SQLAlchemyError):
                with _session_scope() as session:
                    pass

            # Verify session is still closed even after commit failure
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()


class TestGetDb:
    """Test the get_db FastAPI dependency function."""

    def test_get_db_yields_session(self, db_engine):
        """Test that get_db yields a session from the context manager."""
        with patch("jobmon.server.web.db.deps._session_scope") as mock_session_scope:
            mock_session = Mock(spec=Session)
            mock_session_scope.return_value.__enter__.return_value = mock_session
            mock_session_scope.return_value.__exit__.return_value = None

            # Execute
            generator = get_db()
            session = next(generator)

            # Verify
            assert session is mock_session
            mock_session_scope.assert_called_once()

            # Test that the generator is exhausted (only yields once)
            with pytest.raises(StopIteration):
                next(generator)

    def test_get_db_exception_propagation(self, db_engine):
        """Test that exceptions from session_scope are properly propagated."""
        with patch("jobmon.server.web.db.deps._session_scope") as mock_session_scope:
            mock_session_scope.side_effect = SQLAlchemyError("Connection failed")

            # Execute
            generator = get_db()

            # Verify exception is propagated
            with pytest.raises(SQLAlchemyError):
                next(generator)


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

    def test_get_db_with_real_sessionmaker(self, db_engine):
        """Test get_db using the actual sessionmaker (not mocked)."""
        # This test uses the real get_sessionmaker function
        generator = get_db()

        try:
            session = next(generator)

            # Verify we get a real Session
            assert isinstance(session, Session)

            # Verify the session is usable
            assert session.is_active

        except StopIteration:
            pytest.fail("get_db should yield a session")
        finally:
            # Clean up the generator
            try:
                next(generator)
            except StopIteration:
                pass  # Expected

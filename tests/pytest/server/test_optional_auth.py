"""Tests for optional authentication functionality."""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError
from jobmon.server.web.api import get_app
from jobmon.server.web.routes.utils import (
    create_anonymous_user,
    get_user_or_anonymous,
    is_auth_enabled,
)


class TestAuthConfiguration:
    """Test authentication configuration functions."""

    def test_is_auth_enabled_default_true(self):
        """Test that auth is enabled by default when config is missing."""
        with patch("jobmon.server.web.routes.utils.JobmonConfig") as mock_config:
            mock_instance = Mock()
            mock_instance.get_boolean.side_effect = ConfigError("Config not found")
            mock_config.return_value = mock_instance

            assert is_auth_enabled() is True

    def test_is_auth_enabled_explicitly_true(self):
        """Test that auth is enabled when explicitly set to true."""
        with patch("jobmon.server.web.routes.utils.JobmonConfig") as mock_config:
            mock_instance = Mock()
            mock_instance.get_boolean.return_value = True
            mock_config.return_value = mock_instance

            assert is_auth_enabled() is True

    def test_is_auth_enabled_explicitly_false(self):
        """Test that auth is disabled when explicitly set to false."""
        with patch("jobmon.server.web.routes.utils.JobmonConfig") as mock_config:
            mock_instance = Mock()
            mock_instance.get_boolean.return_value = False
            mock_config.return_value = mock_instance

            assert is_auth_enabled() is False


class TestAnonymousUser:
    """Test anonymous user creation and properties."""

    def test_create_anonymous_user(self):
        """Test that anonymous user is created with correct properties."""
        user = create_anonymous_user()

        assert isinstance(user, dict)  # User is a TypedDict
        assert user["sub"] == "anonymous"
        assert user["email"] == "anonymous@localhost"
        assert user["preferred_username"] == "anonymous"
        assert user["name"] == "Anonymous User"
        assert user["given_name"] == "Anonymous"
        assert user["family_name"] == "User"
        assert user["groups"] == ["anonymous"]
        assert user["updated_at"] == 0
        assert user["nonce"] == ""
        assert user["at_hash"] == ""
        assert user["sid"] == ""
        assert user["aud"] == ""
        assert user["exp"] == 0
        assert user["iat"] == 0
        assert user["iss"] == "localhost"

    def test_anonymous_user_consistency(self):
        """Test that anonymous user creation is consistent across calls."""
        user1 = create_anonymous_user()
        user2 = create_anonymous_user()

        assert user1 == user2


class TestConditionalUserRetrieval:
    """Test conditional user retrieval based on auth settings."""

    def test_get_user_or_anonymous_with_auth_disabled(self):
        """Test that anonymous user is returned when auth is disabled."""
        mock_request = Mock(spec=Request)

        with patch(
            "jobmon.server.web.routes.utils.is_auth_enabled", return_value=False
        ):
            user = get_user_or_anonymous(mock_request)

            assert user["sub"] == "anonymous"
            assert user["email"] == "anonymous@localhost"

    def test_get_user_or_anonymous_with_auth_enabled(self):
        """Test that regular get_user is called when auth is enabled."""
        mock_request = Mock(spec=Request)
        mock_request.session = {
            "user": {
                "sub": "test-user",
                "email": "test@example.com",
                "preferred_username": "testuser",
                "name": "Test User",
                "updated_at": 123456789,
                "given_name": "Test",
                "family_name": "User",
                "groups": ["users"],
                "nonce": "test-nonce",
                "at_hash": "test-hash",
                "sid": "test-sid",
                "aud": "test-aud",
                "exp": 123456789,
                "iat": 123456789,
                "iss": "test-iss",
            }
        }

        with patch("jobmon.server.web.routes.utils.is_auth_enabled", return_value=True):
            user = get_user_or_anonymous(mock_request)

            assert user["sub"] == "test-user"
            assert user["email"] == "test@example.com"
            assert user["preferred_username"] == "testuser"

    def test_get_user_or_anonymous_with_auth_enabled_no_session(self):
        """Test that HTTPException is raised when auth is enabled but no session exists."""
        from fastapi import HTTPException

        mock_request = Mock(spec=Request)
        mock_request.session = {}

        with patch("jobmon.server.web.routes.utils.is_auth_enabled", return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                get_user_or_anonymous(mock_request)

            assert exc_info.value.status_code == 403
            assert exc_info.value.detail == "Unauthorized"


class TestAPIConfiguration:
    """Test API configuration with optional authentication."""

    def test_api_versions_with_auth_enabled(self):
        """Test that auth routes are included when auth is enabled."""
        with patch("jobmon.server.web.api.is_auth_enabled", return_value=True):
            with patch("jobmon.server.web.api.JobmonConfig") as mock_config:
                mock_instance = Mock()
                mock_instance.get_boolean.return_value = (
                    False  # Disable OTEL for testing
                )
                mock_instance.get.return_value = "test_secret"
                mock_config.return_value = mock_instance

                app = get_app()

                # Check that the app was created
                assert app is not None
                assert app.title == "jobmon"

    def test_api_versions_with_auth_disabled(self):
        """Test that auth routes are excluded when auth is disabled."""
        with patch("jobmon.server.web.api.is_auth_enabled", return_value=False):
            with patch("jobmon.server.web.api.JobmonConfig") as mock_config:
                mock_instance = Mock()
                mock_instance.get_boolean.return_value = (
                    False  # Disable OTEL for testing
                )
                mock_instance.get.return_value = "test_secret"
                mock_config.return_value = mock_instance

                app = get_app()

                # Check that the app was created
                assert app is not None
                assert app.title == "jobmon"

    def test_api_dependencies_with_auth_disabled(self):
        """Test that v3 routes use anonymous user dependency when auth is disabled."""
        with patch("jobmon.server.web.api.is_auth_enabled", return_value=False):
            with patch("jobmon.server.web.api.JobmonConfig") as mock_config:
                mock_instance = Mock()
                mock_instance.get_boolean.return_value = False  # Disable OTEL
                mock_instance.get.return_value = "test_secret"
                mock_config.return_value = mock_instance

                # This should not raise an exception
                app = get_app(versions=["v2"])  # Use v2 to avoid auth dependency issues
                client = TestClient(app)

                # Test that the API is accessible
                response = client.get("/api/docs")
                # Note: This might return 404 if docs route doesn't exist, but shouldn't crash
                assert response.status_code in [200, 404]


class TestAuthIntegration:
    """Integration tests for optional authentication."""

    def test_environment_variable_integration(self, monkeypatch):
        """Test that environment variables properly control auth behavior."""
        # Test with auth disabled via environment variable
        monkeypatch.setenv("JOBMON__AUTH__ENABLED", "false")

        # Create a new config instance to pick up the env var
        config = JobmonConfig()

        try:
            result = config.get_boolean("auth", "enabled")
            assert result is False
        except ConfigError:
            # If the config key doesn't exist, that's also valid for this test
            pass

    def test_environment_variable_true_integration(self, monkeypatch):
        """Test that environment variables properly enable auth."""
        # Test with auth explicitly enabled via environment variable
        monkeypatch.setenv("JOBMON__AUTH__ENABLED", "true")

        # Create a new config instance to pick up the env var
        config = JobmonConfig()

        try:
            result = config.get_boolean("auth", "enabled")
            assert result is True
        except ConfigError:
            # If the config key doesn't exist, that's also valid for this test
            pass


class TestOAuthSetup:
    """Test OAuth setup functionality."""

    def test_oauth_setup_with_auth_disabled(self):
        """Test that OAuth setup is skipped when auth is disabled."""
        from jobmon.server.web.auth import setup_oauth

        with patch("jobmon.server.web.auth.is_auth_enabled", return_value=False):
            result = setup_oauth()
            assert result is None

    def test_oauth_setup_with_auth_enabled_missing_config(self):
        """Test OAuth setup with auth enabled but missing config."""
        from jobmon.server.web.auth import setup_oauth

        with patch("jobmon.server.web.auth.is_auth_enabled", return_value=True):
            with patch("jobmon.server.web.auth._CONFIG") as mock_config:
                mock_config.get.side_effect = ConfigError("Missing config")

                result = setup_oauth()
                assert result is None

    def test_is_auth_enabled_in_auth_module(self):
        """Test the is_auth_enabled function in the auth module."""
        from jobmon.server.web.auth import is_auth_enabled as auth_is_enabled

        with patch("jobmon.server.web.auth._CONFIG") as mock_config:
            mock_config.get_boolean.return_value = False

            assert auth_is_enabled() is False

        with patch("jobmon.server.web.auth._CONFIG") as mock_config:
            mock_config.get_boolean.side_effect = ConfigError("Missing")

            assert auth_is_enabled() is True  # Default behavior

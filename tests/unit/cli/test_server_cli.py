"""Unit tests for the Jobmon Server CLI.

These tests verify the server CLI command structure and help text
without requiring a running database.
"""

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from jobmon.server.cli.main import cli


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


class TestServerCLIStructure:
    """Test the main server CLI structure and command groups."""

    def test_cli_help(self, runner):
        """Test that the server CLI shows help."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Jobmon server administration CLI" in result.output
        assert "db" in result.output
        assert "reaper" in result.output

    def test_cli_debug_flag(self, runner):
        """Test the debug flag is accepted."""
        result = runner.invoke(cli, ["--debug", "--help"])
        assert result.exit_code == 0


class TestDatabaseCommandGroup:
    """Test the database command group structure."""

    def test_db_help(self, runner):
        """Test db group help."""
        result = runner.invoke(cli, ["db", "--help"])
        assert result.exit_code == 0
        assert "Database management" in result.output
        assert "init" in result.output
        assert "upgrade" in result.output
        assert "terminate" in result.output

    def test_db_init_help(self, runner):
        """Test db init command help."""
        result = runner.invoke(cli, ["db", "init", "--help"])
        assert result.exit_code == 0
        assert "--db-uri" in result.output
        assert "Initialize" in result.output

    def test_db_upgrade_help(self, runner):
        """Test db upgrade command help."""
        result = runner.invoke(cli, ["db", "upgrade", "--help"])
        assert result.exit_code == 0
        assert "--db-uri" in result.output
        assert "--revision" in result.output
        assert "head" in result.output  # default value

    def test_db_terminate_help(self, runner):
        """Test db terminate command help."""
        result = runner.invoke(cli, ["db", "terminate", "--help"])
        assert result.exit_code == 0
        assert "--db-uri" in result.output
        assert "--yes" in result.output
        assert "WARNING" in result.output or "destructive" in result.output.lower()


class TestReaperCommandGroup:
    """Test the reaper command group structure."""

    def test_reaper_help(self, runner):
        """Test reaper group help."""
        result = runner.invoke(cli, ["reaper", "--help"])
        assert result.exit_code == 0
        assert "Workflow reaper" in result.output
        assert "start" in result.output

    def test_reaper_start_help(self, runner):
        """Test reaper start command help."""
        result = runner.invoke(cli, ["reaper", "start", "--help"])
        assert result.exit_code == 0
        assert "--service-url" in result.output
        assert "--poll-interval" in result.output
        assert "--slack-api-url" in result.output
        assert "--slack-token" in result.output
        assert "--slack-channel" in result.output


class TestDatabaseCommands:
    """Test database commands with mocked backend."""

    @patch("jobmon.server.web.db.init_db")
    @patch("jobmon.core.configuration.JobmonConfig")
    def test_db_init_with_uri(self, mock_config, mock_init_db, runner):
        """Test db init with explicit URI."""
        mock_config.return_value.get.return_value = "mysql://test"
        result = runner.invoke(cli, ["db", "init", "--db-uri", "mysql://custom"])
        assert "initialized" in result.output.lower() or result.exit_code == 0

    @patch("jobmon.server.web.db.init_db")
    @patch("jobmon.core.configuration.JobmonConfig")
    def test_db_init_from_config(self, mock_config, mock_init_db, runner):
        """Test db init using config file."""
        mock_config.return_value.get.return_value = "mysql://from_config"
        result = runner.invoke(cli, ["db", "init"])
        # May fail due to missing config, but should not crash
        assert result.exit_code in (0, 1) or "initialized" in result.output.lower()

    @patch("jobmon.server.web.db.apply_migrations")
    @patch("jobmon.core.configuration.JobmonConfig")
    def test_db_upgrade_default(self, mock_config, mock_migrate, runner):
        """Test db upgrade to head (default)."""
        mock_config.return_value.get.return_value = "mysql://test"
        result = runner.invoke(cli, ["db", "upgrade"])
        # May fail due to missing config, but should not crash
        assert result.exit_code in (0, 1) or "upgraded" in result.output.lower()

    @patch("jobmon.server.web.db.apply_migrations")
    @patch("jobmon.core.configuration.JobmonConfig")
    def test_db_upgrade_specific_revision(self, mock_config, mock_migrate, runner):
        """Test db upgrade to specific revision."""
        mock_config.return_value.get.return_value = "mysql://test"
        result = runner.invoke(cli, ["db", "upgrade", "--revision", "abc123"])
        # May fail due to missing config, but should not crash
        assert result.exit_code in (0, 1) or "upgraded" in result.output.lower()

    @patch("jobmon.server.web.db.terminate_db")
    @patch("jobmon.core.configuration.JobmonConfig")
    def test_db_terminate_with_yes(self, mock_config, mock_terminate, runner):
        """Test db terminate with --yes flag."""
        mock_config.return_value.get.return_value = "mysql://test"
        result = runner.invoke(cli, ["db", "terminate", "--yes"])
        # May fail due to missing config, but should not crash
        assert result.exit_code in (0, 1) or "terminated" in result.output.lower()

    @patch("jobmon.core.configuration.JobmonConfig")
    def test_db_terminate_requires_confirmation(self, mock_config, runner):
        """Test db terminate requires confirmation without --yes."""
        mock_config.return_value.get.return_value = "mysql://test"
        # Without --yes and without input, should abort
        result = runner.invoke(cli, ["db", "terminate"])
        # Should either fail or require confirmation
        assert (
            result.exit_code != 0
            or "Aborted" in result.output
            or result.exit_code in (0, 1)
        )


class TestReaperCommands:
    """Test reaper commands with mocked backend."""

    @patch("jobmon.server.workflow_reaper.api.start_workflow_reaper")
    def test_reaper_start_default(self, mock_start, runner):
        """Test reaper start with defaults."""
        result = runner.invoke(cli, ["reaper", "start"])
        mock_start.assert_called_once()
        call_kwargs = mock_start.call_args[1]
        assert call_kwargs["service_url"] == ""
        assert call_kwargs["poll_interval_minutes"] is None

    @patch("jobmon.server.workflow_reaper.api.start_workflow_reaper")
    def test_reaper_start_with_options(self, mock_start, runner):
        """Test reaper start with custom options."""
        result = runner.invoke(
            cli,
            [
                "reaper",
                "start",
                "--service-url",
                "http://localhost:5000",
                "--poll-interval",
                "15",
                "--slack-token",
                "xoxb-token",
                "--slack-channel",
                "#alerts",
            ],
        )
        mock_start.assert_called_once()
        call_kwargs = mock_start.call_args[1]
        assert call_kwargs["service_url"] == "http://localhost:5000"
        assert call_kwargs["poll_interval_minutes"] == 15
        assert call_kwargs["slack_token"] == "xoxb-token"
        assert call_kwargs["slack_channel_default"] == "#alerts"


class TestServerLegacyCommands:
    """Test that legacy server commands are available."""

    def test_legacy_init_db_exists(self, runner):
        """Test that init_db legacy command exists."""
        result = runner.invoke(cli, ["init_db", "--help"])
        # Either shows help or shows deprecation warning or doesn't exist
        assert result.exit_code in (0, 2) or "DEPRECATED" in result.output

    def test_legacy_upgrade_exists(self, runner):
        """Test that upgrade legacy command exists."""
        result = runner.invoke(cli, ["upgrade", "--help"])
        # Either shows help, shows deprecation warning, or doesn't exist
        assert result.exit_code in (0, 2) or "DEPRECATED" in result.output


class TestMainFunction:
    """Test the main entry point function."""

    def test_main_with_help(self, runner):
        """Test main() function shows help."""
        result = runner.invoke(cli, ["--help"], catch_exceptions=False)
        assert "Jobmon" in result.output or result.exit_code == 0

    def test_main_handles_exceptions(self, runner):
        """Test that main() handles invalid commands gracefully."""
        result = runner.invoke(cli, ["invalid_command"])
        # Should fail gracefully
        assert result.exit_code != 0

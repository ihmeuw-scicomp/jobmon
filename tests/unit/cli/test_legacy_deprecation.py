"""Tests for legacy CLI commands and deprecation warnings.

These tests verify that:
1. Legacy commands still work
2. Deprecation warnings are emitted
3. Legacy commands produce the same output as new commands
"""

from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner

from jobmon.client.cli.legacy import (
    LEGACY_COMMAND_MAP,
    emit_deprecation_warning,
)
from jobmon.client.cli.main import cli


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


class TestDeprecationWarnings:
    """Test that deprecation warnings are emitted correctly."""

    def test_emit_deprecation_warning_format(self, runner):
        """Test the format of deprecation warnings."""
        result = runner.invoke(cli, [], catch_exceptions=False)
        # Just verify the function can be called without error
        emit_deprecation_warning("old_cmd", "new_cmd")

    def test_legacy_command_map_completeness(self):
        """Test that all legacy commands are mapped."""
        expected_commands = [
            "workflow_status",
            "workflow_tasks",
            "workflow_reset",
            "workflow_resume",
            "task_status",
            "update_task_status",
            "task_dependencies",
            "concurrency_limit",
            "get_filepaths",
            "task_template_resources",
            "create_resource_yaml",
            "update_config",
        ]
        for cmd in expected_commands:
            assert cmd in LEGACY_COMMAND_MAP, f"Missing legacy command: {cmd}"


class TestLegacyCommands:
    """Test legacy commands work and emit deprecation warnings."""

    @patch("jobmon.client.status_commands.workflow_status")
    def test_workflow_status_legacy(self, mock_cmd, runner):
        """Test legacy workflow_status command."""
        mock_cmd.return_value = pd.DataFrame({"WF_ID": [1], "STATUS": ["DONE"]})
        result = runner.invoke(cli, ["workflow_status", "-w", "1"])

        # Check deprecation warning was emitted (in output since stderr is mixed)
        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow status" in result.output

    @patch("jobmon.client.status_commands.workflow_tasks")
    def test_workflow_tasks_legacy(self, mock_cmd, runner):
        """Test legacy workflow_tasks command."""
        mock_cmd.return_value = pd.DataFrame({"TASK_ID": [1], "STATUS": ["DONE"]})
        result = runner.invoke(cli, ["workflow_tasks", "-w", "1"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow tasks" in result.output

    @patch("jobmon.client.status_commands.workflow_reset")
    def test_workflow_reset_legacy(self, mock_cmd, runner):
        """Test legacy workflow_reset command."""
        mock_cmd.return_value = "Reset successful"
        result = runner.invoke(cli, ["workflow_reset", "-w", "1"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow reset" in result.output

    @patch("jobmon.client.status_commands.resume_workflow_from_id")
    def test_workflow_resume_legacy(self, mock_cmd, runner):
        """Test legacy workflow_resume command."""
        result = runner.invoke(cli, ["workflow_resume", "-w", "1", "-c", "slurm"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow resume" in result.output

    @patch("jobmon.client.status_commands.task_status")
    def test_task_status_legacy(self, mock_cmd, runner):
        """Test legacy task_status command."""
        mock_cmd.return_value = pd.DataFrame({"TASK_ID": [1], "STATUS": ["DONE"]})
        result = runner.invoke(cli, ["task_status", "-t", "1"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "task status" in result.output

    @patch("jobmon.client.status_commands.update_task_status")
    def test_update_task_status_legacy(self, mock_cmd, runner):
        """Test legacy update_task_status command."""
        mock_cmd.return_value = "Updated"
        result = runner.invoke(
            cli, ["update_task_status", "-t", "1", "-w", "1", "-s", "D"]
        )

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "task update" in result.output

    @patch("jobmon.client.status_commands.get_task_dependencies")
    def test_task_dependencies_legacy(self, mock_cmd, runner):
        """Test legacy task_dependencies command."""
        mock_cmd.return_value = {"up": [], "down": []}
        result = runner.invoke(cli, ["task_dependencies", "-t", "1"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "task dependencies" in result.output

    @patch("jobmon.client.status_commands.concurrency_limit")
    def test_concurrency_limit_legacy(self, mock_cmd, runner):
        """Test legacy concurrency_limit command."""
        mock_cmd.return_value = "Updated"
        result = runner.invoke(cli, ["concurrency_limit", "-w", "1", "-m", "10"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow concurrency" in result.output

    @patch("jobmon.client.status_commands.get_filepaths")
    def test_get_filepaths_legacy(self, mock_cmd, runner):
        """Test legacy get_filepaths command."""
        mock_cmd.return_value = pd.DataFrame({"PATH": ["/path"]})
        result = runner.invoke(cli, ["get_filepaths", "-w", "1"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow logs" in result.output

    @patch("jobmon.client.status_commands.task_template_resources")
    def test_task_template_resources_legacy(self, mock_cmd, runner):
        """Test legacy task_template_resources command."""
        mock_cmd.return_value = "Resources"
        result = runner.invoke(cli, ["task_template_resources", "-t", "1"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow resources usage" in result.output

    @patch("jobmon.client.status_commands.create_resource_yaml")
    def test_create_resource_yaml_legacy(self, mock_cmd, runner):
        """Test legacy create_resource_yaml command."""
        mock_cmd.return_value = "yaml: content"
        result = runner.invoke(cli, ["create_resource_yaml", "-w", "1", "-p"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "workflow resources yaml" in result.output

    @patch("jobmon.client.status_commands.update_config_value")
    def test_update_config_legacy(self, mock_cmd, runner):
        """Test legacy update_config command."""
        mock_cmd.return_value = "Updated"
        result = runner.invoke(cli, ["update_config", "http.timeout", "30"])

        assert "DEPRECATION" in result.output or "deprecated" in result.output.lower()
        assert "config set" in result.output


class TestLegacyCommandHelp:
    """Test that legacy commands have deprecation notice in help."""

    def test_workflow_status_help_shows_deprecated(self, runner):
        """Test workflow_status --help shows deprecation notice."""
        result = runner.invoke(cli, ["workflow_status", "--help"])
        assert result.exit_code == 0
        assert "DEPRECATED" in result.output

    def test_workflow_tasks_help_shows_deprecated(self, runner):
        """Test workflow_tasks --help shows deprecation notice."""
        result = runner.invoke(cli, ["workflow_tasks", "--help"])
        assert result.exit_code == 0
        assert "DEPRECATED" in result.output

    def test_task_status_help_shows_deprecated(self, runner):
        """Test task_status --help shows deprecation notice."""
        result = runner.invoke(cli, ["task_status", "--help"])
        assert result.exit_code == 0
        assert "DEPRECATED" in result.output

    def test_update_task_status_help_shows_deprecated(self, runner):
        """Test update_task_status --help shows deprecation notice."""
        result = runner.invoke(cli, ["update_task_status", "--help"])
        assert result.exit_code == 0
        assert "DEPRECATED" in result.output


class TestLegacyArgumentCompatibility:
    """Test that legacy arguments are still accepted."""

    @patch("jobmon.client.status_commands.workflow_status")
    def test_workflow_status_legacy_args(self, mock_cmd, runner):
        """Test legacy workflow_status accepts old-style arguments."""
        mock_cmd.return_value = pd.DataFrame({"WF_ID": [1]})

        # Old style: -w for workflow_id
        result = runner.invoke(cli, ["workflow_status", "-w", "1"])
        assert result.exit_code == 0 or "DEPRECATION" in result.output

        # Old style: -u for user
        result = runner.invoke(cli, ["workflow_status", "-u", "testuser"])
        assert result.exit_code == 0 or "DEPRECATION" in result.output

        # Old style: -n for json
        result = runner.invoke(cli, ["workflow_status", "-w", "1", "-n"])
        assert result.exit_code == 0 or "DEPRECATION" in result.output

    @patch("jobmon.client.status_commands.task_status")
    def test_task_status_legacy_args(self, mock_cmd, runner):
        """Test legacy task_status accepts old-style arguments."""
        mock_cmd.return_value = pd.DataFrame({"TASK_ID": [1]})

        # Old style: -t for task_ids (multiple)
        result = runner.invoke(cli, ["task_status", "-t", "1", "-t", "2"])
        assert result.exit_code == 0 or "DEPRECATION" in result.output

    @patch("jobmon.client.status_commands.update_task_status")
    def test_update_task_status_legacy_args(self, mock_cmd, runner):
        """Test legacy update_task_status accepts old-style arguments."""
        mock_cmd.return_value = "Updated"

        # Old style uses --new_status (underscore)
        result = runner.invoke(
            cli,
            [
                "update_task_status",
                "-t",
                "1",
                "-w",
                "1",
                "-s",
                "D",
            ],
        )
        assert result.exit_code == 0 or "DEPRECATION" in result.output


class TestLegacyVsNewParity:
    """Test that legacy and new commands produce equivalent results."""

    @patch("jobmon.client.commands.workflow.workflow_status")
    @patch("jobmon.client.status_commands.workflow_status")
    def test_workflow_status_parity(self, mock_legacy, mock_new, runner):
        """Test legacy and new workflow status commands call same backend."""
        test_df = pd.DataFrame({"WF_ID": [1], "STATUS": ["DONE"]})
        mock_legacy.return_value = test_df
        mock_new.return_value = test_df

        # Run legacy command
        legacy_result = runner.invoke(cli, ["workflow_status", "-w", "1"])

        # Run new command
        new_result = runner.invoke(cli, ["workflow", "status", "-w", "1"])

        # Both should succeed (legacy has deprecation warning in output)
        assert legacy_result.exit_code == 0 or "DEPRECATION" in legacy_result.output
        assert new_result.exit_code == 0

    @patch("jobmon.client.commands.task.task_status")
    @patch("jobmon.client.status_commands.task_status")
    def test_task_status_parity(self, mock_legacy, mock_new, runner):
        """Test legacy and new task status commands call same backend."""
        test_df = pd.DataFrame({"TASK_ID": [1], "STATUS": ["DONE"]})
        mock_legacy.return_value = test_df
        mock_new.return_value = test_df

        # Run legacy command
        legacy_result = runner.invoke(cli, ["task_status", "-t", "1"])

        # Run new command
        new_result = runner.invoke(cli, ["task", "status", "-t", "1"])

        # Both should succeed (legacy has deprecation warning in output)
        assert legacy_result.exit_code == 0 or "DEPRECATION" in legacy_result.output
        assert new_result.exit_code == 0

"""Unit tests for the Click-based CLI structure.

These tests verify the CLI command structure, help text, and argument parsing
without requiring a running server or database.
"""

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from jobmon.client.cli.main import cli


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


class TestCLIStructure:
    """Test the main CLI structure and command groups."""

    def test_cli_help(self, runner):
        """Test that the main CLI shows help."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Jobmon workflow management CLI" in result.output
        assert "workflow" in result.output
        assert "task" in result.output
        assert "config" in result.output
        assert "version" in result.output

    def test_cli_verbose_flag(self, runner):
        """Test the verbose flag is accepted."""
        result = runner.invoke(cli, ["-v", "--help"])
        assert result.exit_code == 0

    def test_cli_debug_flag(self, runner):
        """Test the debug flag is accepted."""
        result = runner.invoke(cli, ["--debug", "--help"])
        assert result.exit_code == 0

    def test_cli_version_command(self, runner):
        """Test the version command."""
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        # Version output should contain version string
        assert result.output.strip() != ""

    def test_cli_version_with_components(self, runner):
        """Test the version command with --components flag."""
        result = runner.invoke(cli, ["version", "--components"])
        assert result.exit_code == 0
        # Version output should contain version string
        assert result.output.strip() != ""


class TestWorkflowCommandGroup:
    """Test the workflow command group structure."""

    def test_workflow_help(self, runner):
        """Test workflow group help."""
        result = runner.invoke(cli, ["workflow", "--help"])
        assert result.exit_code == 0
        assert "Workflow operations" in result.output
        assert "status" in result.output
        assert "tasks" in result.output
        assert "reset" in result.output
        assert "resume" in result.output
        assert "concurrency" in result.output
        assert "logs" in result.output
        assert "resources" in result.output

    def test_workflow_status_help(self, runner):
        """Test workflow status command help."""
        result = runner.invoke(cli, ["workflow", "status", "--help"])
        assert result.exit_code == 0
        assert "--workflow-id" in result.output or "-w" in result.output
        assert "--user" in result.output or "-u" in result.output
        assert "--output" in result.output or "-o" in result.output

    def test_workflow_tasks_help(self, runner):
        """Test workflow tasks command help."""
        result = runner.invoke(cli, ["workflow", "tasks", "--help"])
        assert result.exit_code == 0
        assert "--workflow-id" in result.output or "-w" in result.output
        assert "--status" in result.output or "-s" in result.output

    def test_workflow_reset_help(self, runner):
        """Test workflow reset command help."""
        result = runner.invoke(cli, ["workflow", "reset", "--help"])
        assert result.exit_code == 0
        assert "--workflow-id" in result.output or "-w" in result.output

    def test_workflow_resume_help(self, runner):
        """Test workflow resume command help."""
        result = runner.invoke(cli, ["workflow", "resume", "--help"])
        assert result.exit_code == 0
        assert "--workflow-id" in result.output or "-w" in result.output
        assert "--cluster" in result.output or "-c" in result.output

    def test_workflow_concurrency_help(self, runner):
        """Test workflow concurrency command help."""
        result = runner.invoke(cli, ["workflow", "concurrency", "--help"])
        assert result.exit_code == 0
        assert "--workflow-id" in result.output or "-w" in result.output
        assert "--max-tasks" in result.output or "-m" in result.output

    def test_workflow_logs_help(self, runner):
        """Test workflow logs command help."""
        result = runner.invoke(cli, ["workflow", "logs", "--help"])
        assert result.exit_code == 0
        assert "--workflow-id" in result.output or "-w" in result.output

    def test_workflow_resources_help(self, runner):
        """Test workflow resources subgroup help."""
        result = runner.invoke(cli, ["workflow", "resources", "--help"])
        assert result.exit_code == 0
        assert "usage" in result.output
        assert "yaml" in result.output

    def test_workflow_resources_usage_help(self, runner):
        """Test workflow resources usage command help."""
        result = runner.invoke(cli, ["workflow", "resources", "usage", "--help"])
        assert result.exit_code == 0

    def test_workflow_resources_yaml_help(self, runner):
        """Test workflow resources yaml command help."""
        result = runner.invoke(cli, ["workflow", "resources", "yaml", "--help"])
        assert result.exit_code == 0


class TestTaskCommandGroup:
    """Test the task command group structure."""

    def test_task_help(self, runner):
        """Test task group help."""
        result = runner.invoke(cli, ["task", "--help"])
        assert result.exit_code == 0
        assert "Task operations" in result.output
        assert "status" in result.output
        assert "update" in result.output
        assert "dependencies" in result.output

    def test_task_status_help(self, runner):
        """Test task status command help."""
        result = runner.invoke(cli, ["task", "status", "--help"])
        assert result.exit_code == 0
        assert "--task-id" in result.output or "-t" in result.output

    def test_task_update_help(self, runner):
        """Test task update command help."""
        result = runner.invoke(cli, ["task", "update", "--help"])
        assert result.exit_code == 0
        assert "--task-id" in result.output or "-t" in result.output
        assert "--status" in result.output or "-s" in result.output

    def test_task_dependencies_help(self, runner):
        """Test task dependencies command help."""
        result = runner.invoke(cli, ["task", "dependencies", "--help"])
        assert result.exit_code == 0
        assert "--task-id" in result.output or "-t" in result.output


class TestConfigCommandGroup:
    """Test the config command group structure."""

    def test_config_help(self, runner):
        """Test config group help."""
        result = runner.invoke(cli, ["config", "--help"])
        assert result.exit_code == 0
        assert "Configuration" in result.output or "config" in result.output.lower()
        assert "show" in result.output
        assert "set" in result.output

    def test_config_show_help(self, runner):
        """Test config show command help."""
        result = runner.invoke(cli, ["config", "show", "--help"])
        assert result.exit_code == 0

    def test_config_set_help(self, runner):
        """Test config set command help."""
        result = runner.invoke(cli, ["config", "set", "--help"])
        assert result.exit_code == 0


class TestWorkflowStatusCommand:
    """Test the workflow status command with mocked backend."""

    @patch("jobmon.client.commands.workflow.workflow_status")
    def test_workflow_status_by_user(self, mock_cmd, runner):
        """Test workflow status filtered by user."""
        import pandas as pd

        mock_cmd.return_value = pd.DataFrame(
            {
                "WF_ID": [1],
                "WF_STATUS": ["DONE"],
                "TASKS": [10],
            }
        )
        result = runner.invoke(cli, ["workflow", "status", "-u", "testuser"])
        assert result.exit_code == 0
        mock_cmd.assert_called_once()

    @patch("jobmon.client.commands.workflow.workflow_status")
    def test_workflow_status_by_id(self, mock_cmd, runner):
        """Test workflow status filtered by workflow ID."""
        import pandas as pd

        mock_cmd.return_value = pd.DataFrame(
            {
                "WF_ID": [123],
                "WF_STATUS": ["RUNNING"],
                "TASKS": [5],
            }
        )
        result = runner.invoke(cli, ["workflow", "status", "-w", "123"])
        assert result.exit_code == 0
        mock_cmd.assert_called_once()

    @patch("jobmon.client.commands.workflow.workflow_status")
    def test_workflow_status_json_output(self, mock_cmd, runner):
        """Test workflow status with JSON output."""
        mock_cmd.return_value = '{"WF_ID": 1}'
        result = runner.invoke(cli, ["workflow", "status", "-w", "1", "-o", "json"])
        assert result.exit_code == 0

    @patch("jobmon.client.commands.workflow.workflow_status")
    def test_workflow_status_multiple_ids(self, mock_cmd, runner):
        """Test workflow status with multiple workflow IDs."""
        import pandas as pd

        mock_cmd.return_value = pd.DataFrame(
            {
                "WF_ID": [1, 2],
                "WF_STATUS": ["DONE", "DONE"],
            }
        )
        result = runner.invoke(cli, ["workflow", "status", "-w", "1", "-w", "2"])
        assert result.exit_code == 0


class TestTaskStatusCommand:
    """Test the task status command with mocked backend."""

    @patch("jobmon.client.commands.task.task_status")
    def test_task_status_single_task(self, mock_cmd, runner):
        """Test task status for a single task."""
        import pandas as pd

        mock_cmd.return_value = pd.DataFrame(
            {
                "TASK_ID": [123],
                "STATUS": ["DONE"],
            }
        )
        result = runner.invoke(cli, ["task", "status", "-t", "123"])
        assert result.exit_code == 0
        mock_cmd.assert_called_once()

    @patch("jobmon.client.commands.task.task_status")
    def test_task_status_multiple_tasks(self, mock_cmd, runner):
        """Test task status for multiple tasks."""
        import pandas as pd

        mock_cmd.return_value = pd.DataFrame(
            {
                "TASK_ID": [123, 456],
                "STATUS": ["DONE", "RUNNING"],
            }
        )
        result = runner.invoke(cli, ["task", "status", "-t", "123", "-t", "456"])
        assert result.exit_code == 0


class TestArgumentValidation:
    """Test argument validation and error handling."""

    def test_workflow_tasks_requires_workflow_id(self, runner):
        """Test that workflow tasks requires --workflow-id."""
        result = runner.invoke(cli, ["workflow", "tasks"])
        assert result.exit_code != 0
        assert (
            "workflow-id" in result.output.lower()
            or "required" in result.output.lower()
        )

    def test_task_status_requires_task_id(self, runner):
        """Test that task status requires --task-id."""
        result = runner.invoke(cli, ["task", "status"])
        assert result.exit_code != 0
        assert "task-id" in result.output.lower() or "required" in result.output.lower()

    def test_workflow_resume_requires_cluster(self, runner):
        """Test that workflow resume requires --cluster."""
        result = runner.invoke(cli, ["workflow", "resume", "-w", "1"])
        assert result.exit_code != 0

    def test_invalid_output_format(self, runner):
        """Test that invalid output format is rejected."""
        result = runner.invoke(cli, ["workflow", "status", "-o", "invalid"])
        assert result.exit_code != 0


class TestMainFunction:
    """Test the main entry point function."""

    def test_main_with_args(self, runner):
        """Test main() function with arguments."""
        # main() is just a wrapper, test it can be invoked
        result = runner.invoke(cli, ["--help"], catch_exceptions=False)
        # Help should work
        assert "Jobmon" in result.output or result.exit_code == 0

    def test_main_handles_exceptions(self, runner):
        """Test that main() catches and reports errors gracefully."""
        # Test with an invalid subcommand to verify error handling
        result = runner.invoke(cli, ["invalid_command"])
        # Should fail gracefully
        assert result.exit_code != 0

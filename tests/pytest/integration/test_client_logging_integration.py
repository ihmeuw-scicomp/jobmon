"""Integration tests for client component logging."""

from unittest.mock import MagicMock, patch

from jobmon.client.cli import ClientCLI


class TestClientLoggingIntegration:
    """Test client CLI and workflow logging integration."""

    def test_client_cli_enables_automatic_logging(self):
        """Test that client CLI automatically configures component logging on startup."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Create client CLI
            cli = ClientCLI()

            # Check that component_name is "client"
            assert cli.component_name == "client"

            # Mock the client command to avoid running actual commands
            mock_args = MagicMock()
            mock_args.func = lambda args: None

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Run the CLI main method
                cli.main("version")

                # Verify that component logging was configured for client
                mock_configure.assert_called_once_with("client")

    def test_client_logging_with_template(self, tmp_path):
        """Test client logging with default template."""
        # Create a temporary client template file
        template_content = """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: simple

loggers:
  jobmon.client:
    handlers: [console]
    level: INFO
    propagate: false

root:
  handlers: [console]
  level: WARNING
"""
        template_file = tmp_path / "logconfig_client.yaml"
        template_file.write_text(template_content.strip())

        with patch(
            "jobmon.core.config.logconfig_utils._get_component_template_path",
            return_value=str(template_file),
        ):
            with patch(
                "jobmon.core.config.logconfig_utils.configure_logging_with_overrides"
            ) as mock_configure:
                from jobmon.core.config.logconfig_utils import (
                    configure_component_logging,
                )

                # Test component logging configuration
                configure_component_logging("client")

                # Verify template was used
                mock_configure.assert_called_once_with(
                    default_template_path=str(template_file),
                    config_section="client",
                    fallback_config=None,
                )

    def test_client_logging_with_file_override(self, tmp_path):
        """Test client logging with file override."""
        # Create custom config file
        custom_config = tmp_path / "custom_client_config.yaml"
        custom_config.write_text(
            """
version: 1
disable_existing_loggers: false

formatters:
  custom:
    format: "CUSTOM: %(message)s"

handlers:
  console:
    class: logging.StreamHandler
    level: DEBUG
    formatter: custom

loggers:
  jobmon.client:
    handlers: [console]
    level: DEBUG

root:
  handlers: [console]
  level: DEBUG
"""
        )

        # Mock configuration to use custom file
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda section, key: {
            ("logging", "client_logconfig_file"): str(custom_config)
        }.get((section, key))

        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig", return_value=mock_config
        ):
            with patch(
                "jobmon.core.config.logconfig_utils.configure_logging_with_overrides"
            ) as mock_configure:
                from jobmon.core.config.logconfig_utils import (
                    configure_component_logging,
                )

                configure_component_logging("client")

                # Verify file override was used
                mock_configure.assert_called_once()
                call_args = mock_configure.call_args
                assert call_args[1]["config_section"] == "client"

    def test_client_logging_failure_does_not_crash(self):
        """Test that client logging failures don't crash client operations."""
        # Mock configure_component_logging to raise an exception
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging",
            side_effect=Exception("Config failed"),
        ):
            # Should not raise an exception
            cli = ClientCLI()

            # Mock command
            mock_args = MagicMock()
            mock_args.func = lambda args: "success"

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Should not crash despite logging configuration failure
                result = cli.main("test")
                assert result == "success"

    def test_workflow_component_logging_method(self):
        """Test workflow _configure_component_logging method."""
        from jobmon.client.workflow import Workflow

        # Mock ToolVersion to avoid database dependencies
        mock_tool_version = MagicMock()
        workflow = Workflow(tool_version=mock_tool_version)

        # Test _configure_component_logging method directly
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            workflow._configure_component_logging()
            mock_configure.assert_called_once_with("client")

    def test_workflow_configure_logging_option(self):
        """Test workflow.run(configure_logging=True) uses component logging."""
        from jobmon.client.workflow import Workflow

        # Mock ToolVersion to avoid database dependencies
        mock_tool_version = MagicMock()
        workflow = Workflow(tool_version=mock_tool_version)

        # Test that configure_logging=True calls component logging
        with patch.object(workflow, "_configure_component_logging") as mock_component:
            with patch.object(workflow, "bind"), patch.object(
                workflow, "_bind_tasks"
            ), patch.object(workflow, "validate"), patch(
                "jobmon.client.workflow.SwarmWorkflowRun"
            ), patch(
                "jobmon.client.workflow.WorkflowRunFactory"
            ), patch(
                "jobmon.client.workflow.DistributorContext"
            ):

                # Mock workflow as bound to avoid property access issues
                workflow._workflow_id = 123
                workflow._status = "REGISTERED"
                workflow._newly_created = True
                workflow._clusters = {"test": MagicMock()}

                try:
                    workflow.run(configure_logging=True)
                except Exception:
                    pass  # We only care about the logging call

                mock_component.assert_called_once()

    def test_client_logging_consistency_with_other_components(self):
        """Test that client CLI follows the same component logging pattern as other CLIs."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Test CLI pattern - same as distributor, worker, and server
            cli = ClientCLI()
            mock_args = MagicMock()
            mock_args.func = lambda args: None

            with patch.object(cli, "parse_args", return_value=mock_args):
                cli.main("test")

                # Verify component logging pattern is consistent across all components
                mock_configure.assert_called_once_with("client")

                # Check that CLI structure is consistent
                assert cli.component_name == "client"
                assert hasattr(cli, "configure_component_logging")

                # Client CLI follows same inheritance pattern as other CLIs
                from jobmon.core.cli import CLI

                assert isinstance(cli, CLI)

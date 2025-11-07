"""Integration tests for distributor automatic logging configuration."""

import logging
from unittest.mock import MagicMock, patch

from jobmon.distributor.cli import DistributorCLI


class TestDistributorLoggingIntegration:
    """Test distributor CLI logging integration."""

    def test_distributor_cli_enables_automatic_logging(self):
        """Test that distributor CLI automatically configures logging on startup."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Create distributor CLI
            cli = DistributorCLI()

            # Check that component_name is set correctly
            assert cli.component_name == "distributor"

            # Mock the distributor command to avoid running actual distributor
            with patch.object(cli, "run_distributor", return_value=None) as mock_run:
                # Mock the argument parsing to return a valid args object
                mock_args = MagicMock()
                mock_args.func = mock_run

                with patch.object(cli, "parse_args", return_value=mock_args):
                    # Run the CLI main method
                    cli.main("start --cluster_name test --workflow_run_id 123")

                    # Verify that component logging was configured
                    mock_configure.assert_called_once_with("distributor")

    def test_distributor_logging_with_template(self, tmp_path):
        """Test distributor logging with template configuration."""
        # Create a minimal distributor template
        template_content = """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(name)s - %(levelname)s - %(message)s"

handlers:
  test_handler:
    class: logging.StreamHandler
    level: INFO
    formatter: simple

loggers:
  jobmon.distributor:
    handlers: [test_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_distributor.yaml"
        template_file.write_text(template_content)

        # Mock JobmonConfig to avoid configuration file dependencies
        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig"
        ) as mock_config_class:
            mock_config = MagicMock()
            # No file override - use ConfigError which gets caught properly
            from jobmon.core.exceptions import ConfigError

            mock_config.get.side_effect = ConfigError("No file override")
            # No section override
            mock_config.get_section_coerced.return_value = {}
            mock_config_class.return_value = mock_config

            # Mock the component template path resolution to point to our temp template
            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                # Clear any existing handlers
                distributor_logger = logging.getLogger("jobmon.distributor")
                distributor_logger.handlers.clear()

                # Create CLI and trigger logging configuration
                cli = DistributorCLI()

                # Mock the command execution to avoid running actual distributor
                with patch.object(cli, "run_distributor", return_value=None):
                    mock_args = MagicMock()
                    mock_args.func = cli.run_distributor

                    with patch.object(cli, "parse_args", return_value=mock_args):
                        cli.main("test")

                        # Check that distributor logger was configured
                        assert len(distributor_logger.handlers) > 0
                        assert distributor_logger.level == logging.INFO

                        # Clean up
                        distributor_logger.handlers.clear()

    def test_distributor_logging_with_file_override(self, tmp_path):
        """Test distributor logging with file-based configuration override."""
        # Create override config file
        override_content = """
version: 1
disable_existing_loggers: false

formatters:
  override:
    format: "OVERRIDE: %(message)s"

handlers:
  override_handler:
    class: logging.StreamHandler
    level: DEBUG
    formatter: override

loggers:
  jobmon.distributor:
    handlers: [override_handler]
    level: DEBUG
    propagate: false
"""
        override_file = tmp_path / "override.yaml"
        override_file.write_text(override_content)

        # Create default template (should be ignored due to file override)
        template_content = """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(message)s"

handlers:
  default_handler:
    class: logging.StreamHandler
    level: INFO
    formatter: simple

loggers:
  jobmon.distributor:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_distributor.yaml"
        template_file.write_text(template_content)

        # Mock JobmonConfig to return file override
        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig"
        ) as mock_config_class:
            mock_config = MagicMock()
            mock_config.get.return_value = str(override_file)
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                with patch("os.path.dirname", return_value=str(tmp_path)):
                    # Clear any existing handlers
                    distributor_logger = logging.getLogger("jobmon.distributor")
                    distributor_logger.handlers.clear()

                    # Create CLI and trigger logging configuration
                    cli = DistributorCLI()

                    # Mock the command execution
                    with patch.object(cli, "run_distributor", return_value=None):
                        mock_args = MagicMock()
                        mock_args.func = cli.run_distributor

                        with patch.object(cli, "parse_args", return_value=mock_args):
                            cli.main("test")

                            # Check that override configuration was used (DEBUG level)
                            assert len(distributor_logger.handlers) > 0
                            assert distributor_logger.level == logging.DEBUG

                            # Clean up
                            distributor_logger.handlers.clear()

    def test_distributor_logging_failure_does_not_crash(self):
        """Test that distributor starts successfully even if logging configuration fails."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging",
            side_effect=Exception("Logging failed"),
        ):
            # Create CLI - should not raise exception
            cli = DistributorCLI()
            assert cli.component_name == "distributor"

            # Mock the command execution
            with patch.object(
                cli, "run_distributor", return_value="success"
            ) as mock_run:
                mock_args = MagicMock()
                mock_args.func = mock_run

                with patch.object(cli, "parse_args", return_value=mock_args):
                    # Should complete successfully despite logging failure
                    result = cli.main("test")
                    assert result == "success"

    def test_distributor_cli_flushes_otlp_on_exit(self):
        """Ensure distributor CLI flushes OTLP telemetry after execution."""
        cli = DistributorCLI()

        mock_args = MagicMock()
        mock_args.func = MagicMock(return_value="ok")

        with patch.object(cli, "parse_args", return_value=mock_args), patch(
            "jobmon.core.otlp.manager.otlp_flush_on_exit"
        ) as mock_flush_context:
            mock_enter = mock_flush_context.return_value.__enter__
            mock_exit = mock_flush_context.return_value.__exit__

            result = cli.main("start")

            assert result == "ok"
            mock_flush_context.assert_called_once()
            mock_enter.assert_called_once()
            mock_exit.assert_called_once()

    def test_distributor_logging_with_section_override(self, tmp_path):
        """Test distributor logging with section-based configuration override."""
        # Create default template
        template_content = """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(message)s"

handlers:
  default_handler:
    class: logging.StreamHandler
    level: INFO
    formatter: simple

loggers:
  jobmon.distributor:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_distributor.yaml"
        template_file.write_text(template_content)

        # Mock JobmonConfig to return section override
        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig"
        ) as mock_config_class:
            mock_config = MagicMock()
            # No file override
            mock_config.get.side_effect = Exception("No file override")
            # Section override
            mock_config.get_section_coerced.return_value = {
                "distributor": {"loggers": {"jobmon.distributor": {"level": "DEBUG"}}}
            }
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                with patch("os.path.dirname", return_value=str(tmp_path)):
                    # Clear any existing handlers
                    distributor_logger = logging.getLogger("jobmon.distributor")
                    distributor_logger.handlers.clear()

                    # Create CLI and trigger logging configuration
                    cli = DistributorCLI()

                    # Mock the command execution
                    with patch.object(cli, "run_distributor", return_value=None):
                        mock_args = MagicMock()
                        mock_args.func = cli.run_distributor

                        with patch.object(cli, "parse_args", return_value=mock_args):
                            cli.main("test")

                            # Check that configuration was applied
                            assert len(distributor_logger.handlers) > 0

                            # Clean up
                            distributor_logger.handlers.clear()

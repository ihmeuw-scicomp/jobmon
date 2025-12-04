"""Parameterized component logging tests.

This module contains tests that verify logging configuration works consistently
across all Jobmon components (client, server, distributor, worker).

Instead of duplicating nearly-identical tests in 4 separate files, we use
pytest parameterization to run the same test logic against each component.

Test Categories:
1. CLI Integration - Tests that CLI classes auto-configure logging
2. Configuration - Tests template, file override, and section override
3. Resilience - Tests graceful failure handling
4. OTLP - Tests OTLP flush on exit (for distributor/worker only)
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from .conftest import (
    ComponentConfig,
    clear_logger_handlers,
    create_mock_args,
    get_cli_class,
)


class TestCLILoggingIntegration:
    """Test that all component CLIs automatically configure logging."""

    def test_cli_enables_automatic_logging(self, component: ComponentConfig):
        """Test that {component.name} CLI automatically configures component logging on startup."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Dynamically import and create CLI
            cli_class = get_cli_class(component)
            cli = cli_class()

            # Verify component_name is set correctly
            assert cli.component_name == component.name

            # Create mock args
            mock_args = create_mock_args(component, cli, return_value=None)

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Run the CLI main method
                cli.main(component.cli_main_args)

                # Verify that component logging was configured
                mock_configure.assert_called_once_with(component.name)

    def test_cli_inherits_from_base_cli(self, component: ComponentConfig):
        """Test that {component.name} CLI inherits from base CLI class."""
        from jobmon.core.cli import CLI

        cli_class = get_cli_class(component)
        cli = cli_class()

        assert isinstance(cli, CLI)
        assert hasattr(cli, "component_name")
        assert hasattr(cli, "configure_component_logging")


class TestLoggingWithConfiguration:
    """Test logging configuration with templates, file overrides, and section overrides."""

    def test_logging_with_programmatic_config(
        self, component: ComponentConfig, tmp_path
    ):
        """Test {component.name} logging uses programmatic configuration as base."""
        # Clear any existing handlers
        component_logger = clear_logger_handlers(component.logger_name)

        with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
            mock_config = MagicMock()
            mock_config.get.return_value = ""
            mock_config.get_section_coerced.return_value = {}
            mock_config_class.return_value = mock_config

            from jobmon.core.config.logconfig_utils import configure_component_logging

            # Configure logging for the component
            configure_component_logging(component.name)

            # Verify logger was configured
            assert len(component_logger.handlers) > 0

            # Clean up
            component_logger.handlers.clear()

    def test_logging_with_file_override(
        self, component: ComponentConfig, tmp_path, override_content: str
    ):
        """Test {component.name} logging with file-based configuration override."""
        # Create override config file
        override_file = tmp_path / f"override_{component.name}.yaml"
        override_file.write_text(override_content.format(logger_name=component.logger_name))

        # Create default template
        template_content = f"""
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
  {component.logger_name}:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / f"logconfig_{component.name}.yaml"
        template_file.write_text(template_content)

        # Clear existing handlers
        component_logger = clear_logger_handlers(component.logger_name)

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
                    # Create CLI and trigger logging configuration
                    cli_class = get_cli_class(component)
                    cli = cli_class()

                    # Mock the command execution
                    mock_args = create_mock_args(component, cli, return_value=None)

                    with patch.object(cli, "parse_args", return_value=mock_args):
                        cli.main(component.cli_main_args)

                        # Check that override configuration was used (DEBUG level)
                        assert len(component_logger.handlers) > 0
                        assert component_logger.level == logging.DEBUG

                        # Clean up
                        component_logger.handlers.clear()

    def test_logging_with_section_override(
        self, component: ComponentConfig, tmp_path, template_content: str
    ):
        """Test {component.name} logging with section-based configuration override."""
        # Create default template
        template_file = tmp_path / f"logconfig_{component.name}.yaml"
        template_file.write_text(template_content.format(logger_name=component.logger_name))

        # Clear existing handlers
        component_logger = clear_logger_handlers(component.logger_name)

        # Mock JobmonConfig to return section override
        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig"
        ) as mock_config_class:
            mock_config = MagicMock()
            # No file override
            mock_config.get.side_effect = Exception("No file override")
            # Section override - set DEBUG level
            mock_config.get_section_coerced.return_value = {
                component.name: {
                    "loggers": {component.logger_name: {"level": "DEBUG"}}
                }
            }
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                with patch("os.path.dirname", return_value=str(tmp_path)):
                    # Create CLI and trigger logging configuration
                    cli_class = get_cli_class(component)
                    cli = cli_class()

                    # Mock the command execution
                    mock_args = create_mock_args(component, cli, return_value=None)

                    with patch.object(cli, "parse_args", return_value=mock_args):
                        cli.main(component.cli_main_args)

                        # Check that configuration was applied
                        assert len(component_logger.handlers) > 0

                        # Clean up
                        component_logger.handlers.clear()


class TestLoggingResilience:
    """Test that logging failures don't crash components."""

    def test_logging_failure_does_not_crash(self, component: ComponentConfig):
        """Test that {component.name} starts successfully even if logging configuration fails."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging",
            side_effect=Exception("Logging configuration failed"),
        ):
            # Create CLI - should not raise exception
            cli_class = get_cli_class(component)
            cli = cli_class()

            assert cli.component_name == component.name

            # Mock the command execution
            mock_args = create_mock_args(component, cli, return_value="success")

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Should complete successfully despite logging failure
                result = cli.main(component.cli_main_args)
                assert result == "success"


class TestOTLPFlush:
    """Test OTLP telemetry flush on exit for long-running components."""

    def test_cli_flushes_otlp_on_exit(self, component_with_otlp_flush: ComponentConfig):
        """Test that {component_with_otlp_flush.name} CLI flushes OTLP telemetry after execution."""
        component = component_with_otlp_flush

        cli_class = get_cli_class(component)
        cli = cli_class()

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


class TestLoggingConsistency:
    """Test that all components follow the same logging patterns."""

    def test_component_logging_pattern_consistency(self, component: ComponentConfig):
        """Test that {component.name} CLI follows the same logging pattern as other CLIs."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            cli_class = get_cli_class(component)
            cli = cli_class()

            mock_args = create_mock_args(component, cli, return_value=None)

            with patch.object(cli, "parse_args", return_value=mock_args):
                cli.main(component.cli_main_args)

                # Verify component logging pattern is consistent
                mock_configure.assert_called_once_with(component.name)

                # Check that CLI structure is consistent
                assert cli.component_name == component.name
                assert hasattr(cli, "configure_component_logging")

                # Verify CLI follows same inheritance pattern
                from jobmon.core.cli import CLI

                assert isinstance(cli, CLI)


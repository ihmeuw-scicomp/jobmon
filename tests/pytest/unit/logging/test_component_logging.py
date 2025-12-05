"""Tests for component logging configuration functionality."""

import logging
from unittest.mock import MagicMock, patch

from jobmon.core.cli import CLI
from jobmon.core.config.logconfig_utils import (
    _get_component_template_path,
    configure_component_logging,
)


class TestComponentLogging:
    """Test component logging configuration functionality."""

    def test_configure_component_logging_with_template(self, tmp_path):
        """Test component logging with default template."""
        # Create a temporary template file
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
  jobmon.test:
    handlers: [test_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_test.yaml"
        template_file.write_text(template_content)

        # Mock the component template path resolution to point to our temp template
        with patch(
            "jobmon.core.config.logconfig_utils._get_component_template_path",
            return_value=str(template_file),
        ):
            # Clear any existing handlers
            test_logger = logging.getLogger("jobmon.test")
            test_logger.handlers.clear()

            # Configure component logging
            configure_component_logging("test")

            # Check that logger was configured
            assert len(test_logger.handlers) > 0
            assert test_logger.level == logging.INFO

            # Clean up
            test_logger.handlers.clear()

    def test_configure_component_logging_generates_programmatic_config(self, tmp_path):
        """Test component logging generates config for known components even without template file."""
        # Clear any existing handlers
        test_logger = logging.getLogger("jobmon.distributor")
        test_logger.handlers.clear()

        # Configure should work via programmatic generation (no template file needed)
        configure_component_logging("distributor")

        # Should have configured the logger programmatically
        assert len(test_logger.handlers) > 0

        # Clean up
        test_logger.handlers.clear()

    def test_configure_component_logging_with_file_override(self, tmp_path):
        """Test component logging with file override."""
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
  jobmon.test:
    handlers: [override_handler]
    level: DEBUG
    propagate: false
"""
        override_file = tmp_path / "override.yaml"
        override_file.write_text(override_content)

        # Create default template (should be ignored)
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
  jobmon.test:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_test.yaml"
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
                # Clear any existing handlers
                test_logger = logging.getLogger("jobmon.test")
                test_logger.handlers.clear()

                # Configure component logging
                configure_component_logging("test")

                # Check that override configuration was used
                assert len(test_logger.handlers) > 0
                assert test_logger.level == logging.DEBUG

                # Clean up
                test_logger.handlers.clear()

    def test_configure_component_logging_with_section_override(self, tmp_path):
        """Test component logging with section override."""
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
  jobmon.test:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_test.yaml"
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
                "test": {"loggers": {"jobmon.test": {"level": "DEBUG"}}}
            }
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                # Clear any existing handlers
                test_logger = logging.getLogger("jobmon.test")
                test_logger.handlers.clear()

                # Configure component logging
                configure_component_logging("test")

                # Check that configuration was applied
                assert len(test_logger.handlers) > 0

                # Clean up
                test_logger.handlers.clear()

    def test_configure_component_logging_invalid_config(self, tmp_path):
        """Test component logging fails silently with invalid config."""
        # Create invalid template file
        template_file = tmp_path / "logconfig_test.yaml"
        template_file.write_text("invalid: yaml: content: [")

        with patch(
            "jobmon.core.config.logconfig_utils._get_component_template_path",
            return_value=str(template_file),
        ):
            # This should not raise an exception
            configure_component_logging("test")

            # Should not configure logger (or configuration fails gracefully)
            test_logger = logging.getLogger("jobmon.test")
            # Handlers might exist from other tests, but it shouldn't crash

    def test_get_component_template_path(self):
        """Test component template path resolution with local package structure."""
        # Test valid components
        distributor_path = _get_component_template_path("distributor")
        worker_path = _get_component_template_path("worker")
        server_path = _get_component_template_path("server")

        # Should return non-empty paths for valid components
        assert distributor_path != ""
        assert worker_path != ""
        assert server_path != ""

        # Paths should contain the expected structure
        assert "distributor" in distributor_path
        assert "worker_node" in worker_path  # Note: worker maps to worker_node
        assert "server" in server_path
        assert "config" in distributor_path
        assert "config" in worker_path
        assert "config" in server_path

        # Test invalid component
        invalid_path = _get_component_template_path("invalid")
        assert invalid_path == ""


class TestComponentCLI:
    """Test CLI integration with component logging."""

    def test_cli_with_component_name(self):
        """Test CLI automatically configures logging when component_name is provided."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            cli = CLI(component_name="test")

            # Mock a simple command
            subparsers = cli.parser.add_subparsers()
            subparser = subparsers.add_parser("test")
            subparser.set_defaults(func=lambda args: "success")

            # Run the CLI
            result = cli.main("test")

            # Check that logging was configured
            mock_configure.assert_called_once_with("test")
            assert result == "success"

    def test_cli_without_component_name(self):
        """Test CLI skips logging configuration when component_name is None."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            cli = CLI(component_name=None)

            # Mock a simple command
            subparsers = cli.parser.add_subparsers()
            subparser = subparsers.add_parser("test")
            subparser.set_defaults(func=lambda args: "success")

            # Run the CLI
            result = cli.main("test")

            # Check that logging was NOT configured
            mock_configure.assert_not_called()
            assert result == "success"

    def test_cli_logging_failure_does_not_crash(self):
        """Test CLI continues when logging configuration fails."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging",
            side_effect=Exception("Config failed"),
        ):
            cli = CLI(component_name="test")

            # Mock a simple command
            subparsers = cli.parser.add_subparsers()
            subparser = subparsers.add_parser("test")
            subparser.set_defaults(func=lambda args: "success")

            # Run the CLI - should not crash
            result = cli.main("test")
            assert result == "success"

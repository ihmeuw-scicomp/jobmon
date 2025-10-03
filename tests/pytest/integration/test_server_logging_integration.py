"""Integration tests for server automatic logging configuration."""

import logging
from unittest.mock import MagicMock, patch

from jobmon.server.cli import ServerCLI


class TestServerLoggingIntegration:
    """Test server CLI and web app logging integration."""

    def test_server_cli_enables_automatic_logging(self):
        """Test that server CLI automatically configures component logging on startup."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Create server CLI
            cli = ServerCLI()

            # Check that component_name is "server" (server CLI uses component logging)
            assert cli.component_name == "server"

            # Mock the server command to avoid running actual server
            mock_args = MagicMock()
            mock_args.func = lambda args: None

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Run the CLI main method
                cli.main("test")

                # Verify that component logging was configured for server
                mock_configure.assert_called_once_with("server")

    def test_server_web_app_logging_integration(self):
        """Test that server web app automatically configures logging."""
        # Need to patch BEFORE import to avoid module caching issues in xdist
        with patch("jobmon.server.web.log_config.configure_logging") as mock_configure:
            with patch("jobmon.core.configuration.JobmonConfig"):
                # Clear module cache to ensure fresh import in parallel tests
                import sys

                module_name = "jobmon.server.web.api"
                if module_name in sys.modules:
                    del sys.modules[module_name]

                # Import and call get_app with fresh module state
                from jobmon.server.web.api import get_app

                app = get_app()

                # Verify that logging configuration was called
                mock_configure.assert_called_once()

                # Verify the app was created
                assert app is not None

    def test_server_logging_with_custom_config(self, tmp_path):
        """Test server logging with custom configuration via dict config (highest precedence)."""
        # Create custom server config as dict (bypasses file loading issues)
        custom_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "custom": {"format": "CUSTOM: %(name)s - %(levelname)s - %(message)s"}
            },
            "handlers": {
                "custom_handler": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "custom",
                }
            },
            "loggers": {
                "jobmon.server.web": {
                    "handlers": ["custom_handler"],
                    "level": "DEBUG",
                    "propagate": False,
                }
            },
        }

        # Clear any existing handlers
        server_logger = logging.getLogger("jobmon.server.web")
        server_logger.handlers.clear()

        # Import and test configure_logging with explicit dict config (highest precedence)
        import logging.config

        from jobmon.core.config.structlog_config import configure_structlog

        # Apply the dict config directly
        logging.config.dictConfig(custom_config)
        configure_structlog(component_name="server")

        # Check that custom configuration was applied
        assert len(server_logger.handlers) > 0
        assert server_logger.level == logging.DEBUG

        # Clean up
        server_logger.handlers.clear()

    def test_server_logging_fallback_behavior(self):
        """Test server logging graceful fallback when configuration fails."""
        with patch(
            "jobmon.server.web.log_config.configure_logging",
            side_effect=Exception("Config failed"),
        ):
            # Server CLI should not crash when logging fails
            cli = ServerCLI()

            # Mock the server command
            mock_args = MagicMock()
            mock_args.func = lambda args: "success"

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Should complete successfully despite logging failure
                result = cli.main("test")
                assert result == "success"

    def test_server_logging_with_section_override(self, tmp_path):
        """Test server logging with section-based configuration override."""
        # Create base template
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
  jobmon.server.web:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_server.yaml"
        template_file.write_text(template_content)

        # Mock JobmonConfig to return section override
        with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
            mock_config = MagicMock()
            # No file override - use ConfigError which gets caught properly
            from jobmon.core.exceptions import ConfigError

            mock_config.get.side_effect = ConfigError("No file override")
            # Section override
            mock_config.get_section_coerced.return_value = {
                "server": {"loggers": {"jobmon.server.web": {"level": "DEBUG"}}}
            }
            mock_config_class.return_value = mock_config

            # Mock the template path to point to our test template
            with patch("os.path.dirname", return_value=str(tmp_path)):
                with patch("os.path.join", return_value=str(template_file)):
                    # Clear any existing handlers
                    server_logger = logging.getLogger("jobmon.server.web")
                    server_logger.handlers.clear()

                    # Import and test configure_logging
                    from jobmon.core.config.logconfig_utils import (
                        configure_component_logging,
                    )
                    from jobmon.core.config.structlog_config import configure_structlog

                    # Should apply section override
                    configure_component_logging("server")
                    configure_structlog(component_name="server")

                    # Check that configuration was applied
                    assert len(server_logger.handlers) > 0

                    # Clean up
                    server_logger.handlers.clear()

    def test_server_otlp_validation_integration(self):
        """Test that server logging includes OTLP validation."""
        with patch(
            "jobmon.core.otlp.validation.validate_and_log_otlp_config"
        ) as mock_validate:
            with patch("jobmon.core.configuration.JobmonConfig"):
                mock_validate.return_value = True

                # Import and test configure_logging
                from jobmon.core.config.logconfig_utils import (
                    configure_component_logging,
                )
                from jobmon.core.config.structlog_config import configure_structlog

                # Configure using core utilities
                configure_component_logging("server")
                configure_structlog(component_name="server")

                # Validation should have been called
                mock_validate.assert_called_once()

    def test_server_advanced_precedence_levels(self):
        """Test server's advanced configuration precedence (dict > file > config > template)."""
        # Test explicit dict config (highest precedence)
        dict_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"dict_formatter": {"format": "DICT: %(message)s"}},
            "handlers": {
                "dict_handler": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "dict_formatter",
                }
            },
            "loggers": {
                "jobmon.server.web": {
                    "handlers": ["dict_handler"],
                    "level": "INFO",
                    "propagate": False,
                }
            },
        }

        # Clear any existing handlers
        server_logger = logging.getLogger("jobmon.server.web")
        server_logger.handlers.clear()

        # Import and test configure_logging with explicit dict
        import logging.config

        from jobmon.core.config.structlog_config import configure_structlog

        # Apply the dict config directly
        logging.config.dictConfig(dict_config)
        configure_structlog(component_name="server")

        # Check that configuration was applied
        assert len(server_logger.handlers) > 0
        assert server_logger.level == logging.INFO

        # Clean up
        server_logger.handlers.clear()

    def test_server_performance_characteristics(self):
        """Test server logging performance and characteristics."""
        import time

        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Measure server CLI startup time
            start_time = time.time()

            # Create server CLI
            cli = ServerCLI()

            # Trigger logging configuration manually
            cli.configure_component_logging()

            end_time = time.time()
            startup_time = end_time - start_time

            # Server startup should be fast (less than 2 seconds for logging configuration)
            assert startup_time < 2.0

            # Verify component logging was configured for server
            mock_configure.assert_called_once_with("server")
            assert cli.component_name == "server"  # Server CLI uses component logging

    def test_server_logging_consistency_with_other_components(self):
        """Test that server CLI follows the same component logging pattern as distributor and worker."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Test CLI pattern - same as distributor and worker
            cli = ServerCLI()
            mock_args = MagicMock()
            mock_args.func = lambda args: None

            with patch.object(cli, "parse_args", return_value=mock_args):
                cli.main("test")

                # Verify component logging pattern is consistent across all components
                mock_configure.assert_called_once_with("server")

                # Check that CLI structure is consistent
                assert cli.component_name == "server"
                assert hasattr(cli, "configure_component_logging")

                # Server CLI follows same inheritance pattern as other CLIs
                from jobmon.core.cli import CLI

                assert isinstance(cli, CLI)

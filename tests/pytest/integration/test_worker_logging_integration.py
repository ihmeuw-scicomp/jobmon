"""Integration tests for worker node automatic logging configuration."""

import logging
from unittest.mock import MagicMock, patch

from jobmon.worker_node.cli import WorkerNodeCLI


class TestWorkerLoggingIntegration:
    """Test worker node CLI logging integration."""

    def test_worker_cli_enables_automatic_logging(self):
        """Test that worker node CLI automatically configures logging on startup."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Create worker CLI
            cli = WorkerNodeCLI()

            # Check that component_name is set correctly
            assert cli.component_name == "worker"

            # Mock the worker command to avoid running actual worker
            mock_args = MagicMock()
            mock_args.func = lambda args: None

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Run the CLI main method
                cli.main("test")

                # Verify that component logging was configured
                mock_configure.assert_called_once_with("worker")

    def test_worker_logging_with_template(self, tmp_path):
        """Test worker logging with template configuration."""
        # Create a minimal worker template
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
  jobmon.worker_node:
    handlers: [test_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_worker.yaml"
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
                worker_logger = logging.getLogger("jobmon.worker_node")
                worker_logger.handlers.clear()

                # Create CLI and trigger logging configuration
                cli = WorkerNodeCLI()

                # Mock the command execution to avoid running actual worker
                mock_args = MagicMock()
                mock_args.func = lambda args: None

                with patch.object(cli, "parse_args", return_value=mock_args):
                    cli.main("test")

                    # Check that worker logger was configured
                    assert len(worker_logger.handlers) > 0
                    assert worker_logger.level == logging.INFO

                    # Clean up
                    worker_logger.handlers.clear()

    def test_worker_logging_with_file_override(self, tmp_path):
        """Test worker logging with file-based configuration override."""
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
  jobmon.worker_node:
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
  jobmon.worker_node:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_worker.yaml"
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
                worker_logger = logging.getLogger("jobmon.worker_node")
                worker_logger.handlers.clear()

                # Create CLI and trigger logging configuration
                cli = WorkerNodeCLI()

                # Mock the command execution
                mock_args = MagicMock()
                mock_args.func = lambda args: None

                with patch.object(cli, "parse_args", return_value=mock_args):
                    cli.main("test")

                    # Check that override configuration was used (DEBUG level)
                    assert len(worker_logger.handlers) > 0
                    assert worker_logger.level == logging.DEBUG

                    # Clean up
                    worker_logger.handlers.clear()

    def test_worker_logging_failure_does_not_crash(self):
        """Test that worker starts successfully even if logging configuration fails."""
        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging",
            side_effect=Exception("Logging failed"),
        ):
            # Create CLI - should not raise exception
            cli = WorkerNodeCLI()
            assert cli.component_name == "worker"

            # Mock the command execution
            mock_args = MagicMock()
            mock_args.func = lambda args: "success"

            with patch.object(cli, "parse_args", return_value=mock_args):
                # Should complete successfully despite logging failure
                result = cli.main("test")
                assert result == "success"

    def test_worker_cli_flushes_otlp_on_exit(self):
        """Ensure worker CLI flushes OTLP telemetry after execution."""
        cli = WorkerNodeCLI()

        mock_args = MagicMock()
        mock_args.func = MagicMock(return_value="ok")

        with patch.object(cli, "parse_args", return_value=mock_args), patch(
            "jobmon.core.otlp.manager.otlp_flush_on_exit"
        ) as mock_flush_context:
            mock_enter = mock_flush_context.return_value.__enter__
            mock_exit = mock_flush_context.return_value.__exit__

            result = cli.main("worker_node_job")

            assert result == "ok"
            mock_flush_context.assert_called_once()
            mock_enter.assert_called_once()
            mock_exit.assert_called_once()

    def test_worker_logging_with_section_override(self, tmp_path):
        """Test worker logging with section-based configuration override."""
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
  jobmon.worker_node:
    handlers: [default_handler]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_worker.yaml"
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
                "worker": {"loggers": {"jobmon.worker_node": {"level": "DEBUG"}}}
            }
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                # Clear any existing handlers
                worker_logger = logging.getLogger("jobmon.worker_node")
                worker_logger.handlers.clear()

                # Create CLI and trigger logging configuration
                cli = WorkerNodeCLI()

                # Mock the command execution
                mock_args = MagicMock()
                mock_args.func = lambda args: None

                with patch.object(cli, "parse_args", return_value=mock_args):
                    cli.main("test")

                    # Check that configuration was applied
                    assert len(worker_logger.handlers) > 0

                    # Clean up
                    worker_logger.handlers.clear()

    def test_worker_short_lived_process_characteristics(self, tmp_path):
        """Test worker logging characteristics for short-lived processes."""
        # Create worker template with optimized handler settings
        # Use a simple StreamHandler to avoid OTLP dependencies in tests
        template_content = """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(name)s - %(levelname)s - %(message)s"

handlers:
  optimized_worker:
    class: logging.StreamHandler
    level: INFO
    formatter: simple
    # This would normally be an OTLP handler with batching optimizations

loggers:
  jobmon.worker_node:
    handlers: [optimized_worker]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_worker.yaml"
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

            with patch(
                "jobmon.core.config.logconfig_utils._get_component_template_path",
                return_value=str(template_file),
            ):
                # Clear any existing handlers
                worker_logger = logging.getLogger("jobmon.worker_node")
                worker_logger.handlers.clear()

                # Create CLI and trigger logging configuration
                cli = WorkerNodeCLI()

                # Mock the command execution
                mock_args = MagicMock()
                mock_args.func = lambda args: None

                with patch.object(cli, "parse_args", return_value=mock_args):
                    cli.main("test")

                    # Check that worker logger was configured
                    assert len(worker_logger.handlers) > 0
                    assert worker_logger.level == logging.INFO

                    # Verify that this would work for a short-lived process
                    # (The handler demonstrates the template structure for optimized settings)

                    # Clean up
                    worker_logger.handlers.clear()

    def test_worker_performance_startup_time(self):
        """Test worker startup time with logging configuration enabled."""
        import time

        with patch(
            "jobmon.core.config.logconfig_utils.configure_component_logging"
        ) as mock_configure:
            # Measure startup time
            start_time = time.time()

            # Create worker CLI
            cli = WorkerNodeCLI()

            # Trigger logging configuration manually (simulates main() call)
            cli.configure_component_logging()

            end_time = time.time()
            startup_time = end_time - start_time

            # Startup should be fast (less than 1 second for logging configuration)
            assert startup_time < 1.0

            # Verify logging was configured
            mock_configure.assert_called_once_with("worker")
            assert cli.component_name == "worker"

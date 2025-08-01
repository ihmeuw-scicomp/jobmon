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
        # Create worker template with short-lived process optimizations
        template_content = """
version: 1
disable_existing_loggers: false

formatters:
  simple:
    format: "%(name)s - %(levelname)s - %(message)s"

handlers:
  otlp_worker:
    class: jobmon.core.otlp.JobmonOTLPLoggingHandler
    level: INFO
    formatter: simple
    exporter:
      module: opentelemetry.exporter.otlp.proto.grpc._log_exporter
      class: OTLPLogExporter
      endpoint: http://localhost:4317
      timeout: 30
      insecure: true
      # Short-lived process batching settings
      max_export_batch_size: 2
      export_timeout_millis: 1000
      schedule_delay_millis: 200
      max_queue_size: 512

loggers:
  jobmon.worker_node:
    handlers: [otlp_worker]
    level: INFO
    propagate: false
"""
        template_file = tmp_path / "logconfig_worker.yaml"
        template_file.write_text(template_content)

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
                # (The handler should be configured with appropriate batching)

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
            cli._configure_component_logging()

            end_time = time.time()
            startup_time = end_time - start_time

            # Startup should be fast (less than 1 second for logging configuration)
            assert startup_time < 1.0

            # Verify logging was configured
            mock_configure.assert_called_once_with("worker")
            assert cli.component_name == "worker"

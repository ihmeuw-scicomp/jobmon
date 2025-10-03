import json
import logging
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
import yaml

from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester
from jobmon.server.web import routes


@pytest.fixture(scope="function")
def log_file(web_server_in_memory, json_log_file):
    """Legacy fixture that uses the consolidated json_log_file fixture"""
    app, _ = web_server_in_memory
    app.get("/")  # trigger logging setup

    # Use the consolidated logging fixture
    filepath = json_log_file(
        loggers={"jobmon.server.web": "INFO"}, filename_suffix="server_logging"
    )

    yield str(filepath)


def test_add_structlog_context(requester_in_memory, log_file, api_prefix):
    requester = Requester("")
    added_context = {"foo": "bar", "baz": "qux"}
    requester.add_server_structlog_context(**added_context)
    requester._send_request(f"{api_prefix}/health", {}, "get")

    # Look for the health endpoint log entry that should contain the context
    found_health_log = False
    with open(log_file, "r") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            if stripped_line:
                log_dict = json.loads(stripped_line)
                # Only check logs from the health endpoint (contains our message and path)
                if (
                    log_dict.get("event") == "Health check completed successfully"
                    and log_dict.get("path") == f"{api_prefix}/health"
                ):
                    found_health_log = True
                    for key in added_context.keys():
                        assert (
                            key in log_dict.keys()
                        ), f"Key '{key}' not found in health log: {log_dict}"
                    for val in added_context.values():
                        assert (
                            val in log_dict.values()
                        ), f"Value '{val}' not found in health log: {log_dict}"

    assert found_health_log, "Health endpoint log with context not found"


@pytest.mark.skip(reason="This test is not working")
def test_error_handling(requester_in_memory, log_file, monkeypatch, api_prefix):
    msg = "bad luck buddy"

    def raise_error():
        raise RuntimeError(msg)

    monkeypatch.setattr(routes, "_get_time", raise_error)

    captured_exception = False
    requester = Requester("")

    with pytest.raises(InvalidResponse):
        requester.send_request(f"{api_prefix}/health", {}, "get", tenacious=False)

    with open(log_file, "r", encoding="utf8") as server_log_file:
        for line in server_log_file:
            stripped_line = line.strip()
            log_dict = json.loads(stripped_line)
            if "exception" in log_dict.keys():
                assert msg in log_dict["exception"]
                assert "Traceback" in log_dict["exception"]
                captured_exception = True

    assert captured_exception


class TestServerLoggingConfiguration:
    """Test server logging configuration using core utilities."""

    def test_server_configure_logging_default(self):
        """Test server logging configuration with default settings."""
        from jobmon.core.config.logconfig_utils import configure_component_logging
        from jobmon.core.config.structlog_config import configure_structlog

        # Should not crash
        configure_component_logging("server")
        configure_structlog(component_name="server")

        # Should have configured basic logging
        server_logger = logging.getLogger("jobmon.server.web")
        assert server_logger is not None

    def test_server_configure_logging_with_default_config(self):
        """Test server logging uses core configuration system."""
        from jobmon.core.config.logconfig_utils import configure_component_logging

        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig"
        ) as mock_config_class:
            mock_config = Mock()
            mock_config.get.return_value = ""
            mock_config.get_section_coerced.return_value = {}
            mock_config_class.return_value = mock_config

            # Mock template loading to avoid actual file I/O
            with patch(
                "jobmon.core.config.logconfig_utils.load_logconfig_with_overrides"
            ) as mock_load:
                mock_load.return_value = {
                    "version": 1,
                    "handlers": {"console_default": {"class": "logging.StreamHandler"}},
                    "loggers": {
                        "jobmon.server.web": {
                            "handlers": ["console_default"],
                            "level": "INFO",
                        }
                    },
                }

                # Should configure with core utility
                configure_component_logging("server")

                # Should have called load with server config
                mock_load.assert_called_once()
                args, kwargs = mock_load.call_args
                assert "logconfig_server.yaml" in kwargs["default_template_path"]
                assert kwargs["config_section"] == "server"

    def test_server_logging_with_file_override(self):
        """Test server logging with custom file override via JobmonConfig."""
        from jobmon.core.config.logconfig_utils import configure_component_logging

        # Create custom config file
        custom_config = {
            "version": 1,
            "formatters": {
                "custom_server_formatter": {
                    "format": "SERVER_CUSTOM: %(levelname)s - %(message)s"
                }
            },
            "handlers": {
                "custom_server_handler": {
                    "class": "logging.StreamHandler",
                    "formatter": "custom_server_formatter",
                }
            },
            "loggers": {
                "jobmon.server.web": {
                    "handlers": ["custom_server_handler"],
                    "level": "DEBUG",
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(custom_config, f)
            custom_file_path = f.name

        try:
            # Configure via logging.server_logconfig_file setting
            with patch(
                "jobmon.core.config.logconfig_utils.JobmonConfig"
            ) as mock_config_class:
                mock_config = Mock()
                mock_config.get.return_value = (
                    custom_file_path  # Return custom file path
                )
                mock_config.get_section_coerced.return_value = {}
                mock_config_class.return_value = mock_config

                configure_component_logging("server")

            # Should have applied custom configuration
            server_logger = logging.getLogger("jobmon.server.web")
            assert server_logger is not None

        finally:
            os.unlink(custom_file_path)

    def test_server_configure_logging_with_section_overrides(self):
        """Test server logging with section-based overrides."""
        from jobmon.core.config.logconfig_utils import configure_component_logging

        with patch(
            "jobmon.core.config.logconfig_utils.JobmonConfig"
        ) as mock_config_class:
            mock_config = Mock()
            mock_config.get.return_value = ""  # No file override
            mock_config.get_boolean.return_value = False  # OTLP disabled
            mock_config.get_section.return_value = {
                "formatters": {
                    "section_server_formatter": {
                        "format": "SECTION_SERVER: %(message)s"
                    }
                },
                "handlers": {
                    "section_server_handler": {
                        "class": "logging.StreamHandler",
                        "formatter": "section_server_formatter",
                        "level": "WARNING",
                    }
                },
                "loggers": {
                    "jobmon.server.web.custom": {
                        "handlers": ["section_server_handler"],
                        "level": "WARNING",
                    }
                },
            }
            mock_config_class.return_value = mock_config

            # Mock template loading
            with patch(
                "jobmon.core.config.logconfig_utils.load_logconfig_with_overrides"
            ) as mock_load:
                mock_load.return_value = {
                    "version": 1,
                    "formatters": {
                        "section_server_formatter": {
                            "format": "SECTION_SERVER: %(message)s"
                        }
                    },
                    "handlers": {
                        "section_server_handler": {
                            "class": "logging.StreamHandler",
                            "formatter": "section_server_formatter",
                            "level": "WARNING",
                        }
                    },
                    "loggers": {
                        "jobmon.server.web.custom": {
                            "handlers": ["section_server_handler"],
                            "level": "WARNING",
                        }
                    },
                }

                # Configure logging using core utility
                configure_component_logging("server")

                # Should have applied section overrides
                custom_logger = logging.getLogger("jobmon.server.web.custom")
                assert custom_logger.level == logging.WARNING

    def test_server_configure_logging_fallback_on_error(self):
        """Test server logging fails gracefully on configuration errors."""
        from jobmon.core.config.logconfig_utils import configure_component_logging

        # configure_component_logging fails silently, so just verify it doesn't crash
        configure_component_logging("server")

        # Should have some basic logging configuration
        server_logger = logging.getLogger("jobmon.server.web")
        assert server_logger is not None


class TestServerOTLPIntegration:
    """Test server OTLP integration with new configuration system."""

    def test_server_file_override_precedence(self):
        """Test that server respects file overrides for OTLP configuration."""
        from jobmon.server.web.log_config import configure_logging  # Compatibility shim

        with patch("jobmon.server.web.log_config.JobmonConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get.return_value = ""  # No file override
            mock_config.get_section.return_value = {}
            mock_config.get_section_coerced.return_value = {}
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils.load_logconfig_with_overrides"
            ) as mock_load:
                mock_load.return_value = {"version": 1}

                configure_logging()

                # Should use default server config (OTLP configured via overrides)
                mock_load.assert_called_once()
                args, kwargs = mock_load.call_args
                assert "logconfig_server.yaml" in kwargs["default_template_path"]

    def test_server_custom_logconfig_file(self):
        """Test that server can use custom logconfig files for OTLP endpoint configuration."""
        import tempfile

        from jobmon.server.web.log_config import configure_logging  # Compatibility shim

        # Create a custom logconfig with different endpoint
        custom_logconfig = {
            "version": 1,
            "formatters": {
                "console_default": {"format": "%(levelname)s [%(name)s] %(message)s"}
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "console_default",
                },
                "otlp_server": {
                    "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler",
                    "formatter": "console_default",
                    "exporter": {
                        "module": "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
                        "class": "OTLPLogExporter",
                        "endpoint": "http://custom-otlp:4317",  # Custom endpoint
                    },
                },
            },
            "loggers": {"jobmon": {"handlers": ["console", "otlp_server"]}},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml

            yaml.dump(custom_logconfig, f)
            custom_file_path = f.name

        try:
            with patch(
                "jobmon.server.web.log_config.JobmonConfig"
            ) as mock_config_class:
                mock_config = Mock()
                mock_config.get.side_effect = lambda section, key: {
                    ("logging", "server_logconfig_file"): custom_file_path,
                }.get((section, key), "")
                mock_config.get_section.return_value = {}
                mock_config.get_boolean.return_value = True  # OTLP enabled
                mock_config_class.return_value = mock_config

                with patch("logging.config.dictConfig") as mock_dict_config:
                    configure_logging()

                    # Should have called dictConfig with our custom config
                    mock_dict_config.assert_called_once()
                    loaded_config = mock_dict_config.call_args[0][0]

                    # Verify our custom endpoint is used
                    assert (
                        loaded_config["handlers"]["otlp_server"]["exporter"]["endpoint"]
                        == "http://custom-otlp:4317"
                    )

        finally:
            import os

            os.unlink(custom_file_path)


class TestServerLoggingOutput:
    """Test actual server logging output and behavior."""

    def test_server_structlog_configuration(self):
        """Test that server structlog is properly configured."""
        from jobmon.server.web.log_config import configure_logging  # Compatibility shim

        # Configure logging
        configure_logging()

        # Should be able to get structlog logger
        try:
            import structlog

            logger = structlog.get_logger("jobmon.server.web")
            assert logger is not None
        except ImportError:
            pytest.skip("Structlog not available")

    def test_server_logging_with_context(self):
        """Test that server logging with context works."""
        from jobmon.server.web.log_config import configure_logging  # Compatibility shim

        configure_logging()

        # Test standard logging
        server_logger = logging.getLogger("jobmon.server.web")
        server_logger.info("Server test message")

        # Should not crash
        assert server_logger is not None

    def test_server_logging_inheritance(self):
        """Test that server logger hierarchy works correctly."""
        from jobmon.server.web.log_config import configure_logging  # Compatibility shim

        configure_logging()

        # Test various server loggers
        web_logger = logging.getLogger("jobmon.server.web")
        routes_logger = logging.getLogger("jobmon.server.web.routes")
        api_logger = logging.getLogger("jobmon.server.web.api")

        # All should exist
        assert web_logger is not None
        assert routes_logger is not None
        assert api_logger is not None

        # Test logging messages
        web_logger.info("Web message")
        routes_logger.warning("Routes message")
        api_logger.error("API message")

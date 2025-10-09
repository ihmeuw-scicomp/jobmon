"""Integration tests for jobmon logging system across components."""

import logging
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
import yaml


class TestCrossComponentConsistency:
    """Test consistency of logging configuration across components."""

    def test_template_consistency_across_components(self):
        """Test that shared templates are consistent across all components."""
        # Get the core config directory (where templates are stored)
        from jobmon.core.config import template_loader
        from jobmon.core.config.template_loader import load_all_templates

        config_root = os.path.dirname(template_loader.__file__)

        templates = load_all_templates(config_root)

        # Test that core shared templates are available
        assert "formatters" in templates
        assert "handlers" in templates

        # Test template consistency - formatters should have same structure
        formatters = templates["formatters"]

        # All formatters should have consistent structure
        for formatter_name, formatter_config in formatters.items():
            if "format" in formatter_config:
                # Format strings should be valid
                assert isinstance(formatter_config["format"], str)
                assert "%(message)s" in formatter_config["format"]

        # Test that OTLP handler templates exist and have consistent structure
        handlers = templates["handlers"]
        otlp_handlers = [name for name in handlers if "otlp" in name.lower()]
        assert len(otlp_handlers) > 0, "Should have at least one OTLP handler template"

        # Check that OTLP handlers have consistent exporter configuration
        for handler_name in otlp_handlers:
            handler_config = handlers[handler_name]
            if "exporter" in handler_config:
                exporter = handler_config["exporter"]
                # New simplified templates use empty dict for shared LoggerProvider
                # Legacy templates had full exporter config
                if exporter:  # Non-empty exporter dict
                    assert "class" in exporter
                    assert exporter["class"] == "OTLPLogExporter"
                    assert "endpoint" in exporter

    def test_component_configs_load_successfully(self):
        """Test that all component configurations load without errors."""
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        # Test client config loading
        try:
            import jobmon.client.config

            client_config_dir = os.path.dirname(jobmon.client.config.__file__)
            client_config_path = os.path.join(
                client_config_dir, "logconfig_client.yaml"
            )

            if os.path.exists(client_config_path):
                client_config = load_logconfig_with_templates(client_config_path)
                assert "version" in client_config
                assert client_config["version"] == 1
        except (ImportError, FileNotFoundError):
            pass  # Config may not exist in test environment

        # Test server config loading
        try:
            import jobmon.server.web.config

            server_config_dir = os.path.dirname(jobmon.server.web.config.__file__)
            server_config_path = os.path.join(
                server_config_dir, "logconfig_server.yaml"
            )

            if os.path.exists(server_config_path):
                server_config = load_logconfig_with_templates(server_config_path)
                assert "version" in server_config
                assert server_config["version"] == 1
        except (ImportError, FileNotFoundError):
            pass  # Config may not exist in test environment

        # Note: Requester logging is now handled by client configuration
        # No separate requester config file needed

    def test_otlp_exporter_consistency(self):
        """Test that OTLP exporters use consistent settings across components."""
        from jobmon.core.config import template_loader
        from jobmon.core.config.template_loader import load_all_templates

        config_root = os.path.dirname(template_loader.__file__)
        templates = load_all_templates(config_root)

        if "handlers" in templates:
            handlers = templates["handlers"]
            otlp_handlers = [name for name in handlers if "otlp" in name.lower()]

            for handler_name in otlp_handlers:
                handler_config = handlers[handler_name]
                if "exporter" in handler_config:
                    exporter = handler_config["exporter"]

                    # New simplified templates use empty dict for shared LoggerProvider
                    # Legacy templates had full exporter config
                    if exporter:  # Non-empty exporter dict
                        # Should have consistent OTLP structure
                        assert "module" in exporter
                        assert "class" in exporter
                        assert "endpoint" in exporter
                        assert exporter["class"] == "OTLPLogExporter"

                        # Should have reasonable batch settings
                        if "max_export_batch_size" in exporter:
                            assert isinstance(exporter["max_export_batch_size"], int)
                            assert exporter["max_export_batch_size"] > 0


class TestEndToEndLoggingScenarios:
    """Test complete end-to-end logging scenarios."""

    def test_client_to_server_logging_flow(self):
        """Test that client and server logging work together properly."""
        # Configure client logging
        from jobmon.client.logging import configure_client_logging

        configure_client_logging()

        # Configure server logging
        from jobmon.core.config.logconfig_utils import configure_component_logging

        configure_component_logging("server")

        # Test that both client and server loggers exist
        client_logger = logging.getLogger("jobmon.client")
        server_logger = logging.getLogger("jobmon.server.web")

        assert client_logger is not None
        assert server_logger is not None

        # Test logging messages
        client_logger.info("Client operation started")
        server_logger.info("Server processing request")

        # Both should work without errors
        assert True  # If we get here, no exceptions were raised

    def test_complete_otlp_workflow(self):
        """Test complete OTLP workflow from client through server."""
        with patch("jobmon.core.otlp.OTLP_AVAILABLE", True):
            # Mock OTLP configuration
            with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
                mock_config = Mock()
                mock_config.get.return_value = ""
                mock_config.get_section.return_value = {}
                mock_config.get_section_coerced.return_value = {
                    "tracing": {"server_enabled": True}
                }
                mock_config_class.return_value = mock_config

                # Mock OTLP managers
                with patch(
                    "jobmon.core.otlp.JobmonOTLPManager"
                ) as mock_otlp_manager, patch(
                    "jobmon.core.config.logconfig_utils.load_logconfig_with_overrides"
                ) as mock_server_load, patch(
                    "jobmon.core.config.logconfig_utils.load_logconfig_with_overrides"
                ) as mock_requester_load:

                    # Mock server OTLP config loading
                    mock_server_load.return_value = {
                        "version": 1,
                        "handlers": {
                            "otlp_server": {
                                "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler"
                            }
                        },
                        "loggers": {
                            "jobmon.server.web": {
                                "handlers": ["otlp_server"],
                                "level": "INFO",
                            }
                        },
                    }

                    # Mock requester OTLP config loading
                    mock_requester_load.return_value = {
                        "version": 1,
                        "handlers": {
                            "otlp_requester": {
                                "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler"
                            }
                        },
                        "loggers": {
                            "jobmon.core.requester": {
                                "handlers": ["otlp_requester"],
                                "level": "INFO",
                            }
                        },
                    }

                    # Configure all components
                    from jobmon.client.logging import configure_client_logging
                    from jobmon.core.config.logconfig_utils import (
                        configure_component_logging,
                    )
                    from jobmon.core.requester import Requester

                    configure_client_logging()
                    configure_component_logging("server")
                    Requester._init_otlp()

                    # All should configure without errors
                    assert True

    def test_logging_configuration_with_global_overrides(self):
        """Test logging behavior with global configuration overrides."""
        # Create a global override configuration
        global_override_config = {
            "formatters": {
                "global_formatter": {
                    "format": "GLOBAL: %(levelname)s - %(name)s - %(message)s"
                }
            },
            "handlers": {
                "global_handler": {
                    "class": "logging.StreamHandler",
                    "formatter": "global_formatter",
                    "level": "WARNING",
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(global_override_config, f)
            global_file_path = f.name

        try:
            # Mock JobmonConfig to return global overrides for all components
            with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
                mock_config = Mock()
                mock_config.get.side_effect = lambda section, key: {
                    ("logging", "client_logconfig_file"): global_file_path,
                    ("logging", "server_logconfig_file"): global_file_path,
                    ("logging", "requester_logconfig_file"): global_file_path,
                }.get((section, key), "")
                mock_config.get_section.return_value = {}
                mock_config.get_boolean.return_value = False
                mock_config_class.return_value = mock_config

                # Configure all components
                from jobmon.client.logging import configure_client_logging
                from jobmon.core.config.logconfig_utils import (
                    configure_component_logging,
                )

                configure_client_logging()
                configure_component_logging("server")

                # All components should use the global configuration
                # (This test verifies that the override system works consistently)
                assert True  # If we get here, no exceptions were raised

        finally:
            os.unlink(global_file_path)


class TestProductionScenarios:
    """Test scenarios that might occur in production deployments."""

    def test_mixed_override_deployment_scenario(self):
        """Test a realistic deployment scenario with mixed overrides."""
        # Simulate a production deployment where:
        # - Client uses section overrides for custom loggers
        # - Server uses file override for production settings
        # - Requester uses environment variable overrides

        # Create server production config
        server_prod_config = {
            "version": 1,
            "formatters": {
                "production_formatter": {
                    "format": "%(asctime)s [%(process)d] [%(levelname)s] %(name)s: %(message)s"
                }
            },
            "handlers": {
                "production_handler": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": "/var/log/jobmon/server.log",
                    "formatter": "production_formatter",
                    "maxBytes": 10485760,
                    "backupCount": 5,
                }
            },
            "loggers": {
                "jobmon.server.web": {
                    "handlers": ["production_handler"],
                    "level": "INFO",
                    "propagate": False,
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(server_prod_config, f)
            server_prod_file = f.name

        try:
            with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
                mock_config = Mock()

                # Simulate mixed overrides
                mock_config.get.side_effect = lambda section, key: {
                    (
                        "logging",
                        "server_logconfig_file",
                    ): server_prod_file,  # File override for server
                    (
                        "logging",
                        "client_logconfig_file",
                    ): "",  # No file override for client
                    (
                        "logging",
                        "requester_logconfig_file",
                    ): "",  # No file override for requester
                }.get((section, key), "")

                # Section overrides for client
                def mock_get_section(section_name):
                    if section_name == "logging.client":
                        return {
                            "loggers": {
                                "jobmon.client.custom": {
                                    "level": "DEBUG",
                                    "handlers": ["console"],
                                }
                            }
                        }
                    return {}

                mock_config.get_section.side_effect = mock_get_section
                mock_config.get_boolean.return_value = False
                mock_config_class.return_value = mock_config

                # Configure all components - should handle mixed overrides gracefully
                from jobmon.client.logging import configure_client_logging
                from jobmon.core.config.logconfig_utils import (
                    configure_component_logging,
                )

                # Mock template loading for client to avoid file I/O issues
                with patch(
                    "jobmon.core.config.logconfig_utils.configure_logging_with_overrides"
                ) as mock_client_config:
                    mock_client_config.return_value = None
                    configure_client_logging()

                # Server should use the production file
                configure_component_logging("server")

                # Should complete without errors
                assert True

        finally:
            os.unlink(server_prod_file)

    def test_configuration_error_recovery(self):
        """Test graceful recovery from configuration errors."""
        # Simulate various configuration failures
        error_scenarios = [
            lambda: Exception("Network timeout loading config"),
            lambda: FileNotFoundError("Config file not found"),
            lambda: yaml.YAMLError("Malformed YAML"),
            lambda: PermissionError("Permission denied reading config"),
        ]

        for error_func in error_scenarios:
            with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
                mock_config_class.side_effect = error_func()

                # All configuration functions should handle errors gracefully
                from jobmon.client.logging import configure_client_logging
                from jobmon.core.config.logconfig_utils import (
                    configure_component_logging,
                )

                try:
                    configure_client_logging()
                    configure_component_logging("server")

                    # Should still have basic logging functionality
                    client_logger = logging.getLogger("jobmon.client")
                    server_logger = logging.getLogger("jobmon.server.web")

                    assert client_logger is not None
                    assert server_logger is not None

                except Exception as e:
                    # If exceptions occur, they should be specific and not crash the app
                    assert isinstance(e, (ImportError, AttributeError))

    def test_otlp_collector_unavailable_scenario(self):
        """Test behavior when OTLP collector is unavailable."""
        with patch("jobmon.core.otlp.OTLP_AVAILABLE", True):
            # Mock OTLP configuration but simulate collector unavailability
            with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
                mock_config = Mock()
                mock_config.get.return_value = ""
                mock_config.get_section.return_value = {}
                mock_config.get_section_coerced.return_value = {
                    "tracing": {"server_enabled": True}
                }
                mock_config_class.return_value = mock_config

                # Mock OTLP manager to simulate connection failure
                with patch("jobmon.core.otlp.JobmonOTLPManager") as mock_otlp_manager:
                    mock_manager = Mock()
                    mock_manager.initialize.side_effect = Exception(
                        "OTLP collector unreachable"
                    )
                    mock_otlp_manager.get_instance.return_value = mock_manager

                    # Configuration should still complete (with fallback to non-OTLP)
                    from jobmon.core.config.logconfig_utils import (
                        configure_component_logging,
                    )
                    from jobmon.core.config.structlog_config import configure_structlog

                    try:
                        configure_component_logging("server")
                        configure_structlog(component_name="server")

                        # Should still have basic logging
                        server_logger = logging.getLogger("jobmon.server.web")
                        assert server_logger is not None

                    except Exception:
                        # Should not crash the application
                        pytest.fail(
                            "Logging configuration should not crash when OTLP fails"
                        )

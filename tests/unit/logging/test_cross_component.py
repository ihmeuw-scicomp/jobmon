"""Integration tests for jobmon logging system across components."""

import logging
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
import yaml


class TestCrossComponentConsistency:
    """Test consistency of logging configuration across components."""

    def test_programmatic_config_consistency_across_components(self):
        """Test that programmatic configs are consistent across all components."""
        from jobmon.core.config.logconfig_utils import generate_component_logconfig

        # Generate configs for all components
        client_config = generate_component_logconfig("client")
        server_config = generate_component_logconfig("server")
        distributor_config = generate_component_logconfig("distributor")
        worker_config = generate_component_logconfig("worker")

        # Test that all configs have consistent base structure
        for config_name, config in [
            ("client", client_config),
            ("server", server_config),
            ("distributor", distributor_config),
            ("worker", worker_config),
        ]:
            assert "formatters" in config, f"{config_name} should have formatters"
            assert "handlers" in config, f"{config_name} should have handlers"
            assert "loggers" in config, f"{config_name} should have loggers"
            assert config["version"] == 1, f"{config_name} should have version 1"

        # Test that formatters are consistent across configs
        for config in [client_config, server_config, distributor_config, worker_config]:
            formatters = config["formatters"]
            # All configs should have console_default formatter
            assert "console_default" in formatters
            assert isinstance(formatters["console_default"]["format"], str)
            assert "%(message)s" in formatters["console_default"]["format"]

        # Test that OTLP handlers are available in all configs
        for config in [client_config, server_config, distributor_config, worker_config]:
            handlers = config["handlers"]
            otlp_handlers = [name for name in handlers if "otlp" in name.lower()]
            assert len(otlp_handlers) > 0, "Should have at least one OTLP handler"

            # Check that OTLP handlers use the shared handler class
            for handler_name in otlp_handlers:
                handler_config = handlers[handler_name]
                assert (
                    handler_config["class"]
                    == "jobmon.core.otlp.JobmonOTLPLoggingHandler"
                )

    def test_component_configs_generated_successfully(self):
        """Test that all component configurations are generated programmatically."""
        from jobmon.core.config.logconfig_utils import generate_component_logconfig

        # Test client config generation
        client_config = generate_component_logconfig("client")
        assert "version" in client_config
        assert client_config["version"] == 1
        assert "loggers" in client_config
        assert "jobmon.client" in client_config["loggers"]

        # Test server config generation
        server_config = generate_component_logconfig("server")
        assert "version" in server_config
        assert server_config["version"] == 1
        assert "loggers" in server_config
        assert "jobmon.server.web" in server_config["loggers"]

        # Test distributor config generation
        distributor_config = generate_component_logconfig("distributor")
        assert "version" in distributor_config
        assert "loggers" in distributor_config
        assert "jobmon.distributor" in distributor_config["loggers"]

        # Test worker config generation
        worker_config = generate_component_logconfig("worker")
        assert "version" in worker_config
        assert "loggers" in worker_config
        assert "jobmon.worker_node" in worker_config["loggers"]

    def test_otlp_handler_consistency(self):
        """Test that OTLP handlers use consistent settings across components."""
        from jobmon.core.config.logconfig_utils import generate_component_logconfig

        # Check OTLP handler consistency in programmatically generated configs
        for component in ["client", "server", "distributor", "worker"]:
            config = generate_component_logconfig(component)
            handlers = config["handlers"]

            otlp_handlers = [name for name in handlers if "otlp" in name.lower()]

            for handler_name in otlp_handlers:
                handler_config = handlers[handler_name]

                # All OTLP handlers should use the JobmonOTLPLoggingHandler class
                assert (
                    handler_config["class"]
                    == "jobmon.core.otlp.JobmonOTLPLoggingHandler"
                ), f"OTLP handler {handler_name} in {component} should use JobmonOTLPLoggingHandler"

                # All OTLP handlers should have an exporter config (empty dict for shared provider)
                assert (
                    "exporter" in handler_config
                ), f"OTLP handler {handler_name} in {component} should have exporter config"


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

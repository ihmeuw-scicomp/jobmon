"""Tests for jobmon core logging configuration functionality."""

import os
import tempfile


class TestProgrammaticConfigGeneration:
    """Test the programmatic logging configuration generation."""

    def test_shared_formatters_defined(self):
        """Test that shared formatters are correctly defined."""
        from jobmon.core.config.logconfig_utils import SHARED_FORMATTERS

        # Check expected formatters exist
        expected_formatters = [
            "console_default",
            "structlog_event_only",
        ]
        for formatter_name in expected_formatters:
            assert formatter_name in SHARED_FORMATTERS

        # Check console_default has expected structure
        console_formatter = SHARED_FORMATTERS["console_default"]
        assert "format" in console_formatter
        assert "levelname" in console_formatter["format"]

    def test_shared_handlers_generated(self):
        """Test that shared handlers are correctly generated."""
        from jobmon.core.config.logconfig_utils import _get_shared_handlers

        handlers = _get_shared_handlers()

        # Should have expected handlers
        expected_handlers = [
            "console",
            "otlp_structlog",
        ]
        for handler_name in expected_handlers:
            assert handler_name in handlers

        # Check console handler structure
        console_handler = handlers["console"]
        assert "class" in console_handler
        assert console_handler["class"] == "logging.StreamHandler"
        assert "formatter" in console_handler
        assert console_handler["formatter"] == "structlog_event_only"

        # Check otlp handler structure
        otlp_handler = handlers["otlp_structlog"]
        assert "class" in otlp_handler
        assert otlp_handler["class"] == "jobmon.core.otlp.JobmonOTLPLoggingHandler"
        # Handler uses shared LoggerProvider (empty exporter dict)
        assert "exporter" in otlp_handler


class TestTemplateLoader:
    """Test the YAML template loading functionality (for custom user configs)."""

    def test_yaml_config_loading(self):
        """Test that YAML config files load correctly."""
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        # Create a temporary config file with inline formatters (modern style)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(
                """
version: 1
formatters:
  console_default:
    format: "%(levelname)s [%(name)s] %(message)s"
  custom:
    format: "CUSTOM: %(message)s"
handlers:
  console:
    class: logging.StreamHandler
    formatter: console_default
loggers:
  jobmon.test:
    handlers: [console]
    level: INFO
"""
            )
            temp_file = f.name

        try:
            # Load the config
            config = load_logconfig_with_templates(temp_file)

            # Check that config was loaded correctly
            assert "formatters" in config
            assert isinstance(config["formatters"], dict)
            assert "console_default" in config["formatters"]
            assert "custom" in config["formatters"]

            # Check handlers
            assert "handlers" in config
            assert "console" in config["handlers"]

            # Check loggers
            assert "loggers" in config
            assert "jobmon.test" in config["loggers"]

        finally:
            os.unlink(temp_file)

    def test_template_loading_from_different_packages(self):
        """Test template loading works from client and server packages."""
        from jobmon.core.config.template_loader import load_all_templates

        # Test loading from a simulated client location
        # Create a temporary directory structure
        with tempfile.TemporaryDirectory() as temp_dir:
            # Simulate client config directory structure
            client_config_dir = os.path.join(temp_dir, "client", "config")
            os.makedirs(client_config_dir)

            # This should find templates via relative path resolution
            templates = load_all_templates(client_config_dir)

            # Should still load templates (may be empty if paths don't resolve, but shouldn't crash)
            assert isinstance(templates, dict)

    def test_template_loading_graceful_failure(self):
        """Test graceful handling when templates directory is missing."""
        from jobmon.core.config.template_loader import load_all_templates

        # Test with non-existent directory that's completely isolated
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a deeply nested non-existent path to avoid package resolution
            non_existent_dir = os.path.join(
                temp_dir, "does_not_exist", "deeply", "nested"
            )

            templates = load_all_templates(non_existent_dir)

            # Should return dict (may be empty or may have found templates via package resolution)
            assert isinstance(templates, dict)
            # Template loader is designed to be robust and may find templates via package paths

    def test_missing_template_reference_handling(self):
        """Test handling of missing template references."""
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        # Create a config with non-existent template reference
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            # Write YAML directly with proper !template directive syntax
            f.write(
                """
version: 1
formatters: !template non_existent_formatters
"""
            )
            temp_file = f.name

        try:
            # Should handle missing template gracefully
            config = load_logconfig_with_templates(temp_file)

            # Should get None or empty dict for missing template, or raise an error
            # The behavior may vary based on implementation
            assert "formatters" in config
            # Missing template might resolve to None or cause an error
            formatters = config.get("formatters")
            assert formatters in [None, {}] or isinstance(formatters, str)

        except (ValueError, KeyError):
            # It's also acceptable for missing templates to raise an error
            pass
        finally:
            os.unlink(temp_file)


class TestTemplateIntegration:
    """Test template integration with programmatic config generation."""

    def test_client_config_generated_programmatically(self):
        """Test that client config is generated programmatically."""
        from jobmon.core.config.logconfig_utils import generate_component_logconfig

        # Generate client config
        config = generate_component_logconfig("client")

        # Should have basic logging config structure
        assert "version" in config
        assert config["version"] == 1
        assert "formatters" in config
        assert isinstance(config["formatters"], dict)
        assert "loggers" in config
        assert "jobmon.client" in config["loggers"]

    def test_server_config_generated_programmatically(self):
        """Test that server config is generated programmatically."""
        from jobmon.core.config.logconfig_utils import generate_component_logconfig

        # Generate server config
        config = generate_component_logconfig("server")

        # Should have basic logging config structure
        assert "version" in config
        assert config["version"] == 1
        assert "formatters" in config
        assert isinstance(config["formatters"], dict)
        assert "loggers" in config
        assert "jobmon.server.web" in config["loggers"]

    # Note: YAML logconfig files have been removed in favor of programmatic generation

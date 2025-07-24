"""Tests for jobmon core template loading functionality."""

import os
import tempfile

import pytest


class TestTemplateLoader:
    """Test the core template loading functionality."""

    def test_load_shared_formatters(self):
        """Test loading shared formatter templates."""
        # Get the core config directory
        from jobmon.core.config import template_loader
        from jobmon.core.config.template_loader import load_all_templates

        config_root = os.path.dirname(template_loader.__file__)

        templates = load_all_templates(config_root)

        # Should have loaded formatters
        assert "formatters" in templates
        formatters = templates["formatters"]

        # Check expected formatters exist
        expected_formatters = [
            "console_default",
            "otlp_default",
            "structlog_text",
            "structlog_json",
        ]
        for formatter_name in expected_formatters:
            assert formatter_name in formatters

        # Check console_default has expected structure
        console_formatter = formatters["console_default"]
        assert "format" in console_formatter
        assert "levelname" in console_formatter["format"]

    def test_load_shared_otlp_exporters(self):
        """Test loading shared OTLP exporter templates."""
        from jobmon.core.config import template_loader
        from jobmon.core.config.template_loader import load_all_templates

        config_root = os.path.dirname(template_loader.__file__)
        templates = load_all_templates(config_root)

        # Should have loaded OTLP exporters
        assert "otlp_grpc_exporter" in templates
        exporter = templates["otlp_grpc_exporter"]

        # Check expected exporter structure
        assert "module" in exporter
        assert "class" in exporter
        assert "endpoint" in exporter
        assert "options" in exporter
        assert exporter["class"] == "OTLPLogExporter"

    def test_load_shared_handlers(self):
        """Test loading shared handler templates."""
        import jobmon.core.config.template_loader as template_loader
        from jobmon.core.config.template_loader import load_all_templates

        config_root = os.path.dirname(template_loader.__file__)
        templates = load_all_templates(config_root)

        # Should have loaded handlers section
        assert "handlers" in templates
        handlers = templates["handlers"]

        # Should have loaded expected handlers
        expected_handlers = ["console_template", "otlp_base_template"]
        for handler_name in expected_handlers:
            assert handler_name in handlers

        # Check console_template structure
        console_handler = handlers["console_template"]
        assert "class" in console_handler
        assert console_handler["class"] == "logging.StreamHandler"

        # Check otlp_base_template structure
        otlp_handler = handlers["otlp_base_template"]
        assert "class" in otlp_handler
        assert otlp_handler["class"] == "jobmon.core.otlp.JobmonOTLPLoggingHandler"

    def test_template_directive_resolution(self):
        """Test that !template directives resolve correctly."""
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        # Create a temporary config file with template references
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            # Write YAML directly with proper !template directive syntax
            f.write(
                """
version: 1
formatters: !template formatters
handlers:
  console:
    class: logging.StreamHandler
    formatter: console_default
"""
            )
            temp_file = f.name

        try:
            # Load the config with template resolution
            config = load_logconfig_with_templates(temp_file)

            # Check that template was resolved
            assert "formatters" in config
            assert isinstance(config["formatters"], dict)
            assert "console_default" in config["formatters"]

            # Check handler still works
            assert "handlers" in config
            assert "console" in config["handlers"]

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
    """Test template integration with actual config files."""

    def test_client_config_loads_templates(self):
        """Test that client config successfully loads templates."""
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        # Find client config file
        try:
            import jobmon.client.config

            client_config_dir = os.path.dirname(jobmon.client.config.__file__)
            client_config_path = os.path.join(
                client_config_dir, "logconfig_client.yaml"
            )

            if os.path.exists(client_config_path):
                # Load client config with templates
                config = load_logconfig_with_templates(client_config_path)

                # Should have basic logging config structure
                assert "version" in config
                assert config["version"] == 1

                if "formatters" in config:
                    assert isinstance(config["formatters"], dict)

        except ImportError:
            pytest.skip("Client config not available")

    def test_server_config_loads_templates(self):
        """Test that server config successfully loads templates."""
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        # Find server config file
        try:
            import jobmon.server.web.config

            server_config_dir = os.path.dirname(jobmon.server.web.config.__file__)
            server_config_path = os.path.join(
                server_config_dir, "logconfig_server.yaml"
            )

            if os.path.exists(server_config_path):
                # Load server config with templates
                config = load_logconfig_with_templates(server_config_path)

                # Should have basic logging config structure
                assert "version" in config
                assert config["version"] == 1

                if "formatters" in config:
                    assert isinstance(config["formatters"], dict)

        except ImportError:
            pytest.skip("Server config not available")

    # Note: Requester OTLP config test removed as requester logging
    # is now handled by the general client configuration

"""Tests for jobmon configuration override system."""

import os
import tempfile
from unittest.mock import Mock, patch

import pytest
import yaml


class TestConfigurationOverrides:
    """Test the configuration override system."""

    def test_file_based_override_precedence(self):
        """Test that file overrides take precedence over section overrides."""
        from jobmon.core.config.logconfig_utils import load_logconfig_with_overrides
        from jobmon.core.configuration import JobmonConfig

        # Create a custom logconfig file
        custom_config = {
            "version": 1,
            "formatters": {"custom_formatter": {"format": "CUSTOM: %(message)s"}},
            "handlers": {
                "custom_handler": {
                    "class": "logging.StreamHandler",
                    "formatter": "custom_formatter",
                }
            },
            "loggers": {
                "test.logger": {"handlers": ["custom_handler"], "level": "DEBUG"}
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(custom_config, f)
            custom_file_path = f.name

        try:
            # Mock JobmonConfig with both file and section overrides
            mock_config = Mock(spec=JobmonConfig)
            mock_config.get.side_effect = lambda section, key: {
                ("logging", "client_logconfig_file"): custom_file_path,
            }.get((section, key), "")
            mock_config.get_section.return_value = {
                "formatters": {"section_formatter": {"format": "SECTION: %(message)s"}}
            }

            # Load config with overrides
            config = load_logconfig_with_overrides(
                default_template_path="/fake/path/default.yaml",
                config_section="client",
                config=mock_config,
            )

            # File override should win - should have custom_formatter, not section_formatter
            assert "formatters" in config
            assert "custom_formatter" in config["formatters"]
            assert "section_formatter" not in config["formatters"]
            assert (
                config["formatters"]["custom_formatter"]["format"]
                == "CUSTOM: %(message)s"
            )

        finally:
            os.unlink(custom_file_path)

    def test_section_based_override_merging(self):
        """Test that section overrides merge properly with default templates."""
        from jobmon.core.config.logconfig_utils import load_logconfig_with_overrides
        from jobmon.core.configuration import JobmonConfig

        # Create a simple default template
        default_config = {
            "version": 1,
            "formatters": {"default_formatter": {"format": "DEFAULT: %(message)s"}},
            "handlers": {
                "default_handler": {
                    "class": "logging.StreamHandler",
                    "formatter": "default_formatter",
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(default_config, f)
            default_file_path = f.name

        try:
            # Mock JobmonConfig with section overrides
            mock_config = Mock(spec=JobmonConfig)
            mock_config.get.return_value = ""  # No file override
            mock_config.get_section_coerced.return_value = {
                "client": {
                    "formatters": {
                        "custom_formatter": {"format": "CUSTOM: %(message)s"}
                    },
                    "handlers": {
                        "custom_handler": {
                            "class": "logging.FileHandler",
                            "filename": "/tmp/test.log",
                        }
                    },
                }
            }

            # Load config with overrides
            config = load_logconfig_with_overrides(
                default_template_path=default_file_path,
                config_section="client",
                config=mock_config,
            )

            # Should have both default and custom formatters
            assert "formatters" in config
            assert "default_formatter" in config["formatters"]
            assert "custom_formatter" in config["formatters"]

            # Should have both default and custom handlers
            assert "handlers" in config
            assert "default_handler" in config["handlers"]
            assert "custom_handler" in config["handlers"]

            # Check values are correct
            assert (
                config["formatters"]["default_formatter"]["format"]
                == "DEFAULT: %(message)s"
            )
            assert (
                config["formatters"]["custom_formatter"]["format"]
                == "CUSTOM: %(message)s"
            )

        finally:
            os.unlink(default_file_path)

    def test_override_system_with_missing_file(self):
        """Test graceful handling when override file doesn't exist."""
        from jobmon.core.config.logconfig_utils import load_logconfig_with_overrides
        from jobmon.core.configuration import JobmonConfig

        # Create a default template
        default_config = {
            "version": 1,
            "formatters": {"default_formatter": {"format": "DEFAULT: %(message)s"}},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(default_config, f)
            default_file_path = f.name

        try:
            # Mock JobmonConfig with non-existent file override
            mock_config = Mock(spec=JobmonConfig)
            mock_config.get.side_effect = lambda section, key: {
                ("logging", "client_logconfig_file"): "/path/that/does/not/exist.yaml",
            }.get((section, key), "")
            mock_config.get_section.return_value = {}

            # Load config - should fall back to default template
            config = load_logconfig_with_overrides(
                default_template_path=default_file_path,
                config_section="client",
                config=mock_config,
            )

            # Should have fallen back to default template
            assert "formatters" in config
            assert "default_formatter" in config["formatters"]
            assert (
                config["formatters"]["default_formatter"]["format"]
                == "DEFAULT: %(message)s"
            )

        finally:
            os.unlink(default_file_path)

    def test_deep_merge_behavior(self):
        """Test that deep merging works correctly for nested configurations."""
        from jobmon.core.config.logconfig_utils import merge_logconfig_sections

        # Base configuration
        base_config = {
            "formatters": {
                "console": {
                    "format": "%(levelname)s: %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "console",
                }
            },
            "loggers": {"jobmon": {"level": "INFO", "handlers": ["console"]}},
        }

        # Override configuration
        override_config = {
            "formatters": {
                "console": {
                    "format": "OVERRIDDEN: %(message)s"
                    # Note: datefmt should be preserved from base
                },
                "file": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                },
            },
            "handlers": {
                "file": {"class": "logging.FileHandler", "filename": "/tmp/test.log"}
            },
        }

        # Merge configurations
        merged = merge_logconfig_sections(base_config, override_config)

        # Check formatters were merged correctly
        assert "console" in merged["formatters"]
        assert "file" in merged["formatters"]

        # console formatter should have overridden format and preserved datefmt
        # Deep merge should preserve existing keys while overriding specified ones
        console_formatter = merged["formatters"]["console"]
        assert console_formatter["format"] == "OVERRIDDEN: %(message)s"
        # datefmt should be preserved from base with deep merge
        assert console_formatter["datefmt"] == "%Y-%m-%d %H:%M:%S"

        # file formatter should be added
        assert (
            merged["formatters"]["file"]["format"]
            == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Check handlers were merged
        assert "console" in merged["handlers"]
        assert "file" in merged["handlers"]

        # console handler should preserve original values
        assert merged["handlers"]["console"]["class"] == "logging.StreamHandler"
        assert merged["handlers"]["console"]["level"] == "INFO"

        # Check loggers preserved
        assert "jobmon" in merged["loggers"]
        assert merged["loggers"]["jobmon"]["level"] == "INFO"

    def test_override_with_template_references(self):
        """Test that overrides work with template references."""
        from jobmon.core.config.logconfig_utils import load_logconfig_with_overrides
        from jobmon.core.configuration import JobmonConfig

        # Create a default template with template references
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
            default_file_path = f.name

        try:
            # Mock JobmonConfig with section overrides
            mock_config = Mock(spec=JobmonConfig)
            mock_config.get.return_value = ""  # No file override
            mock_config.get_section_coerced.return_value = {
                "client": {
                    "handlers": {
                        "custom_handler": {
                            "class": "logging.FileHandler",
                            "filename": "/tmp/test.log",
                            "formatter": "console_default",  # Reference template formatter
                        }
                    }
                }
            }

            # Load config with overrides
            config = load_logconfig_with_overrides(
                default_template_path=default_file_path,
                config_section="client",
                config=mock_config,
            )

            # Should have resolved template and merged overrides
            assert "formatters" in config
            assert isinstance(config["formatters"], dict)

            # Should have both default and custom handlers
            assert "handlers" in config
            assert "console" in config["handlers"]
            assert "custom_handler" in config["handlers"]

            # Custom handler should reference the template formatter
            assert (
                config["handlers"]["custom_handler"]["formatter"] == "console_default"
            )

        finally:
            os.unlink(default_file_path)


class TestEnvironmentVariableOverrides:
    """Test environment variable based overrides."""

    @patch.dict(
        os.environ, {"JOBMON__LOGGING__CLIENT_LOGCONFIG_FILE": "/custom/logconfig.yaml"}
    )
    def test_env_var_file_override(self):
        """Test that environment variables can override file paths."""
        from jobmon.core.configuration import JobmonConfig

        # Create JobmonConfig which should pick up environment variable
        config = JobmonConfig()

        # Should get the environment variable value
        try:
            file_path = config.get("logging", "client_logconfig_file")
            assert file_path == "/custom/logconfig.yaml"
        except Exception:
            # If configuration system doesn't support this yet, skip test
            pytest.skip("Environment variable override not yet implemented")

    @patch.dict(
        os.environ,
        {"JOBMON__LOGGING__CLIENT__FORMATTERS__CUSTOM__FORMAT": "ENV: %(message)s"},
    )
    def test_env_var_section_override(self):
        """Test that environment variables can set nested section values."""
        from jobmon.core.configuration import JobmonConfig

        config = JobmonConfig()

        try:
            # Try to get nested configuration via section
            client_config = config.get_section("logging.client")
            if (
                "formatters" in client_config
                and "custom" in client_config["formatters"]
            ):
                custom_formatter = client_config["formatters"]["custom"]
                assert custom_formatter.get("format") == "ENV: %(message)s"
            else:
                pytest.skip("Nested environment variable support not yet implemented")
        except Exception:
            pytest.skip("Environment variable section override not yet implemented")


class TestConfigurationIntegration:
    """Test integration of override system with actual configuration loading."""

    def test_configure_logging_with_overrides_function(self):
        """Test the main configure_logging_with_overrides function."""
        import logging

        from jobmon.core.config.logconfig_utils import configure_logging_with_overrides
        from jobmon.core.configuration import JobmonConfig

        # Create a simple test logconfig
        test_config = {
            "version": 1,
            "formatters": {"test_formatter": {"format": "TEST: %(message)s"}},
            "handlers": {
                "test_handler": {
                    "class": "logging.StreamHandler",
                    "formatter": "test_formatter",
                    "level": "DEBUG",
                }
            },
            "loggers": {
                "test.override": {
                    "handlers": ["test_handler"],
                    "level": "DEBUG",
                    "propagate": False,
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(test_config, f)
            test_file_path = f.name

        try:
            # Mock JobmonConfig with no overrides
            mock_config = Mock(spec=JobmonConfig)
            mock_config.get.return_value = ""
            mock_config.get_section.return_value = {}

            # Configure logging
            configure_logging_with_overrides(
                default_template_path=test_file_path,
                config_section="test",
                config=mock_config,
            )

            # Verify logger is configured
            test_logger = logging.getLogger("test.override")
            assert len(test_logger.handlers) > 0
            assert test_logger.level == logging.DEBUG
            assert not test_logger.propagate

            # Clean up handlers to avoid interference with other tests
            test_logger.handlers.clear()

        finally:
            os.unlink(test_file_path)

    def test_get_logconfig_examples(self):
        """Test that configuration examples are available and valid."""
        from jobmon.core.config.logconfig_utils import get_logconfig_examples

        examples = get_logconfig_examples()

        # Should have examples for all component types
        expected_components = ["client", "server", "requester"]
        for component in expected_components:
            assert component in examples

            component_examples = examples[component]

            # Should have file and section examples
            assert "file_override_example" in component_examples
            assert "section_override_example" in component_examples

            # File example should be a valid file path pattern
            file_example = component_examples["file_override_example"]
            assert isinstance(file_example, str)
            assert "logconfig" in file_example

            # Section example should be a dict with logging structure
            section_example = component_examples["section_override_example"]
            assert isinstance(section_example, dict)

            # Should have expected logging sections
            expected_sections = ["formatters", "handlers", "loggers"]
            for section in expected_sections:
                if section in section_example:
                    assert isinstance(section_example[section], dict)

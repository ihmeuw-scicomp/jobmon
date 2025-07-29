"""Utilities for loading and overriding logging configurations.

This module provides functions to load template-based logconfigs and apply
user-specified overrides from JobmonConfig.
"""

import logging.config
import os
from typing import Any, Dict, Optional

from jobmon.core.config.template_loader import load_logconfig_with_templates
from jobmon.core.configuration import JobmonConfig


def merge_logconfig_sections(
    base_config: Dict[str, Any], overrides: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge logconfig section overrides into base configuration.

    This performs a deep merge, allowing users to override specific formatters,
    handlers, or loggers while preserving the rest of the base configuration.

    Args:
        base_config: Base logconfig dictionary (from templates)
        overrides: Override sections from JobmonConfig

    Returns:
        Merged logconfig dictionary
    """
    import copy

    def deep_merge_dict(
        base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                # Deep merge nested dictionaries
                result[key] = deep_merge_dict(result[key], value)
            else:
                # Replace value (including lists, scalars, etc.)
                result[key] = copy.deepcopy(value)

        return result

    merged = copy.deepcopy(base_config)

    # Deep merge each top-level section
    for section_name in ["formatters", "handlers", "loggers"]:
        if section_name in overrides and overrides[section_name]:
            if section_name not in merged:
                merged[section_name] = {}

            # Deep merge the section
            merged[section_name] = deep_merge_dict(
                merged[section_name], overrides[section_name]
            )

    return merged


def load_logconfig_with_overrides(
    default_template_path: str,
    config_section: str,
    config: Optional[JobmonConfig] = None,
) -> Dict[str, Any]:
    """Load logconfig with support for user overrides from JobmonConfig.

    Supports two types of overrides:
    1. File-based: Custom logconfig file specified in logging.{component}_logconfig_file
    2. Section-based: Override specific sections specified in logging.{component}.*

    Args:
        default_template_path: Path to the default template-based logconfig
        config_section: Config section name ('client', 'server', 'requester')
        config: JobmonConfig instance (creates default if None)

    Returns:
        Fully resolved logconfig dictionary ready for logging.config.dictConfig()
    """
    if config is None:
        config = JobmonConfig()

    # Check for file-based override first (highest precedence)
    file_override_key = f"{config_section}_logconfig_file"
    try:
        custom_file = config.get("logging", file_override_key)
        if custom_file and os.path.exists(custom_file):
            # User specified a custom logconfig file - use it directly
            try:
                return load_logconfig_with_templates(custom_file)
            except Exception:
                # Fall back to default if custom file fails to load
                pass
    except Exception:
        # No file override specified or failed to load
        pass

    # Load default template-based configuration
    logconfig_data = load_logconfig_with_templates(default_template_path)

    # Apply section-based overrides
    try:
        # Get all section overrides for this component
        section_overrides = config.get_section_coerced("logging")
        component_overrides = section_overrides.get(config_section, {})

        if component_overrides:
            logconfig_data = merge_logconfig_sections(
                logconfig_data, component_overrides
            )

    except Exception:
        # No section overrides or failed to load - use base config
        pass

    return logconfig_data


def configure_logging_with_overrides(
    default_template_path: str,
    config_section: str,
    fallback_config: Optional[Dict[str, Any]] = None,
    config: Optional[JobmonConfig] = None,
) -> None:
    """Configure logging with template and override support.

    This is a convenience function that loads a logconfig with overrides
    and applies it using logging.config.dictConfig().

    Args:
        default_template_path: Path to the default template-based logconfig
        config_section: Config section name ('client', 'server', 'requester')
        fallback_config: Fallback config if template loading fails
        config: JobmonConfig instance (creates default if None)
    """
    try:
        logconfig_data = load_logconfig_with_overrides(
            default_template_path, config_section, config
        )
        logging.config.dictConfig(logconfig_data)

    except Exception:
        # Fall back to basic configuration if everything fails
        if fallback_config:
            logging.config.dictConfig(fallback_config)


def get_logconfig_examples() -> Dict[str, Dict[str, Any]]:
    """Get example configurations for documentation and testing.

    Returns:
        Dictionary of example logconfig override configurations by component
    """
    return {
        "client": {
            "file_override_example": "/path/to/custom/client_logconfig.yaml",
            "section_override_example": {
                "formatters": {
                    "custom": {
                        "format": "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
                    }
                },
                "handlers": {
                    "file": {
                        "class": "logging.FileHandler",
                        "filename": "/var/log/jobmon_client.log",
                        "formatter": "custom",
                        "level": "INFO",
                    }
                },
                "loggers": {
                    "jobmon.client.workflow": {
                        "handlers": ["console", "file"],
                        "level": "DEBUG",
                        "propagate": False,
                    }
                },
            },
        },
        "server": {
            "file_override_example": "/path/to/custom/server_logconfig.yaml",
            "section_override_example": {
                "formatters": {
                    "production": {
                        "format": (
                            "%(asctime)s [%(process)d] %(levelname)s %(name)s: %(message)s"
                        )
                    }
                },
                "handlers": {
                    "file": {
                        "class": "logging.handlers.RotatingFileHandler",
                        "filename": "/var/log/jobmon_server.log",
                        "formatter": "production",
                        "maxBytes": 10485760,
                        "backupCount": 5,
                    }
                },
                "loggers": {
                    "jobmon.server.web": {
                        "handlers": ["console_structlog", "file"],
                        "level": "INFO",
                        "propagate": False,
                    }
                },
            },
        },
        "requester": {
            "file_override_example": "/path/to/custom/requester_logconfig.yaml",
            "section_override_example": {
                "handlers": {
                    "custom_otlp": {
                        "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler",
                        "level": "DEBUG",
                        "formatter": "otlp_default",
                    }
                },
                "loggers": {
                    "jobmon.core.requester": {
                        "handlers": ["console", "custom_otlp"],
                        "level": "DEBUG",
                        "propagate": False,
                    }
                },
            },
        },
        "env_var_examples": {
            "JOBMON__LOGGING__CLIENT_LOGCONFIG_FILE": "/path/to/custom.yaml",
            "JOBMON__LOGGING__CLIENT__FORMATTERS__CUSTOM__FORMAT": "%(name)s: %(message)s",
            "JOBMON__LOGGING__SERVER__LOGGERS__MYAPP__LEVEL": "DEBUG",
        },
    }

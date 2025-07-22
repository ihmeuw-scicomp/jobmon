"""Configure Logging for structlog, OpenTelemetry, etc."""

import logging
import logging.config
import os
from typing import Dict, List, Optional

import structlog
import yaml

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError


def configure_structlog(extra_processors: Optional[List] = None) -> None:
    """Configure structlog processors."""
    if extra_processors is None:
        extra_processors = []
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            *extra_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def load_logging_config_from_file(filepath: str) -> Dict:
    """Load logging configuration from a YAML file."""
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return yaml.safe_load(f)
    else:
        return {}


def merge_logging_configs(base_config: Dict, new_config: Dict) -> None:
    """Recursively merge new_config into base_config."""
    for key, value in new_config.items():
        if (
            key in base_config
            and isinstance(base_config[key], dict)
            and isinstance(value, dict)
        ):
            merge_logging_configs(base_config[key], value)
        else:
            base_config[key] = value


def configure_logging(
    dict_config: Optional[Dict] = None, file_config: str = ""
) -> None:
    """Setup logging with automatic logconfig selection and user override support.

    This function supports:
    1. Explicit dict_config or file_config (highest precedence)
    2. User file overrides via logging.server_logconfig_file
    3. User section overrides via logging.server.*
    4. Automatic OTLP selection based on otlp.web_enabled
    5. Legacy OTLP endpoint override via otlp.endpoint
    6. Fallback to default templates and legacy config

    Args:
        dict_config: Explicit logging configuration dictionary
        file_config: Path to explicit logging configuration file
    """
    logging_config = dict_config

    # If explicit dict_config or file_config provided, use that (highest precedence)
    if logging_config is None and file_config:
        try:
            from jobmon.core.config.template_loader import load_logconfig_with_templates

            logging_config = load_logconfig_with_templates(file_config)
        except ImportError:
            logging_config = load_logging_config_from_file(file_config)

    # Auto-select and load with user overrides
    if logging_config is None:
        try:
            config = JobmonConfig()
        except Exception:
            # Fall back to default configuration if JobmonConfig fails
            logging_config = {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": default_formatters.copy(),
                "handlers": default_handlers.copy(),
                "loggers": default_loggers.copy(),
            }
            logging.config.dictConfig(logging_config)
            return

        try:
            # Check for user file override first
            custom_file = config.get("logging", "server_logconfig_file")
            if custom_file and os.path.exists(custom_file):
                from jobmon.core.config.template_loader import (
                    load_logconfig_with_templates,
                )

                logging_config = load_logconfig_with_templates(custom_file)
            else:
                # Use override utilities with automatic OTLP selection
                from jobmon.core.config.logconfig_utils import (
                    load_logconfig_with_overrides,
                )

                # Check if OTLP is enabled for server
                try:
                    otlp_enabled = config.get_boolean("otlp", "web_enabled")
                except ConfigError:
                    otlp_enabled = False

                # Determine which template to use
                current_dir = os.path.dirname(__file__)
                if otlp_enabled:
                    default_template_path = os.path.join(
                        current_dir, "config/logconfig_server_otlp.yaml"
                    )
                else:
                    default_template_path = os.path.join(
                        current_dir, "config/logconfig_server.yaml"
                    )

                # Load with override support
                logging_config = load_logconfig_with_overrides(
                    default_template_path=default_template_path,
                    config_section="server",
                    config=config,
                )

                # Legacy: Override endpoint in OTLP handlers if configured
                if otlp_enabled:
                    try:
                        override_endpoint = config.get("otlp", "endpoint")
                        if override_endpoint and logging_config:
                            handlers = logging_config.get("handlers", {})
                            for handler_name in ["otlp_server", "otlp_structlog"]:
                                if (
                                    handler_name in handlers
                                    and "exporter" in handlers[handler_name]
                                ):
                                    handlers[handler_name]["exporter"][
                                        "endpoint"
                                    ] = override_endpoint
                    except ConfigError:
                        pass  # Use default endpoint from logconfig

        except Exception:
            # Fallback to legacy defaults if everything fails
            logging_config = {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": default_formatters.copy(),
                "handlers": default_handlers.copy(),
                "loggers": default_loggers.copy(),
            }

    # Apply the logging configuration
    logging.config.dictConfig(logging_config)


# Default formatters
default_formatters: Dict = {
    "text": {
        "()": structlog.stdlib.ProcessorFormatter,
        "processor": structlog.dev.ConsoleRenderer(),
        "keep_exc_info": True,
        "keep_stack_info": True,
    },
    "json": {
        "()": structlog.stdlib.ProcessorFormatter,
        "processor": structlog.processors.JSONRenderer(),
    },
}

# Default handlers
default_handlers: Dict = {
    "console_text": {
        "level": "INFO",
        "class": "logging.StreamHandler",
        "formatter": "text",
    },
    "console_json": {
        "level": "INFO",
        "class": "logging.StreamHandler",
        "formatter": "json",
    },
}

# Default loggers
default_loggers: Dict = {
    "jobmon.server.web": {
        "handlers": ["console_json"],
        "level": "INFO",
        "propagate": False,
    },
    "sqlalchemy": {
        "handlers": ["console_json"],
        "level": "WARN",
        "propagate": False,
    },
}

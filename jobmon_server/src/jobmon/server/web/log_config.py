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


def configure_logging() -> None:
    """Setup logging with default handlers and OpenTelemetry if enabled."""
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": default_formatters.copy(),
        "handlers": default_handlers.copy(),
    }
    # Load logging configuration from file if provided
    config = JobmonConfig()
    try:
        log_config_file = config.get("logging", "log_config_file")
        file_config = load_logging_config_from_file(log_config_file)
        merge_logging_configs(logging_config, file_config)
    except ConfigError:
        # Use default configurations
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": default_formatters.copy(),
            "handlers": default_handlers.copy(),
            "loggers": default_loggers.copy(),
        }

    # Apply the logging configuration
    logging.config.dictConfig(logging_config)

    # Configure structlog after logging is configured
    configure_structlog()

    logging.getLogger(__name__).info("Logging has been configured.")


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
    "otel": {
        "()": "jobmon.core.otlp.OpenTelemetryLogFormatter",
        "format": "%(asctime)s [%(levelname)s] [trace_id=%(trace_id)s,"
        " span_id=%(span_id)s, parent_span_id=%(parent_span_id)s]"
        " - %(message)s",
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
    "otel_text": {
        "level": "INFO",
        "class": "opentelemetry.sdk._logs.LoggingHandler",
        "formatter": "otel",
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

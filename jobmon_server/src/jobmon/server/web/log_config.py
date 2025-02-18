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
    """Setup logging with default handlers and OpenTelemetry if enabled."""
    logging_config = dict_config
    if logging_config is None and file_config:
        logging_config = load_logging_config_from_file(file_config)

    if logging_config is None:
        config = JobmonConfig()
        try:
            log_config_file = config.get("logging", "log_config_file")
            logging_config = load_logging_config_from_file(log_config_file)
        except ConfigError:
            logging_config = {
                "version": 1,
                "disable_existing_loggers": True,
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

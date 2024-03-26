"""Configure Logging for structlogs, syslog, etc."""

import logging.config
from typing import Dict, List, Optional

import structlog

# from jobmon.server import __version__


def configure_structlog(extra_processors: Optional[List] = None) -> None:
    """Configure logging format, handlers, etc."""
    if extra_processors is None:
        extra_processors = []
    structlog.configure(
        processors=[
            # bring in threadlocal context
            structlog.contextvars.merge_contextvars,
            # This performs the initial filtering, so we don't
            # evaluate e.g. DEBUG when unnecessary
            structlog.stdlib.filter_by_level,
            # Adds logger=module_name (e.g __main__)
            structlog.stdlib.add_logger_name,
            # Adds level=info, debug, etc.
            structlog.stdlib.add_log_level,
            # add jobmon version to json object
            *extra_processors,
            # Performs the % string interpolation as expected
            structlog.stdlib.PositionalArgumentsFormatter(),
            # add datetime to our logs
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            # Include the stack when stack_info=True
            structlog.processors.StackInfoRenderer(),
            # Include the exception when exc_info=True
            # e.g log.exception() or log.warning(exc_info=True)'s behavior
            structlog.processors.format_exc_info,
            # Creates the necessary args, kwargs for log()
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        # Our "event_dict" is explicitly a dict
        context_class=dict,
        # Provides the logging.Logger for the underlaying log call
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Provides predefined methods - log.debug(), log.info(), etc.
        wrapper_class=structlog.stdlib.BoundLogger,
        # Caching of our logger
        cache_logger_on_first_use=True,
    )


default_formatters: Dict = {
    # copied formatter from here: https://github.com/hynek/structlog/issues/235
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
default_loggers: Dict = {
    # only configure loggers of given name
    "jobmon.server.web": {
        "handlers": ["console_json"],
        "level": "INFO",
    },
    "werkzeug": {
        "handlers": ["console_json"],
        "level": "WARN",
    },
    "sqlalchemy": {
        "handlers": ["console_json"],
        "level": "WARN",
    },
    # enable SQL debug
    # 'sqlalchemy.engine': {
    #     'level': 'INFO',
    # }
}


def configure_logging(
    loggers_dict: Optional[Dict] = None,
    handlers_dict: Optional[Dict] = None,
    formatters_dict: Optional[Dict] = None,
) -> None:
    """Setup logging with default handlers."""
    if formatters_dict is None:
        formatters_dict = default_formatters
    if handlers_dict is None:
        handlers_dict = default_handlers
    if loggers_dict is None:
        loggers_dict = default_loggers
    dict_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters_dict,
        "handlers": handlers_dict,
        "loggers": loggers_dict,
    }
    logging.config.dictConfig(dict_config)

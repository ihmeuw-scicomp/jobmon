"""Configure Logging for structlogs, syslog, etc."""
import logging.config
from typing import Any, Dict, MutableMapping, Optional

from elasticapm.handlers.structlog import structlog_processor as elasticapm_processor
import structlog

from jobmon.server import __version__


def _processor_add_version(
    logger: logging.Logger, log_method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict["jobmon_version"] = __version__
    return event_dict


def configure_structlog() -> None:
    """Configure logging format, handlers, etc."""
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
            _processor_add_version,
            # Performs the % string interpolation as expected
            structlog.stdlib.PositionalArgumentsFormatter(),
            # add datetime to our logs
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            # Include the stack when stack_info=True
            structlog.processors.StackInfoRenderer(),
            # Adds transaction.id, trace.id, span.id for APM visualization
            elasticapm_processor,
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
    }
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

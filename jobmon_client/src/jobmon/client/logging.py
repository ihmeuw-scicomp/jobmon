"""Configuration setting for client-side only."""
from __future__ import annotations

import logging
import logging.config
import sys
from typing import Dict, Optional, Type


_DEFAULT_LOG_FORMAT = (
    "%(asctime)s [%(name)-12s] %(module)s %(levelname)-8s: %(message)s"
)

default_config: Dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": _DEFAULT_LOG_FORMAT, "datefmt": "%Y-%m-%d %H:%M:%S"}
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": sys.stdout,
        }
    },
}


class JobmonLoggerConfig:
    """A class to automatically format and attach handlers to client logging modules."""

    dict_config = default_config

    @classmethod
    def set_default_config(
        cls: Type[JobmonLoggerConfig], dict_config: Optional[Dict] = None
    ) -> None:
        """Set the default logging configuration for this factory.

        Args:
            dict_config: A logging dict config to utilize when adding new loggers. each logger
                added via add_logger method expects to find a handler called "default"
        """
        if dict_config is None:
            dict_config = default_config

        if not isinstance(dict_config, dict):
            raise ValueError(
                f"dict_config param must be a dict or None. Got {type(dict_config)}"
            )
        else:
            cls.dict_config = dict_config
            logging.config.dictConfig(cls.dict_config)

    @classmethod
    def attach_default_handler(
        cls: Type[JobmonLoggerConfig], logger_name: str, log_level: int = logging.INFO
    ) -> None:
        """A method to setup the default logging config for a specific logger_name.

        Args:
            logger_name: The logger to setup
            log_level: the log severity of the handler.

        Returns: None
        """
        logger_config = {
            "loggers": {
                logger_name: {
                    "handlers": ["default"],
                    "level": log_level,
                    "propagate": False,
                },
            }
        }
        cls.dict_config.update(logger_config)
        logging.config.dictConfig(cls.dict_config)

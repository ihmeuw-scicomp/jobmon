# from jobmon.server.web.start import create_app  # noqa F401
from typing import Optional

from fastapi import FastAPI

from sqlalchemy import orm

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web import session_factory
from jobmon.server.web.app_factory import AppFactory  # noqa F401
from jobmon.server.web.log_config import configure_structlog  # noqa F401
from jobmon.server.web.log_config import configure_logging  # noqa F401

# scoped session associated with the current thread
SessionLocal = orm.scoped_session(session_factory)


def get_app(config: Optional[JobmonConfig] = None) -> FastAPI:
    """Get a flask app based on the config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    if config is None:
        app_factory = AppFactory.from_defaults()
    else:
        app_factory = AppFactory(
            config.get("db", "sqlalchemy_database_uri"),
            config.get_boolean("otlp", "web_enabled"),
        )
    app = app_factory.get_app()
    return app

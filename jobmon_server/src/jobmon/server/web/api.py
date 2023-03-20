# from jobmon.server.web.start import create_app  # noqa F401
from typing import Optional

from flask import Flask

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.app_factory import AppFactory  # noqa F401
from jobmon.server.web.log_config import configure_logging  # noqa F401


def get_app(config: Optional[JobmonConfig] = None) -> Flask:
    """Get a flask app based on the config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    app_factory = AppFactory(config)
    app = app_factory.get_app()
    return app

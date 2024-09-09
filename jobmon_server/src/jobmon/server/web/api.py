from importlib import import_module
from importlib.resources import files  # type: ignore
from typing import Any, Optional, Type

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import structlog


from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.log_config import configure_structlog  # noqa F401
from jobmon.server.web.log_config import configure_logging  # noqa F401
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.server_side_exception import ServerError

# a singleton to holp jobmon config
_jobmon_config = None

logger = structlog.get_logger()

url_prefix = "/api"

def get_app() -> FastAPI:
    """Get a flask app based on the config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    # create app from env or config if testing

    app = FastAPI(
        title="jobmon",
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
    )

    # Add CORS middleware to the FastAPI app
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Adjust the origins as needed
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["Content-Type"],
    )

    add_hooks_and_handlers(app)

    for version in ["v3"]:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        # Get the router dynamically from the module (assuming it's an APIRouter)
        api_router = getattr(mod, f"api_{version}_router")

        # Include the router with a version-specific prefix
        logger.info(f"Adding router for version {version}")
        app.include_router(api_router, prefix=f"{url_prefix}/{version}")

        # include fsm, cli, and reapper
        for r in ["fsm_router"]:
            logger.info(f"Adding router for {r}")
            mod = import_module(f"jobmon.server.web.routes.{version}.fsm")
            router = getattr(mod, r)
            app.include_router(router, prefix=f"{url_prefix}/{version}")

    return app


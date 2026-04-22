import logging
import os
from contextlib import asynccontextmanager
from importlib import import_module
from typing import AsyncIterator, List, Optional

# Additional imports for middlewares and dependencies
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.db import db_lifespan
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.middleware.db_reset_retry import DBResetRetryMiddleware
from jobmon.server.web.middleware.security_headers import SecurityHeadersMiddleware
from jobmon.server.web.routes.utils import (
    get_user,
    get_user_or_anonymous,
    is_auth_enabled,
)

log = logging.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage combined lifespan for all application resources.

    Manages:
    - Database engine lifecycle (creation on startup, disposal on shutdown)
    - OTLP graceful shutdown
    """
    # Use the database lifespan as the primary context manager
    async with db_lifespan(app):
        yield

    # OTLP shutdown is handled by the shutdown event registered in get_app
    # No additional cleanup needed here


def get_app(versions: Optional[List[str]] = None) -> FastAPI:
    """Get a FastAPI app based on the config. If no config is provided, defaults are used.

    Args:
        versions: The versions of the API to include.
    """
    config = JobmonConfig()

    # Configure logging after uvicorn workers are forked to prevent duplicate emissions
    from jobmon.server.web.logging import configure_server_logging

    configure_server_logging()

    # Initialize the FastAPI app with lifespan for database management
    app_title = "jobmon"
    openapi_url = "/api/openapi.json"

    app = FastAPI(
        title=app_title,
        openapi_url=openapi_url,
        docs_url=None,
        lifespan=app_lifespan,
    )
    app = add_hooks_and_handlers(app)

    from jobmon.core.otlp.manager import register_otlp_shutdown_event

    register_otlp_shutdown_event(app)

    # Configure remaining OTLP components
    try:
        telemetry_section = config.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})
        USE_OTEL = tracing_config.get("server_enabled", False)
    except Exception:
        USE_OTEL = False
    if USE_OTEL:
        # Import OTel modules here to avoid unnecessary imports when OTel is disabled
        from jobmon.server.web.otlp import get_server_otlp_manager

        # Initialize server OTLP manager
        server_otlp = get_server_otlp_manager()
        server_otlp.initialize()  # Actually initialize the manager!

        # Instrument SQLAlchemy BEFORE any engine creation
        server_otlp.instrument_sqlalchemy()
        server_otlp.instrument_requests()

        # Instrument FastAPI for HTTP request tracing
        # OTEL_LOGS_EXPORTER=none prevents auto log export (we use manual LoggerProvider)
        server_otlp.instrument_app(app)

    # Logging is already configured at module import time to avoid duplicate
    # configuration in multi-worker environments

    # Mount static files
    docs_static_uri = "/static"  # Adjust as necessary
    docs_static_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "static"
    )
    app.mount(
        docs_static_uri, StaticFiles(directory=docs_static_path), name="docs_static"
    )

    # Check if auth is enabled first (needed for CORS configuration)
    auth_enabled = is_auth_enabled()

    # Add middlewares
    # Configure CORS origins based on environment and auth setting
    allowed_origins = []

    # Get CORS origins from config or environment
    try:
        cors_origins = config.get("cors", "allowed_origins")
        allowed_origins = [origin.strip() for origin in cors_origins.split(",")]
    except Exception:
        # Default CORS origins for development
        allowed_origins = [
            "http://localhost:3000",  # Default Vite dev server
            "http://localhost:3001",  # Alternative frontend port
            "http://127.0.0.1:3000",  # IPv4 localhost
            "http://127.0.0.1:3001",  # IPv4 localhost alternative
        ]

    # Configure CORS middleware based on auth status
    if auth_enabled:
        # When auth is enabled, we need credentials, so specify exact origins
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
        )
    else:
        # When auth is disabled, we can use wildcard since no credentials are needed
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)

    # Only add session middleware when authentication is enabled
    if auth_enabled:
        app.add_middleware(
            SessionMiddleware, secret_key=config.get("session", "secret_key")
        )

    app.add_middleware(SecurityHeadersMiddleware, csp=True)

    # Retry middleware wraps the entire request pipeline (handler +
    # dependency finalizers) so transient DB connection-resets on Azure
    # Private Link are retried once instead of surfacing as 5xx. The
    # generic exception handler in ``hooks_and_handlers`` re-raises
    # connection-reset errors so they escape FastAPI's
    # ``ExceptionMiddleware`` and land here; the ``budget_seconds`` cap
    # prevents a slow query + retry from blowing past the client's
    # read_timeout (default 20s).
    retry_cfg: dict = {}
    try:
        db_cfg = config.get_section_coerced("db")
        retry_cfg = db_cfg.get("retry") or {}
        if not isinstance(retry_cfg, dict):
            retry_cfg = {}
    except Exception:
        retry_cfg = {}
    app.add_middleware(
        DBResetRetryMiddleware,
        max_attempts=int(
            retry_cfg.get("max_attempts", DBResetRetryMiddleware.DEFAULT_MAX_ATTEMPTS)
        ),
        backoff_seconds=float(
            retry_cfg.get(
                "backoff_seconds", DBResetRetryMiddleware.DEFAULT_BACKOFF_SECONDS
            )
        ),
        budget_seconds=float(
            retry_cfg.get(
                "budget_seconds", DBResetRetryMiddleware.DEFAULT_BUDGET_SECONDS
            )
        ),
    )

    # Include routers with conditional authentication
    versions = versions or (["auth", "v3"] if auth_enabled else ["v3"])
    url_prefix = "/api"  # Adjust as necessary
    for version in versions:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        # Get the router dynamically from the module (assuming it's an APIRouter)
        api_router = getattr(mod, f"api_{version}_router")
        # Include the router with a version-specific prefix
        dependencies = None
        if version == "v3":
            if auth_enabled:
                dependencies = [Depends(get_user)]
            else:
                dependencies = [Depends(get_user_or_anonymous)]

            # Include health router separately without authentication
            health_router = getattr(mod, f"api_{version}_health_router")
            app.include_router(health_router, prefix=url_prefix)

        app.include_router(api_router, prefix=url_prefix, dependencies=dependencies)

    # Custom documentation endpoints
    @app.get("/api/docs", include_in_schema=False)
    async def custom_swagger_ui_html() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=app_title + " API",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url=f"{docs_static_uri}/swagger-ui-bundle.js",
            swagger_css_url=f"{docs_static_uri}/swagger-ui.css",
        )

    @app.get("/api/redoc", include_in_schema=False)
    async def redoc_html() -> HTMLResponse:
        return get_redoc_html(
            openapi_url=openapi_url,
            title=app_title + " ReDoc",
            redoc_js_url=f"{docs_static_uri}/redoc.standalone.js",
        )

    return app

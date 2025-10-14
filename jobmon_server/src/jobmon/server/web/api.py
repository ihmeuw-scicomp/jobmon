import os
from importlib import import_module
from typing import List, Optional

# Additional imports for middlewares and dependencies
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.middleware.security_headers import SecurityHeadersMiddleware
from jobmon.server.web.routes.utils import (
    get_user,
    get_user_or_anonymous,
    is_auth_enabled,
)


def get_app(versions: Optional[List[str]] = None) -> FastAPI:
    """Get a FastAPI app based on the config. If no config is provided, defaults are used.

    Args:
        versions: The versions of the API to include.
        log_config_file: Path to the logging configuration file.
    """
    config = JobmonConfig()

    # Configure logging after uvicorn workers are forked to prevent duplicate emissions
    from jobmon.server.web.logging import configure_server_logging
    configure_server_logging()

    # Initialize the FastAPI app
    app_title = "jobmon"
    openapi_url = "/api/openapi.json"

    app = FastAPI(
        title=app_title,
        openapi_url=openapi_url,
        docs_url=None,
    )
    app = add_hooks_and_handlers(app)

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
        )

    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)

    # Only add session middleware when authentication is enabled
    if auth_enabled:
        app.add_middleware(
            SessionMiddleware, secret_key=config.get("session", "secret_key")
        )

    app.add_middleware(SecurityHeadersMiddleware, csp=True)

    # Include routers with conditional authentication
    versions = versions or (["auth", "v3", "v2"] if auth_enabled else ["v3", "v2"])
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

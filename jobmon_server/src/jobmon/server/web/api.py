from importlib import import_module
import os
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
from jobmon.server.web.log_config import configure_logging, configure_structlog
from jobmon.server.web.middleware.security_headers import SecurityHeadersMiddleware
from jobmon.server.web.routes.utils import get_user


def get_app(versions: Optional[List[str]] = None) -> FastAPI:
    """Get a FastAPI app based on the config. If no config is provided, defaults are used.

    Args:
        versions: The versions of the API to include.
        log_config_file: Path to the logging configuration file.
    """
    config = JobmonConfig()

    # Configure logging
    configure_logging()

    # Initialize the FastAPI app
    app_title = "jobmon"
    openapi_url = "/api/openapi.json"

    app = FastAPI(
        title=app_title,
        openapi_url=openapi_url,
        docs_url=None,
    )
    app = add_hooks_and_handlers(app)

    USE_OTEL = config.get_boolean("otlp", "web_enabled")
    # OpenTelemetry instrumentation
    if USE_OTEL:
        # Import OTel modules here to avoid unnecessary imports when OTel is disabled
        from jobmon.core.otlp import OtlpAPI, add_span_details_processor

        otlp_api = OtlpAPI()
        otlp_api.instrument_sqlalchemy()
        otlp_api.instrument_requests()
        otlp_api.instrument_app(app)

        configure_structlog([add_span_details_processor])
    else:
        configure_structlog()

    # Mount static files
    docs_static_uri = "/static"  # Adjust as necessary
    docs_static_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "static"
    )
    app.mount(
        docs_static_uri, StaticFiles(directory=docs_static_path), name="docs_static"
    )

    # Add middlewares
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Adjust the origins as needed
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["Content-Type"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
    app.add_middleware(
        SessionMiddleware, secret_key=config.get("session", "secret_key")
    )
    app.add_middleware(SecurityHeadersMiddleware, csp=True)

    # Include routers
    versions = versions or ["auth", "v3", "v2", "v1"]
    url_prefix = "/api"  # Adjust as necessary
    for version in versions:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        # Get the router dynamically from the module (assuming it's an APIRouter)
        api_router = getattr(mod, f"api_{version}_router")
        # Include the router with a version-specific prefix
        dependencies = None
        if version == "v3":
            dependencies = [Depends(get_user)]
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

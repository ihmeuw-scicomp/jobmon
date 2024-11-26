from importlib import import_module
import os
from typing import Any, Optional

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
)
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse
import structlog

from jobmon.core.otlp import OtlpAPI
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.log_config import configure_structlog  # noqa F401
from jobmon.server.web.middleware.security_headers import SecurityHeadersMiddleware
from jobmon.server.web.routes.utils import get_user
from jobmon.server.web.server_side_exception import ServerError

url_prefix = "/api"

docs_static_uri = f"{url_prefix}/docs_static"
docs_uri = f"{url_prefix}/docs"

_CONFIG = get_jobmon_config()


def _init_logging(otlp_api: Optional[OtlpAPI] = None) -> bool:
    """Initialize logging for the app."""
    try:
        extra_processors = []
        if otlp_api:

            def add_open_telemetry_spans(_: Any, __: Any, event_dict: dict) -> dict:
                """Add OpenTelemetry spans to the log record."""
                if otlp_api is not None:
                    span, trace, parent_span = otlp_api.get_span_details()  # type: ignore
                else:
                    raise ServerError("otlp_api is None.")

                event_dict["span"] = {
                    "span_id": span or None,
                    "trace_id": trace or None,
                    "parent_span_id": parent_span or None,
                }
                return event_dict

            extra_processors.append(add_open_telemetry_spans)
        configure_structlog(extra_processors)
        return True
    except Exception as e:
        structlog.get_logger().error(f"Failed to initialize logging: {e}")
        return False


def get_app(
    versions: Optional[list[str]] = None,
    use_otlp: bool = False,
    structlog_configured: bool = False,
    otlp_api: Optional[OtlpAPI] = None,
) -> FastAPI:
    """Get a FastAPI app based on the config. If no config is provided, defaults are used.

    Args:
        use_otlp: Whether to use OpenTelemetry.
        structlog_configured: Whether structlog is already configured.
        otlp_api: The OpenTelemetry API to use.
    """
    if use_otlp and otlp_api is None:
        otlp_api = OtlpAPI()
        otlp_api.instrument_sqlalchemy()

    if not structlog_configured:
        structlog_configured = _init_logging(otlp_api)
    app_title = "jobmon"
    openapi_url = "/api/openapi.json"

    app = FastAPI(
        title=app_title,
        openapi_url=openapi_url,
        docs_url=None,
    )
    docs_static_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "static"
    )
    app.mount(
        docs_static_uri, StaticFiles(directory=docs_static_path), name="docs_static"
    )
    # Add CORS middleware to the FastAPI app
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Adjust the origins as needed
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["Content-Type"],
    )
    app.add_middleware(
        SessionMiddleware, secret_key=_CONFIG.get("session", "secret_key")
    )
    app.add_middleware(SecurityHeadersMiddleware, csp=True)
    add_hooks_and_handlers(app)
    versions = versions or ["auth", "v3", "v2", "v1"]
    for version in versions:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        # Get the router dynamically from the module (assuming it's an APIRouter)
        api_router = getattr(mod, f"api_{version}_router")
        # Include the router with a version-specific prefix
        dependencies = None
        if version == "v3":
            dependencies = [Depends(get_user)]
        app.include_router(
            api_router, prefix=f"{url_prefix}", dependencies=dependencies
        )

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

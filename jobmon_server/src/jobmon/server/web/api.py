from importlib import import_module
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import structlog

from jobmon.core.otlp import OtlpAPI
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.log_config import configure_structlog  # noqa F401
from jobmon.server.web.log_config import configure_logging  # noqa F401
from jobmon.server.web.server_side_exception import ServerError

url_prefix = "/api"


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
    for version in ["v3", "v2", "v1"]:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        # Get the router dynamically from the module (assuming it's an APIRouter)
        api_router = getattr(mod, f"api_{version}_router")
        # Include the router with a version-specific prefix
        app.include_router(api_router, prefix=f"{url_prefix}")

    return app

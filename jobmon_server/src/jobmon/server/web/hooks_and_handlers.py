from typing import Any, cast, Dict, Optional

from flask import Flask, jsonify, request
import structlog
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from jobmon.server.web.server_side_exception import InvalidUsage, ServerError

logger = structlog.get_logger(__name__)


def _handle_error(error: Exception, status_code: Optional[int] = None) -> Any:
    """Handle all exceptions in a uniform manner."""
    # Extract status code from the error
    status_code = status_code or getattr(error, "status_code", 500)

    # Check for deadlock scenario
    if "deadlock found" in str(error).lower():
        status_code = 423

    response_data = {
        "type": str(type(error)),
        "exception_message": str(error),
        "status_code": str(status_code),
    }
    logger.exception("server encountered:", status_code=status_code, route=request.path)

    response = jsonify(error=response_data)
    response.content_type = "application/json"
    response.status_code = status_code
    return response


def add_hooks_and_handlers(app: Flask) -> Flask:
    """Add logging hooks and exception handlers."""

    @app.errorhandler(Exception)
    def handle_generic_exception(error: Any) -> Any:
        return _handle_error(error)

    @app.errorhandler(InvalidUsage)
    @app.errorhandler(ServerError)
    def handle_custom_errors(error: Any) -> Any:
        return _handle_error(error, error.status_code)

    @app.errorhandler(404)
    def page_not_found(e: Any) -> Any:
        logger.warning("Route not found:", route=request.url)
        return f"This route does not exist: {request.url}", 404

    @app.before_request
    def add_requester_context() -> None:
        """Add structured logging context before each request."""
        structlog.contextvars.clear_contextvars()

        try:
            data = cast(Dict, request.get_json())
        except (BadRequest, UnsupportedMediaType):
            data = {}

        context_data = (
            data.pop("server_structlog_context", {})
            if request.method in ["POST", "PUT"]
            else data
        )
        if context_data:
            structlog.contextvars.bind_contextvars(path=request.path, **context_data)

    return app

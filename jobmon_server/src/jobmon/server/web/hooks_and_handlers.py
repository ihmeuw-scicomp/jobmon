from typing import Any, cast, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import json
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_404_NOT_FOUND
import structlog
from werkzeug.exceptions import BadRequest, UnsupportedMediaType

from jobmon.server.web.db_admin import SessionLocal
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError

logger = structlog.get_logger(__name__)

# Dependency for removing session after each request
async def teardown_session():
    try:
        yield
    finally:
        SessionLocal.remove()

def _handle_error(request: Request, error: Exception, status_code: Optional[int] = None) -> Any:
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
    logger.exception("server encountered:", status_code=status_code,
                     route=request.url.path)
    rd = {"error": response_data}
    response = JSONResponse(content=rd,
                            media_type="application/custom+json",
                            status_code=status_code)
    return response


def add_hooks_and_handlers(app: FastAPI) -> FastAPI:
    """Add logging hooks and exception handlers."""

    @app.exception_handler(Exception)
    def handle_generic_exception(request: Request, error: Any) -> Any:
        if isinstance(error, StarletteHTTPException):
            if error.status_code == HTTP_404_NOT_FOUND:
                logger.warning("Route not found:", route=request.url)
                return JSONResponse(content={"error": f"Route {request.url} not found"},
                                    status_code=HTTP_404_NOT_FOUND)
            return _handle_error(request, error, error.status_code)
        if isinstance(error, InvalidUsage) or isinstance(error, ServerError):
            return _handle_error(request, error, error.status_code)
        return _handle_error(request, error)

    @app.middleware("http")
    async def add_requester_context(request, call_next) -> None:
        """Add structured logging context before each request."""
        structlog.contextvars.clear_contextvars()
        data = {}
        try:
            # Only parse JSON if the content type is application/json
            if request.headers.get("content-type") == "application/json":
                body = await request.body()  # Get the raw body
                if body:  # Check if the body is not empty
                    data = cast(Dict[str, Any], await request.json())
        except (BadRequest, UnsupportedMediaType, json.JSONDecodeError):
            data = {}

        context_data = (
            data.pop("server_structlog_context", {})
            if request.method in ["POST", "PUT"]
            else data
        )
        if context_data:
            structlog.contextvars.bind_contextvars(path=request.path, **context_data)
        response = await call_next(request)
        return response

    # Include the teardown function globally, using FastAPI dependencies (for session cleanup)
    @app.middleware("http")
    async def session_middleware(request, call_next):
        response = await call_next(request)
        teardown_session()  # Call the session teardown after Request
        return response

    return app

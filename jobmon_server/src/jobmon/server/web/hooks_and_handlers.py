import json
from typing import Any, AsyncGenerator, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_404_NOT_FOUND
import structlog

from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError

logger = structlog.get_logger(__name__)


# Dependency for removing session after each request
async def teardown_session() -> AsyncGenerator:
    """Remove the session after each request."""
    try:
        yield
    finally:
        get_session_local().remove()  # type: ignore


def _handle_error(
    request: Request, error: Exception, status_code: Optional[int] = None
) -> Any:
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
    logger.exception(
        "server encountered:", status_code=status_code, route=request.url.path
    )
    rd = {"error": response_data}
    response = JSONResponse(
        content=rd,  # type: ignore
        media_type="application/custom+json",  # type: ignore
        status_code=status_code,  # type: ignore
    )
    return response


def add_hooks_and_handlers(app: FastAPI) -> FastAPI:
    """Add logging hooks and exception handlers."""

    @app.exception_handler(Exception)
    def handle_generic_exception(request: Request, error: Any) -> Any:
        if isinstance(error, StarletteHTTPException):
            if error.status_code == HTTP_404_NOT_FOUND:
                logger.warning("Route not found:", route=request.url)
                return JSONResponse(
                    content={"error": f"Route {request.url} not found"},
                    status_code=HTTP_404_NOT_FOUND,
                )
            return _handle_error(request, error, error.status_code)
        if isinstance(error, InvalidUsage) or isinstance(error, ServerError):
            return _handle_error(request, error, error.status_code)
        return _handle_error(request, error)

    @app.middleware("http")
    async def add_requester_context(request: Request, call_next: Callable) -> None:
        """Add structured logging context.

        It will add it before each request from headers, body, or query params.
        """
        structlog.contextvars.clear_contextvars()

        context_data = None

        # Step 1: Check headers for X-Server-Structlog-Context (newer clients)
        context_str = request.headers.get("X-Server-Structlog-Context")
        if context_str:
            try:
                context_data = json.loads(context_str)
            except json.JSONDecodeError:
                structlog.contextvars.bind_contextvars(
                    path=request.url.path,
                    error="Invalid JSON in X-Server-Structlog-Context header",
                )

        # Step 2: If not found in headers, check request body or query params (older clients)
        if context_data is None:
            if request.method in ["POST", "PUT"]:
                try:
                    # Read the request body
                    body = await request.body()
                    if body:
                        data = json.loads(body.decode("utf-8"))
                        if "server_structlog_context" in data:
                            context_data = data.pop("server_structlog_context")
                            # Reset the request body without server_structlog_context
                            new_body = json.dumps(data).encode("utf-8")
                            request._body = new_body
                            request.scope["body"] = new_body
                except json.JSONDecodeError:
                    pass  # Ignore if body is not JSON
            elif request.method == "GET":
                context_str = request.query_params.get("server_structlog_context")
                if context_str:
                    try:
                        context_data = json.loads(context_str)
                    except json.JSONDecodeError:
                        structlog.contextvars.bind_contextvars(
                            path=request.url.path,
                            error="Invalid JSON in server_structlog_context query param",
                        )

        # Step 3: Bind the context if found
        if context_data:
            structlog.contextvars.bind_contextvars(
                path=request.url.path, **context_data
            )

        # Step 4: Proceed with the request
        response = await call_next(request)
        return response

    # Include the teardown function globally, using FastAPI dependencies (for session cleanup)
    @app.middleware("http")
    async def session_middleware(request: Request, call_next: Callable) -> Any:
        response = await call_next(request)
        teardown_session()  # Call the session teardown after Request
        return response

    return app

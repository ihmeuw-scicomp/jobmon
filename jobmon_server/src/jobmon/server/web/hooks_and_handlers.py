import json
import os
import traceback
import uuid
from typing import Any, Callable, Optional

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import ClientDisconnect
from starlette.responses import Response
from starlette.status import HTTP_404_NOT_FOUND

from jobmon.server.web.server_side_exception import InvalidUsage, ServerError

logger = structlog.get_logger(__name__)


def _record_exception_in_span(error: Exception) -> None:
    """Record exception details in the current OpenTelemetry span."""
    try:
        from opentelemetry import trace
        from opentelemetry.trace import Status, StatusCode

        span = trace.get_current_span()
        if span and span.is_recording():
            # Set span status to ERROR
            span.set_status(Status(StatusCode.ERROR, str(error)))

            # Record the exception as a span event
            span.record_exception(error)

            # Add exception details as span attributes
            span.set_attribute("error.type", type(error).__name__)
            span.set_attribute("error.message", str(error))

            # Add exception module and stack trace
            span.set_attribute("error.module", type(error).__module__)

            # Add HTTP status code if available
            if hasattr(error, "status_code"):
                span.set_attribute("http.status_code", error.status_code)

            # Add stack trace as a span event for more detailed debugging
            stack_trace = traceback.format_exc()
            if stack_trace and stack_trace != "NoneType: None\n":
                span.add_event("exception.stacktrace", {"stacktrace": stack_trace})

            # Debug logging to verify span recording is working
            logger.debug(
                "Recorded exception in span",
                span_id=format(span.get_span_context().span_id, "016x"),
                trace_id=format(span.get_span_context().trace_id, "032x"),
                error_type=type(error).__name__,
                error_message=str(error),
                span_recording=span.is_recording(),
            )
        else:
            logger.warning(
                "No active span to record exception",
                span_exists=span is not None,
                span_recording=span.is_recording() if span else False,
                error_type=type(error).__name__,
            )

    except Exception as e:
        # Don't let span recording errors break the main error handling
        logger.warning("Failed to record exception in span", record_error=str(e))


def _handle_error(
    request: Request, error: Exception, status_code: Optional[int] = None
) -> Any:
    """Handle all exceptions in a uniform manner."""
    # Record the exception in the OpenTelemetry span
    _record_exception_in_span(error)

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

    # Enhanced logging with exception details
    logger.exception(
        "server encountered:",
        status_code=status_code,
        route=request.url.path,
        error_type=type(error).__name__,
        error_message=str(error),
        full_exception=str(error),
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

    @app.exception_handler(ClientDisconnect)
    async def handle_client_disconnect(
        request: Request, exc: ClientDisconnect
    ) -> Response:
        logger.info("Client disconnected during request", route=request.url.path)
        return Response(status_code=499)

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

        # Generate unique request ID for correlation
        request_id = str(uuid.uuid4())[:8]
        
        # Bind request correlation context
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            path=request.url.path,
            method=request.method,
        )

        context_data = None

        # Step 1: Check headers for X-Server-Structlog-Context (newer clients)
        context_str = request.headers.get("X-Server-Structlog-Context")
        if context_str:
            try:
                context_data = json.loads(context_str)
            except json.JSONDecodeError:
                structlog.contextvars.bind_contextvars(
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
                            error="Invalid JSON in server_structlog_context query param",
                        )

        # Step 3: Bind the context if found
        if context_data:
            structlog.contextvars.bind_contextvars(**context_data)

        # Step 4: Proceed with the request
        response = await call_next(request)
        return response

    return app

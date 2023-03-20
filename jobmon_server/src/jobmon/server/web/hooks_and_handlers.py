"""Add handlers to deal with server-side exceptions and logging."""
from typing import Any, cast, Dict, Optional

from elasticapm.contrib.flask import ElasticAPM
from flask import Flask, jsonify, request
from MySQLdb import OperationalError
import structlog
from werkzeug.exceptions import BadRequest


from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


def add_hooks_and_handlers(app: Flask, apm: Optional[ElasticAPM] = None) -> Flask:
    """Add logging hooks and exception handlers."""

    @app.errorhandler(Exception)
    def handle_anything(error: Any) -> Any:
        if apm is not None:
            apm.capture_exception(exc_info=(type(error), error, error.__traceback__))
        try:
            status_code = error.status_code
        except AttributeError:
            status_code = 500

        if "deadlock found" in str(error).lower():
            status_code = 423

        response_dict = {
            "type": str(type(error)),
            "exception_message": str(error),
            "status_code": str(status_code),
        }
        logger.exception(
            "server encountered:", status_code=status_code, route=request.path
        )
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = status_code
        return response

    # handle 404 at the application level not the blueprint level
    @app.errorhandler(404)
    def page_not_found(e: Any) -> tuple:
        return f"This route does not exist: {request.url}", 404

    # error handling
    @app.errorhandler(InvalidUsage)
    def handle_4xx(error: InvalidUsage) -> Any:
        logger.exception(
            "server encountered:", status_code=error.status_code, route=request.path
        )
        if apm is not None:
            apm.capture_exception(exc_info=(type(error), error, error.__traceback__))
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    # error handling
    @app.errorhandler(ServerError)
    def handle_5xx(error: ServerError) -> Any:
        logger.exception(
            "server encountered:", status_code=error.status_code, route=request.path
        )
        if apm is not None:
            apm.capture_exception(exc_info=(type(error), error, error.__traceback__))
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    # error handling
    @app.errorhandler(OperationalError)
    def handle_mysql_gone_away(error: OperationalError) -> Any:
        if "2013, 'Lost connection to MySQL server during query'" in str(error):
            engine = SessionLocal().get_bind()
            # A new connection pool is created immediately after the old one has been disposed
            engine.dispose()

    @app.before_request
    def add_requester_context() -> None:
        # get a clean threadlocal structlog context for this request
        structlog.contextvars.clear_contextvars()

        server_structlog_context = None

        try:
            data = cast(Dict, request.get_json())
        except BadRequest:
            # Some get requests come without any json data.
            # All requests issued by Jobmon's requester automatically come with an empty dict;
            # however, if we call raw get requests outside of Jobmon we should handle it
            data = {}
        if request.method == "GET":
            server_structlog_context = data
        if request.method in ["POST", "PUT"]:
            server_structlog_context = data.pop("server_structlog_context", {})
        if server_structlog_context:
            structlog.contextvars.bind_contextvars(
                path=request.path, **server_structlog_context
            )

    return app

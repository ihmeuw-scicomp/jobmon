from __future__ import annotations

from importlib import import_module
from typing import Any, List, Optional, Type

from flask import Flask
import sqlalchemy

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web import session_factory
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.server_side_exception import ServerError


class AppFactory:
    """Factory for creating Flask apps."""

    # Class-level attributes for OTLP and SQLAlchemy instrumentation
    otlp_api = None
    _structlog_configured = False

    def __init__(
        self, sqlalchemy_database_uri: str = "", use_otlp: bool = False
    ) -> None:
        """Initialize the AppFactory object with the SQLAlchemy database URI.

        Args:
            sqlalchemy_database_uri: The SQLAlchemy database URI.
            use_otlp: Whether to use OTLP instrumentation.
        """
        if use_otlp and AppFactory.otlp_api is None:
            self._init_otlp()

        if not AppFactory._structlog_configured:
            self._init_logging()

        # Create SQLAlchemy engine
        self.engine = sqlalchemy.create_engine(
            sqlalchemy_database_uri, pool_recycle=200, future=True
        )
        session_factory.configure(bind=self.engine)

    @classmethod
    def from_defaults(cls: Type[AppFactory]) -> AppFactory:
        """Create an AppFactory from the default configuration."""
        config = JobmonConfig()
        return cls(
            config.get("db", "sqlalchemy_database_uri"),
            config.get_boolean("otlp", "web_enabled"),
        )

    @classmethod
    def _init_otlp(cls: Type[AppFactory]) -> None:
        from jobmon.core.otlp import OtlpAPI

        cls.otlp_api = OtlpAPI()
        cls.otlp_api.instrument_sqlalchemy()

    @classmethod
    def _init_logging(cls: Type[AppFactory]) -> None:
        from jobmon.server.web.log_config import configure_structlog

        extra_processors = []
        if cls.otlp_api:

            def add_open_telemetry_spans(_: Any, __: Any, event_dict: dict) -> dict:
                """Add OpenTelemetry spans to the log record."""
                if cls.otlp_api is not None:
                    span, trace, parent_span = cls.otlp_api.get_span_details()
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
        cls._structlog_configured = True

    def get_app(
        self, blueprints: Optional[List[str]] = None, url_prefix: str = "/api"
    ) -> Flask:
        """Create and configure the Flask app.

        Args:
            blueprints: The blueprints to register with the app.
            url_prefix: The URL prefix for the app.
        """
        if blueprints is None:
            blueprints = ["fsm", "cli", "reaper"]
        app = Flask(__name__)
        app.config["CORS_HEADERS"] = "Content-Type"

        # Register the versions, reverse order
        for version in ["v2", "v1"]:
            mod = import_module(f"jobmon.server.web.routes.{version}")
            app.register_blueprint(
                getattr(mod, f"api_{version}_blueprint"),
                url_prefix=f"{url_prefix}/{version}",
            )

        if self.otlp_api:
            self.otlp_api.instrument_app(app)

        add_hooks_and_handlers(app)

        return app

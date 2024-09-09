from __future__ import annotations

from importlib import import_module
from typing import Any, Optional, Type

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import sessionmaker
import structlog

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.api import create_engine_from_config
from jobmon.server.web.hooks_and_handlers import add_hooks_and_handlers
from jobmon.server.web.server_side_exception import ServerError

logger = structlog.get_logger(__name__)

class AppFactory:
    """Factory for creating Flask apps."""

    # Class-level attributes for OTLP and SQLAlchemy instrumentation
    otlp_api = None
    _structlog_configured = False

    def __init__(
        self, config: Optional[JobmonConfig] = None, use_otlp: bool = False
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
        self.engine = create_engine_from_config(config)

    def get_session_local(self) -> sessionmaker:
        """Create a session from the SQLAlchemy engine."""
        return sessionmaker(autocommit=False, autoflush=False, bind=self.engine)


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
        self, url_prefix: str = "/api"
    ) -> FastAPI:
        """Create and configure the Flask app.

        Args:
            blueprints: The blueprints to register with the app.
            url_prefix: The URL prefix for the app.
        """
        app = FastAPI()

        # Add CORS middleware to the FastAPI app
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # Adjust the origins as needed
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["Content-Type"],
        )

        if self.otlp_api:
            self.otlp_api.instrument_app(app)

        add_hooks_and_handlers(app)

        return app

"""Server-specific OTLP manager that builds on core functionality."""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

# Check if OpenTelemetry is available
try:
    import opentelemetry.trace  # noqa: F401

    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False


class ServerOTLPManager:
    """Server-specific OTLP manager that builds on core functionality."""

    def __init__(self) -> None:
        """Initialize server OTLP manager."""
        self._core_manager: Optional[Any] = None
        self._initialized = False
        # Guard flags to prevent double instrumentation warnings
        self._fastapi_instrumented = False
        self._requests_instrumented = False
        self._sqlalchemy_instrumented = False

    def initialize(self) -> None:
        """Initialize the core OTLP manager for server use."""
        if self._initialized:
            return

        if not OTLP_AVAILABLE:
            logger = logging.getLogger(__name__)
            logger.info("OpenTelemetry not available - server OTLP disabled")
            return

        try:
            from jobmon.core.otlp import JobmonOTLPManager

            # Initialize core manager (provides OTLP providers for logconfig handlers)
            self._core_manager = JobmonOTLPManager.get_instance()
            self._core_manager.initialize()

            # Note: OTLP logging handlers are configured via dictConfig/logconfig
            # using JobmonOTLPLoggingHandler and JobmonOTLPStructlogHandler

            self._initialized = True

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to initialize server OTLP: {e}")
            self._initialized = False
            self._core_manager = None

    @property
    def tracer_provider(self) -> Optional[Any]:
        """Get the tracer provider from core manager."""
        if self._core_manager:
            return self._core_manager.tracer_provider
        return None

    @property
    def logger_provider(self) -> Optional[Any]:
        """Get the logger provider from core manager."""
        if self._core_manager:
            return self._core_manager.logger_provider
        return None

    def get_tracer(self, name: str) -> Optional[Any]:
        """Get a tracer for the given name."""
        if self._core_manager:
            return self._core_manager.get_tracer(name)
        return None

    def instrument_app(self, app: Any) -> None:
        """Instrument FastAPI application with OpenTelemetry.

        This is server-specific functionality that should not be in core.
        """
        if not OTLP_AVAILABLE or not self._initialized or self._fastapi_instrumented:
            return

        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor().instrument_app(app)
            self._fastapi_instrumented = True
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to instrument FastAPI app: {e}")

    @classmethod
    def instrument_requests(cls: Type[ServerOTLPManager]) -> None:
        """Instrument requests library - server-specific implementation."""
        if not OTLP_AVAILABLE:
            return

        try:
            from opentelemetry.instrumentation.requests import RequestsInstrumentor

            # Use a class-level guard on the singleton to avoid duplicate calls
            manager = get_server_otlp_manager()
            if not manager._requests_instrumented:
                RequestsInstrumentor().instrument()
                manager._requests_instrumented = True
        except ImportError:
            pass

    @classmethod
    def instrument_sqlalchemy(cls: Type[ServerOTLPManager]) -> None:
        """Instrument SQLAlchemy globally - server-specific implementation."""
        if not OTLP_AVAILABLE:
            return

        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            manager = get_server_otlp_manager()
            if not manager._sqlalchemy_instrumented:
                # Instrument globally once with desired options.
                SQLAlchemyInstrumentor().instrument(
                    enable_commenter=True, skip_dep_check=True
                )
                manager._sqlalchemy_instrumented = True
        except ImportError:
            pass

    @classmethod
    def instrument_engine(cls: Type[ServerOTLPManager], engine: Any) -> Any:
        """Instrument a specific SQLAlchemy engine with OpenTelemetry.

        This directly creates an EngineTracer for the engine, bypassing the
        SQLAlchemyInstrumentor class. This is necessary because:

        1. SQLAlchemyInstrumentor uses a class-level `is_instrumented_by_opentelemetry`
           flag that prevents `_instrument()` from running if global instrumentation
           was already performed.
        2. Our engine.py imports `create_engine` BEFORE `instrument_sqlalchemy()` patches
           it, so engines are created with the unpatched function.
        3. The EngineTracer attaches event listeners for query tracing that wouldn't
           otherwise be present.

        Returns:
            The EngineTracer instance (kept alive to prevent garbage collection
            of event listeners), or None if instrumentation failed.
        """
        if not OTLP_AVAILABLE:
            return None

        try:
            from opentelemetry import trace
            from opentelemetry.instrumentation.sqlalchemy import __version__
            from opentelemetry.instrumentation.sqlalchemy.engine import EngineTracer
            from opentelemetry.metrics import get_meter
            from opentelemetry.semconv.metrics import MetricInstruments

            # Get tracer from global provider (set up by JobmonOTLPManager.initialize())
            tracer = trace.get_tracer(
                "opentelemetry.instrumentation.sqlalchemy",
                __version__,
                schema_url="https://opentelemetry.io/schemas/1.11.0",
            )

            # Get meter for connection pool metrics
            meter = get_meter(
                "opentelemetry.instrumentation.sqlalchemy",
                __version__,
                schema_url="https://opentelemetry.io/schemas/1.11.0",
            )

            connections_usage = meter.create_up_down_counter(
                name=MetricInstruments.DB_CLIENT_CONNECTIONS_USAGE,
                unit="connections",
                description="Number of connections in state described by state attribute.",
            )

            # Create EngineTracer directly - this attaches event listeners for
            # before_cursor_execute, after_cursor_execute, handle_error, etc.
            engine_tracer = EngineTracer(
                tracer=tracer,
                engine=engine,
                connections_usage=connections_usage,
                enable_commenter=True,
                commenter_options={},
                enable_attribute_commenter=False,
            )

            logger = logging.getLogger(__name__)
            logger.debug(
                "Instrumented SQLAlchemy engine with OpenTelemetry EngineTracer"
            )

            return engine_tracer

        except ImportError:
            return None
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to instrument SQLAlchemy engine: {e}")
            return None


# Singleton instance for server use
_server_otlp_manager: Optional[ServerOTLPManager] = None


def get_server_otlp_manager() -> ServerOTLPManager:
    """Get or create the server OTLP manager singleton."""
    global _server_otlp_manager
    if _server_otlp_manager is None:
        _server_otlp_manager = ServerOTLPManager()
        _server_otlp_manager.initialize()
    return _server_otlp_manager


def initialize_server_otlp() -> ServerOTLPManager:
    """Initialize server-specific OTLP functionality.

    This should be called by the server during startup.

    Returns:
        The server OTLP manager instance
    """
    return get_server_otlp_manager()

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
    def instrument_engine(cls: Type[ServerOTLPManager], engine: Any) -> None:
        """Instrument a specific SQLAlchemy engine with OpenTelemetry.

        This ALWAYS instruments the provided engine, even if global instrumentation
        was already performed. This is necessary because Python's import semantics
        mean that modules which import `create_engine` before global instrumentation
        runs will have a reference to the original, unpatched function.

        The SQLAlchemyInstrumentor handles duplicate instrumentation gracefully -
        it checks if the engine is already instrumented before adding listeners.
        """
        if not OTLP_AVAILABLE:
            return

        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            # Always instrument the specific engine. SQLAlchemyInstrumentor internally
            # tracks which engines have been instrumented and won't double-instrument.
            # This is necessary because:
            # 1. engine.py imports create_engine BEFORE instrument_sqlalchemy() patches it
            # 2. So engines created in engine.py use the unpatched create_engine
            # 3. Per-engine instrumentation is the only way to instrument them
            SQLAlchemyInstrumentor().instrument(
                engine=engine, enable_commenter=True, skip_dep_check=True
            )

            logger = logging.getLogger(__name__)
            logger.debug("Instrumented SQLAlchemy engine with OpenTelemetry")
        except ImportError:
            pass
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to instrument SQLAlchemy engine: {e}")


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

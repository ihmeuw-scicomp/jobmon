"""Server-specific OTLP manager that builds on core functionality."""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

# Check if OpenTelemetry is available
try:
    OTLP_AVAILABLE = True
except ImportError:
    OTLP_AVAILABLE = False


class ServerOTLPManager:
    """Server-specific OTLP manager that builds on core functionality."""

    def __init__(self) -> None:
        """Initialize server OTLP manager."""
        self._core_manager: Optional[Any] = None
        self._initialized = False

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
            # using JobmonOTLPLoggingHandler and ServerOTLPStructlogHandler

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
        if not OTLP_AVAILABLE or not self._initialized:
            return

        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor().instrument_app(app)
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

            RequestsInstrumentor().instrument()
        except ImportError:
            pass

    @classmethod
    def instrument_sqlalchemy(cls: Type[ServerOTLPManager]) -> None:
        """Instrument SQLAlchemy globally - server-specific implementation."""
        if not OTLP_AVAILABLE:
            return

        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            SQLAlchemyInstrumentor().instrument()
        except ImportError:
            pass

    @classmethod
    def instrument_engine(cls: Type[ServerOTLPManager], engine: Any) -> None:
        """Instrument a specific SQLAlchemy engine - server-specific implementation."""
        if not OTLP_AVAILABLE:
            return

        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            SQLAlchemyInstrumentor().instrument(
                engine=engine, enable_commenter=True, skip_dep_check=True
            )
        except ImportError:
            pass


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

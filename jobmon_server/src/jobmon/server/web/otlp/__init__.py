"""Server-specific OpenTelemetry instrumentation for jobmon.

This package contains server-specific OTLP functionality that should not be
in the shared jobmon_core package. It handles:

- FastAPI application instrumentation
- SQLAlchemy engine instrumentation
- Server-specific configuration patterns
- Structured logging with structlog

Architecture:
- Delegates to JobmonOTLPManager from core for shared functionality
- Adds server-specific instrumentation methods
- Provides server-specific structured logging handlers
"""

from __future__ import annotations

# Core server OTLP exports
from .manager import (
    OTLP_AVAILABLE,
    ServerOTLPManager,
    get_server_otlp_manager,
    initialize_server_otlp,
)

__all__ = [
    "ServerOTLPManager",
    "get_server_otlp_manager",
    "initialize_server_otlp",
    "OTLP_AVAILABLE",
]

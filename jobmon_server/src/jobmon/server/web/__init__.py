"""Web API subpackage."""

# Configure structlog early to ensure consistent formatting across all server modules
# This happens on first import of jobmon.server.web package, before any get_logger() calls
# CRITICAL: We configure structlog here but NOT Python logging - Python logging
# is configured later by configure_logging_with_overrides() to avoid double formatting
from jobmon.core.config.structlog_config import configure_structlog

configure_structlog(component_name="server")

# Configure stdlib logging once at module import time to avoid duplicate configuration
# in multi-worker environments. This ensures logging is configured before any workers start.
from jobmon.server.web.logging import configure_server_logging

configure_server_logging()

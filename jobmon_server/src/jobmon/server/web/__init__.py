"""Web API subpackage."""

# Configure stdlib logging once at module import time to avoid duplicate configuration
# in multi-worker environments. This ensures logging is configured before any workers start.
from jobmon.server.web.logging import configure_server_logging

configure_server_logging()

# Configure structlog to integrate with stdlib loggers (not create duplicate handlers)
# This must come AFTER stdlib logging is configured
from jobmon.core.config.structlog_config import configure_structlog

configure_structlog(component_name="server")

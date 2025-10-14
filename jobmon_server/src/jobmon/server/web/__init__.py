"""Web API subpackage."""

# Configure logging once at module import time to avoid duplicate configuration
# in multi-worker environments. This ensures logging is configured before any workers start.
# This includes both stdlib logging and structlog configuration.
from jobmon.server.web.logging import configure_server_logging

configure_server_logging()

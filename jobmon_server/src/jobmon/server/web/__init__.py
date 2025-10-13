"""Web API subpackage."""

# Structlog configuration is handled by the logging configuration system
# to avoid double configuration that causes duplicate logs

# Configure stdlib logging once at module import time to avoid duplicate configuration
# in multi-worker environments. This ensures logging is configured before any workers start.
from jobmon.server.web.logging import configure_server_logging

configure_server_logging()

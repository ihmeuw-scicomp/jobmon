from importlib.metadata import version

__version__ = version("jobmon_client")

# Configure structlog early to ensure consistent formatting across all client modules
# This happens on first import of jobmon.client package, before any get_logger() calls
# CRITICAL: We configure structlog here but NOT Python logging - Python logging
# is configured later by configure_client_logging() to avoid double formatting
from jobmon.core.config.structlog_config import configure_structlog

configure_structlog(component_name="client")

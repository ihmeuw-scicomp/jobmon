"""Logging configuration for jobmon server applications.

This module provides the standard logging configuration used by
server startup and API initialization.

The module supports both legacy dict-based configs and new template-based
configurations. Server logs are automatically captured by OTLP when
enabled (handled by the server configuration overrides).
"""

from __future__ import annotations

import os
import sys
from typing import Dict

# Legacy default configuration - used as fallback only
_DEFAULT_LOG_FORMAT = (
    "%(asctime)s [%(name)-12s] %(module)s %(levelname)-8s: %(message)s"
)

default_config: Dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": _DEFAULT_LOG_FORMAT, "datefmt": "%Y-%m-%d %H:%M:%S"}
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": sys.stdout,
        }
    },
}


# Use a file-based lock to prevent duplicate configuration across multiple workers
import fcntl
import tempfile
import atexit


def configure_server_logging() -> None:
    """Configure server logging with template and user override support.

    This is the primary interface for configuring server logging. It supports:
    1. Default template-based configuration
    2. User file overrides via logging.server_logconfig_file
    3. User section overrides via logging.server.*
    4. Environment variable overrides

    Configuration precedence:
    1. Custom file (logging.server_logconfig_file)
    2. Section overrides (logging.server.formatters/handlers/loggers)
    3. Default template (logconfig_server.yaml)
    4. Basic fallback configuration

    Note: Server OTLP is handled separately by the server OTLP manager.
    """
    import os
    import tempfile
    
    # Use a file-based lock to prevent duplicate configuration across multiple workers
    lock_file_path = os.path.join(tempfile.gettempdir(), "jobmon_server_logging.lock")
    
    try:
        # Try to acquire an exclusive lock
        lock_file = open(lock_file_path, 'w')
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        # DEBUG: Track configuration calls
        print(f"[SERVER_LOGGING_DEBUG] configure_server_logging called, acquired lock")
        
        from jobmon.core.config.logconfig_utils import configure_logging_with_overrides

        # Get default template path
        current_dir = os.path.dirname(__file__)
        default_template_path = os.path.join(current_dir, "config/logconfig_server.yaml")

        print(f"[SERVER_LOGGING_DEBUG] Configuring logging with template: {default_template_path}")

        # Configure Python logging with override support
        # Note: structlog is already configured in __init__.py
        configure_logging_with_overrides(
            default_template_path=default_template_path,
            config_section="server",
            fallback_config=default_config,
        )

        print(f"[SERVER_LOGGING_DEBUG] Server logging configuration completed")
        
        # Keep the lock file open until process exit
        atexit.register(lambda: lock_file.close())
        
    except (OSError, IOError):
        # Another worker already has the lock, skip configuration
        print(f"[SERVER_LOGGING_DEBUG] Skipping duplicate configuration - another worker is configuring")
        return

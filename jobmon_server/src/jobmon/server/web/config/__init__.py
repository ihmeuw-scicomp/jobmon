"""Jobmon server web configuration package.

This package contains server-specific logging configurations that
use shared templates from jobmon.core.config.templates.

Server configs:
- logconfig_server.yaml: Server logging with structlog (OTLP disabled)
- logconfig_server_otlp.yaml: Server logging with OTLP enabled
- server_otlp_example.yaml: Example server OTLP configuration

These configurations are automatically selected by the server's
configure_logging() function based on the otlp.web_enabled setting.
"""

# Re-export functions from the parent config module for backward compatibility
from typing import Optional

from jobmon.core.configuration import JobmonConfig

# a singleton to hold jobmon config that enables testing
_jobmon_config = None


def get_jobmon_config(config: Optional[JobmonConfig] = None) -> JobmonConfig:
    """Get the jobmon config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    global _jobmon_config
    if config is None:
        # allow testing to override the config
        if _jobmon_config is None:
            # create from default
            _jobmon_config = JobmonConfig()
        else:
            # do nothing; use the existing config instance
            pass
    else:
        # leave an entry for testing
        _jobmon_config = config
    return _jobmon_config


__all__ = ["get_jobmon_config"]

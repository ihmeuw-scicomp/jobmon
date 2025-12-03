"""Jobmon server web configuration package.

This package contains server-specific configuration for the jobmon web server.

Logging Configuration:
Server logging is now configured programmatically via generate_component_logconfig()
in jobmon.core.config.logconfig_utils. Users can override defaults via:
1. File override: Set logging.server_logconfig_file in jobmonconfig.yaml
2. Section override: Set logging.server.* sections in jobmonconfig.yaml

The configure_server_logging() function in jobmon.server.web.logging handles
the logging initialization with support for user overrides.
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

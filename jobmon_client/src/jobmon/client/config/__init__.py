"""Jobmon client configuration package.

This package contains client-specific logging configurations for the high-level
jobmon client API (workflow, task, tool, etc.). These configurations use shared
templates from jobmon.core.config.templates.

Client configs:
- logconfig_client.yaml: Client logging configuration with OTLP handlers available
  (OTLP can be enabled/disabled by configuring which handlers are used in loggers)

The core package handles requester-specific OTLP configuration, while this
package handles the broader client application logging configuration.

All configurations now use shared formatters and handlers via template references
for consistency and maintainability across environments.
"""

# Make template loader accessible for client configs
try:
    __all__ = [
        "load_logconfig_with_templates",
        "load_yaml_with_templates",
    ]
except ImportError:
    # Core config may not be available in some contexts
    __all__ = []

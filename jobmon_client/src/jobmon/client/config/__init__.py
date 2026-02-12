"""Jobmon client configuration package.

This package contains client-specific configuration for the high-level
jobmon client API (workflow, task, tool, etc.).

Logging Configuration:
Client logging is now configured programmatically via generate_component_logconfig()
in jobmon.core.config.logconfig_utils. Users can override defaults via:
1. File override: Set logging.client_logconfig_file in jobmonconfig.yaml
2. Section override: Set logging.client.* sections in jobmonconfig.yaml

The core package handles requester-specific OTLP configuration, while this
package handles the broader client application logging configuration.
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

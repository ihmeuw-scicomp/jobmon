"""Jobmon core configuration package.

This package contains:
- Shared logging configuration templates
- Requester-specific OTLP logconfig (core responsibility)
- Template loader for YAML !template and !include directives
- Default configuration values

Client-specific configurations are now in jobmon.client.config.
Server-specific configurations are in jobmon.server.web.config.

The template system allows for DRY configuration management across
all packages while maintaining clean boundaries.
"""

from .logconfig_utils import (
    configure_logging_with_overrides,
    get_logconfig_examples,
    load_logconfig_with_overrides,
    merge_logconfig_sections,
)

# Make template loader and override utilities easily accessible
from .template_loader import load_logconfig_with_templates, load_yaml_with_templates

__all__ = [
    "load_logconfig_with_templates",
    "load_yaml_with_templates",
    "load_logconfig_with_overrides",
    "configure_logging_with_overrides",
    "merge_logconfig_sections",
    "get_logconfig_examples",
]

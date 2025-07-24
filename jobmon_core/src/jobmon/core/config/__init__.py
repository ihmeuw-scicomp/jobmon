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

from typing import Any

import structlog

from .logconfig_utils import (
    configure_logging_with_overrides,
    get_logconfig_examples,
    load_logconfig_with_overrides,
    merge_logconfig_sections,
)

# Make template loader and override utilities easily accessible
from .template_loader import load_logconfig_with_templates, load_yaml_with_templates


def StructlogFormatterFactory(
    renderer_type: str = "console", **kwargs: Any
) -> structlog.stdlib.ProcessorFormatter:
    """Factory function that creates ProcessorFormatter with instantiated processors.

    This avoids multiprocessing serialization issues by instantiating processors
    directly rather than using string references that fail in subprocess environments.

    Args:
        renderer_type: Either "console" or "json"
        **kwargs: Additional arguments passed to ProcessorFormatter

    Returns:
        A properly configured ProcessorFormatter instance
    """
    # Instantiate the appropriate processor directly
    if renderer_type == "console":
        processor: Any = structlog.dev.ConsoleRenderer()
    elif renderer_type == "json":
        processor = structlog.processors.JSONRenderer()
    else:
        raise ValueError(f"Unknown renderer_type: {renderer_type}")

    # Create ProcessorFormatter with instantiated processor (multiprocessing-safe)
    return structlog.stdlib.ProcessorFormatter(
        processor=processor,
        keep_exc_info=kwargs.get("keep_exc_info", True),
        keep_stack_info=kwargs.get("keep_stack_info", True),
        **{
            k: v
            for k, v in kwargs.items()
            if k not in ("keep_exc_info", "keep_stack_info", "renderer_type")
        },
    )


__all__ = [
    "load_logconfig_with_templates",
    "load_yaml_with_templates",
    "load_logconfig_with_overrides",
    "configure_logging_with_overrides",
    "merge_logconfig_sections",
    "get_logconfig_examples",
    "StructlogFormatterFactory",
]

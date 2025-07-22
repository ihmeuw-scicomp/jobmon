"""Shared logging configuration templates for jobmon.

This package contains YAML template files that define reusable patterns
for formatters, handlers, and OTLP exporters. These templates are used
by both client and server configurations via !template directives.

Templates:
- formatters.yaml: Console, OTLP, and structlog formatters
- otlp_exporters.yaml: OTLP/gRPC exporter configurations
- handlers.yaml: Logging handler patterns

The template system ensures consistency across all logging configurations
while eliminating duplication.
"""

"""Shared logging configuration templates for jobmon.

This package contains YAML template files that define reusable patterns
for formatters and complex handlers. These templates are used
by both client and server configurations via !template directives.

Templates:
- formatters.yaml: Console, OTLP, and structlog formatters
- handlers.yaml: Complex OTLP handlers (simple handlers are inlined)

The template system focuses on high-value templates while keeping
simple configurations inline for clarity.
"""

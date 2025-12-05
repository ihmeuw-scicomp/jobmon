=====================
Logging Architecture
=====================

This document captures the technical design of Jobmon's :mod:`structlog`
integration.  The goal is to collect telemetry metadata without disrupting the
host application's logging behaviour.

Overview
========

The architecture is composed of five primary pieces:

1. Context isolation system
2. Telemetry isolation processor
3. Structlog configuration strategy
4. Python logging handler configuration
5. Thread-local event storage for OTLP exporters
6. Direct-rendering forwarding shim

Context Isolation System
========================

*Location:* ``jobmon/core/logging/context.py``

The context module tracks Jobmon telemetry metadata independently from the
general structlog context:

* Import-time validation ensures ``structlog.contextvars.get_contextvars`` is
  available, otherwise a ``RuntimeError`` is raised.
* Helper APIs provide a context manager, query utilities, and explicit clear
  operations.
* All telemetry fields use the ``telemetry_`` prefix for automatic namespacing.
  The prefix is added automatically by ``set_jobmon_context`` and ``@bind_context``.
* Shared normalization helpers ensure the prefixing rules and ``None`` filtering
  are applied consistently across ``set_jobmon_context`` and ``bind_jobmon_context``.

Example usage::

   from jobmon.core.logging import bind_jobmon_context, get_jobmon_context

   with bind_jobmon_context(workflow_run_id=123):
       logger.info("Task started")

Telemetry Isolation Processor
=============================

*Location:* ``jobmon/core/config/structlog_config.py``

``create_telemetry_isolation_processor(prefixes)`` builds a structlog processor
that injects telemetry metadata into loggers whose name starts with the supplied
prefixes and strips the metadata everywhere else.  The processor is tagged with
``__jobmon_telemetry_isolation__`` so duplicate installation can be skipped.

Key behaviours:

* Telemetry is added using ``event_dict.setdefault`` so existing keys are not
  overwritten.
* Host loggers are cleaned by removing each metadata key explicitly.
* The processor short-circuits when no metadata is bound.

Structlog Configuration Strategy
================================

*Location:* ``jobmon/core/config/structlog_config.py``

``configure_structlog`` composes the processor chain Jobmon requires and leaves
rendering to the host application.  The default chain is:

1. ``structlog.contextvars.merge_contextvars``
2. Component processor (optional) – adds the ``component`` field
3. ``structlog.stdlib.filter_by_level``
4. ``structlog.stdlib.add_logger_name``
5. ``structlog.stdlib.add_log_level``
6. Telemetry isolation processor (optional)
7. Extra processors supplied by the caller
8. ``_store_event_dict_for_otlp`` – captures the raw event for OTLP handlers
9. ``_forward_event_to_logging_handlers`` – mirrors structlog events to stdlib
   handlers when the host renders output directly
10. ``structlog.stdlib.ProcessorFormatter.wrap_for_formatter`` – ensures stdlib
    logging handlers continue to function

The new ``_build_structlog_processor_chain`` helper centralises this assembly so
``configure_structlog`` and ``prepend_jobmon_processors_to_existing_config`` share
identical ordering rules.  The builder takes explicit flags for OTLP capture,
logger name enforcement, and forwarding shims, which makes it easier to reason
about the direct-rendering path while keeping stdlib integrations unchanged.
``configure_structlog`` now also accepts optional ``extra_processors`` so callers
can append lightweight enrichment processors without reimplementing the base
chain.  ``configure_structlog_with_otlp`` delegates to this entrypoint after
resolving OTLP span processors, eliminating duplicate configuration code.

``prepend_jobmon_processors_to_existing_config`` supports host-controlled
configurations by prepending missing processors. Telemetry metadata is isolated
to ``jobmon.*`` logger namespaces. Ensures OTLP capture is available by injecting
``_store_event_dict_for_otlp`` when the host configuration does not already include it.

Two helper functions keep the layering accurate:

* ``_uses_stdlib_integration`` inspects both the configured logger factory and
  wrapper class.  It recognises ``structlog.stdlib.LoggerFactory``/``BoundLogger``
  pairs as stdlib, while ``structlog.PrintLoggerFactory`` or custom factories are
  handled as direct renderers.  Unknown factories default to "stdlib" to retain
  safe behaviour.
* ``_forward_event_to_logging_handlers`` is only appended when Jobmon detects a
  direct-rendering host.  It synthesises a ``logging.LogRecord`` (including
  ``exc_info``) and hands it to any stdlib handlers attached to the logger.  OTLP
  handlers therefore receive identical payloads regardless of how the host
  renders console output.

Python Logging Handlers
=======================

*Location:* ``jobmon_client/src/jobmon/client/logging.py``

Two pathways exist:

* **Direct rendering hosts** (e.g. FHS) – ``_configure_client_logging_for_direct_rendering``
  still loads the standard template but prunes non-Jobmon handlers, retaining
  only the OTLP handler definitions.  Combined with
  ``_forward_event_to_logging_handlers`` this preserves host-controlled console
  rendering while keeping OTLP telemetry flowing.

Direct-rendering Forwarding Shim
================================

``_forward_event_to_logging_handlers`` bridges the remaining gap between
structlog's direct-rendering factories and stdlib handlers.  When Jobmon detects
that the host renders events itself (no stdlib integration) the processor copies
each processed event into a ``logging.LogRecord`` and forwards it to the
configured handlers.  The shim preserves ``exc_info`` tuples so OTLP exports
include stack traces and error types.

This processor is only installed once (``_processor_present`` guards it) and is
ignored when the logger has no handlers, keeping the hot path inexpensive.

* **Stdlib integration** – ``configure_client_logging`` delegates to the
  template-based configuration system (``logconfig_client.yaml``) which defines
  console and OTLP handlers.  Overrides can be provided through JobmonConfig
  files or sections.


Thread-local Event Storage
==========================

``_store_event_dict_for_otlp`` caches the structured ``event_dict`` in
thread-local storage whenever OTLP handlers are active.  The helper is guarded
by a reference count, exposed via ``enable_structlog_otlp_capture`` and
``disable_structlog_otlp_capture``.  For convenience – particularly in tests –
``structlog_otlp_capture_enabled`` offers a context manager that ensures the
count is decremented even when exceptions occur.

OTLP handlers retrieve the cached event from the thread-local when building
``opentelemetry`` log records.  This allows non-Jobmon loggers to omit telemetry
metadata while exporters still capture the full payload.

Architecture Detection
======================

When the host configures structlog first, Jobmon prepends processors to the
existing configuration::

   from jobmon.core.config.structlog_config import (
       is_structlog_configured,
       prepend_jobmon_processors_to_existing_config,
   )

   if is_structlog_configured():
       prepend_jobmon_processors_to_existing_config()
   else:
       configure_structlog(component_name="client")

The prepended processors are limited to the subset that is missing from the
host chain.  The isolation processor is skipped when an identical one is already
present (based on the stored prefixes tuple).

Handler Configuration Examples
==============================

Default client configuration (``jobmon_client/src/jobmon/client/config/logconfig_client.yaml``)::

   handlers:
     console:
       class: logging.StreamHandler
       formatter: structlog_event_only
     otlp_structlog:
       class: jobmon.core.otlp.JobmonOTLPLoggingHandler
       exporter: {}

   loggers:
     jobmon.client:
       handlers: [console]
       propagate: false

Local development configuration with OTLP enabled
(``docker_config/logconfig.otlp.yaml``)::

   handlers:
     console_structlog: {...}
     otlp_structlog:
       class: jobmon.core.otlp.JobmonOTLPLoggingHandler
       exporter: {}
     otlp:
       class: jobmon.core.otlp.JobmonOTLPLoggingHandler
       exporter: {}

   loggers:
     jobmon.client:
       handlers: [console_structlog, otlp_structlog]
       propagate: false
     uvicorn.access:
       handlers: [console_default, otlp]

Thread-local Storage and OTLP Handlers
======================================

``JobmonOTLPLoggingHandler`` (and its ``JobmonOTLPStructlogHandler`` alias)
import the cached event, extract telemetry metadata, and emit
``opentelemetry.sdk._logs.LogRecord`` instances.

Telemetry metadata is serialised only when the values are JSON compatible; more
complex structures are stringified to avoid errors.

Registration with the structlog capture hook is skipped when OTLP support is
unavailable, preventing unnecessary thread-local state in minimalist deployments.

Testing Strategy
================

Unit tests in ``tests/pytest/core/test_jobmon_context.py`` and
``tests/pytest/client/test_client_logging.py`` cover:

* Telemetry isolation for Jobmon vs host namespaces
* Context manager cleanup
* Custom telemetry prefixes
* Compatibility with FHS rendering

Integration tests cover host-first vs Jobmon-first configuration, OTLP export
behaviour, direct-rendering forwarding, and lazy configuration orchestration.

Troubleshooting Tips
====================

Host logs show Jobmon metadata
    Ensure the isolation processor is present and the host loggers do not use
    the ``jobmon.*`` namespace prefix.

Telemetry not exported
    Confirm ``_store_event_dict_for_otlp`` is in the processor chain and an OTLP
    handler is attached to ``jobmon.*`` loggers. Check ``telemetry.logging``
    settings in JobmonConfig and verify the exporter is properly configured.

Double console output
    Ensure ``propagate`` is set to ``false`` for Jobmon loggers and that handlers
    are not installed multiple times.

Formatter collisions with host applications
    Verify that Jobmon only prepends processors compatible with the host's
    rendering strategy. Update ``_uses_stdlib_integration`` when new deployment
    patterns emerge.

Performance Notes
=================

* The isolation processor operates in ``O(m)`` where ``m`` is the number of
  telemetry keys (~20).
* Thread-local capture copies the event dictionary once per log call when OTLP
  logging is enabled.
* Overall overhead is below 10% compared to pure structlog usage.

Maintenance Checklist
=====================

When adding telemetry metadata:

1. Ensure the key uses the ``telemetry_`` prefix (automatic when using ``set_jobmon_context`` or ``@bind_context``).
2. Bind the new key via ``set_jobmon_context`` or ``@bind_context``.
3. Extend documentation examples if the metadata is externally visible.

When supporting a new host architecture:

1. Enhance ``prepend_jobmon_processors_to_existing_config`` if different
   processors are required.
2. Update architecture detection heuristics.
3. Add integration tests for the new flow.

When debugging processor chains::

   import structlog
   processors = structlog.get_config().get("processors", [])
   for i, proc in enumerate(processors, 1):
       name = getattr(proc, "__name__", repr(proc))
       print(f"{i}. {name}")

Document History
================

*Last updated:* 2025-11-03 – Added direct-rendering forwarding shim, refined
client logging configuration for OTLP, and documented detection heuristics.


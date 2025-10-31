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

Context Isolation System
========================

*Location:* ``jobmon/core/logging/context.py``

The context module tracks Jobmon telemetry metadata independently from the
general structlog context:

* Import-time validation ensures ``structlog.contextvars.get_contextvars`` is
  available, otherwise a ``RuntimeError`` is raised.
* Helper APIs provide a context manager, query utilities, and explicit clear
  operations.
* ``JOBMON_METADATA_KEYS`` enumerates the telemetry fields; applications can
  extend the set at runtime through ``register_jobmon_metadata_keys``.

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
9. ``structlog.stdlib.ProcessorFormatter.wrap_for_formatter`` – ensures stdlib
   logging handlers continue to function

``prepend_jobmon_processors_to_existing_config`` supports host-controlled
configurations by prepending missing processors.  Callers may now pass
``telemetry_logger_prefixes`` to keep Jobmon metadata on additional namespaces.

The helper ``_uses_stdlib_integration`` identifies whether the host relies on
stdlib logging based on the configured logger factory and wrapper class.  When
the architecture cannot be determined, the function defaults to assuming stdlib
integration so that logging behaviour remains safe.

Python Logging Handlers
=======================

*Location:* ``jobmon_client/src/jobmon/client/logging.py``

Two pathways exist:

* **Direct rendering hosts** (e.g. FHS) – ``_configure_minimal_client_logging``
  establishes logger hierarchy without installing handlers.  Console output is
  handled entirely by the host, while telemetry flows through the structlog
  processors.
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
       prepend_jobmon_processors_to_existing_config(["jobmon.", "myapp.telemetry"])
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
       class: jobmon.core.otlp.JobmonOTLPStructlogHandler
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
       class: jobmon.core.otlp.JobmonOTLPStructlogHandler
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
``opentelemetry.sdk._logs.LogRecord`` instances with deduplication attributes
such as ``jobmon.emission_count`` and ``jobmon.is_duplicate``.

Telemetry metadata is serialised only when the values are JSON compatible; more
complex structures are stringified to avoid errors.

Testing Strategy
================

Unit tests in ``tests/pytest/core/test_jobmon_context.py`` cover:

* Telemetry isolation for Jobmon vs host namespaces
* Context manager cleanup
* Custom telemetry prefixes
* Compatibility with FHS rendering

Integration tests cover host-first vs Jobmon-first configuration, OTLP export
behaviour, and lazy configuration orchestration.

Troubleshooting Tips
====================

Host logs show Jobmon metadata
    Ensure the isolation processor is present, ``enable_jobmon_context`` is
    true, and the host loggers do not use Jobmon prefixes.

Telemetry not exported
    Confirm ``_store_event_dict_for_otlp`` is in the processor chain and an OTLP
    handler is attached to ``jobmon.*`` loggers.  Check ``telemetry.logging``
    settings in JobmonConfig and verify the exporter is properly configured.

Double console output
    Ensure ``propagate`` is set to ``false`` for Jobmon loggers and that handlers
    are not installed multiple times.

Formatter collisions with FHS
    Verify that Jobmon only prepends processors compatible with the host's
    rendering strategy.  Update ``_uses_stdlib_integration`` when new deployment
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

1. Update ``JOBMON_METADATA_KEYS``.
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

*Last updated:* 2025-10-31 – Major structlog simplification and configurable
telemetry prefixes.


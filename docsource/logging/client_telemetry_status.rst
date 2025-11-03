===============================================
Jobmon Client Telemetry – Requirements & Status
===============================================

Background
==========
Jobmon must emit structured telemetry for all ``jobmon.client.*`` loggers to the
OpenTelemetry (OTLP) pipeline, even when host applications fully control
structlog rendering (for example Forecasting Health Systems' ``PrintLogger``
configuration).  After introducing telemetry isolation and lazy configuration,
client logs continued to appear on the console but were missing from OTLP in
FHS deployments.  The recent code changes aimed to bridge that gap; however,
the issue persists and requires a precise statement of requirements and current
behaviour to guide further work.

Functional Requirements
=======================

1. **Telemetry completeness**
   - Every ``jobmon.client`` log entry should reach the configured OTLP exporter
     with full metadata (including bound context such as ``workflow_run_id`` and
     ``task_instance_id``) regardless of the host structlog renderer.

2. **Console neutrality**
   - Host applications retain sole control of console formatting when they
     render structlog events directly. Jobmon must not introduce duplicate or
     reformatted console output.

3. **Exception fidelity**
   - Errors logged via ``logger.exception`` or ``exc_info`` must propagate their
     traceback information into the OTLP payload.

4. **Isolation guarantees**
   - Non-Jobmon namespaces must remain free of Jobmon telemetry keys unless the
     host opts in. The solution cannot regress metadata isolation.

5. **Configuration compatibility**
   - The mechanism must work with the existing YAML-based logging templates and
     JobmonConfig overrides used in stage/dev installations.

Current Implementation (November 2025)
======================================

Structlog pipeline
------------------
* ``_store_event_dict_for_otlp`` saves the structured event in a thread-local
  cache when OTLP handlers are active.
* ``_forward_event_to_logging_handlers`` synthesises a ``logging.LogRecord`` and
  forwards it to stdlib handlers when Jobmon detects a direct-rendering host.
  The helper preserves ``exc_info`` payloads.
* ``_uses_stdlib_integration`` now inspects the configured logger factory, its
  wrapper chain (``__wrapped__``, ``wrapped_factory``, ``factory``, ``func``),
  and the wrapper class MRO to distinguish stdlib integration from direct
  renderers such as custom subclasses around ``structlog.PrintLoggerFactory``.

Client logging configuration
---------------------------
* ``configure_client_logging`` loads the default template and, for
  direct-rendering hosts, removes every handler except those whose class lives
  in ``jobmon.core.otlp``. The intention is to keep OTLP handlers while avoiding
  duplicate console output.
* When stdlib integration is detected, Jobmon applies the template unchanged
  (console + OTLP handlers).
* With ``telemetry.debug`` enabled, the direct-rendering path emits a single
  debug snapshot summarising the detected architecture, pruned handler list, and
  live ``jobmon.*`` handlers after configuration.
* After pruning, Jobmon now attaches ``JobmonOTLPStructlogHandler`` directly to
  any ``jobmon.*`` logger that ends up without handlers, ensuring the OTLP
  fallback remains in place even when ``dictConfig`` drops the handler entries.

Testing additions
-----------------
* ``test_direct_rendering_forwards_to_stdlib_handlers`` simulates a direct
  renderer and confirms that a stub OTLP handler receives forwarded
  ``LogRecord`` objects (with ``exc_info`` intact) after the client is
  configured.
* ``test_direct_rendering_attaches_otlp_handler_when_none_remaining`` exercises
  the post-``dictConfig`` fallback that reinstates OTLP handlers when pruned
  configs leave ``jobmon.*`` loggers empty.
* ``test_direct_rendering_multiple_config_calls_keep_single_handler`` guards
  against duplicate fallback handlers across repeated lazy configuration
  attempts.
* ``tests/pytest/core/test_structlog_detection.py`` covers subclasses,
  wrappers, and ``functools.partial`` wrappers around
  ``structlog.PrintLoggerFactory`` to keep detection heuristics honest.

Observed Behaviour
==================

* In controlled unit tests, the OTLP stub successfully receives forwarded
  records.
* In staging-like runs (FHS application + Jobmon client), OTLP telemetry for
  ``jobmon.client`` loggers remains absent. Distributor and server telemetry is
  present, so the pipeline itself is functional.
* Manual logging configuration inspection during the failing scenario shows
  that the final ``logging.config.dictConfig`` call installs an OTLP handler in
  the configuration dictionary, but the resulting ``jobmon.client`` logger in
  the runtime still has no handlers attached.

Identified Gaps & Questions
===========================

1. **Handler removal logic**
   - ``_remove_non_jobmon_handlers`` can still leave an empty handler list when
     overrides omit OTLP definitions. Capture the new debug snapshot in staging
     to confirm the pruned configuration that reaches ``dictConfig``.

2. **Direct-rendering detection**
   - Heuristics now unwrap nested factories, but we still need to confirm the
     actual factory type emitted by FHS deployments and extend the rules if it
     diverges further.

3. **Handler attachment timing**
   - The fallback reinstates OTLP handlers when ``dictConfig`` drops them, yet
     repeated reconfiguration might still produce short windows without
     handlers. Monitor the debug output to ensure the fallback fires only when
     necessary.

4. **OTLP handler registration**
   - Fallback attachment guarantees a handler instance, but we still need to
     verify that OTLP export is active by observing telemetry in staging after
     these changes land.

Plan for Further Investigation
===============================

1. Enable ``telemetry.debug`` in staging and collect the new snapshot to confirm
   the detected architecture, pruned handler lists, and any fallback activity.
2. Inspect ``structlog.get_config()`` during a failing run to verify that the
   expanded heuristics recognise the factory/wrapper chain provided by FHS.
3. Observe OTLP traffic (or the absence thereof) after the fallback attachment
   to confirm that ``JobmonOTLPStructlogHandler`` is instantiating and exporting
   successfully.
4. If telemetry is still missing, add targeted logging around
   ``logging.config.dictConfig`` invocation counts to spot duplicate
   reconfiguration cycles.

This document should be updated as new findings emerge so the requirements and
observed behaviour stay aligned.


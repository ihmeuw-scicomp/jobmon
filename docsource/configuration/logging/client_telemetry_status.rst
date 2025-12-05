:orphan:

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

1. **Telemetry completeness**: Every ``jobmon.client`` log entry should reach the
   configured OTLP exporter with full metadata (including bound context such as
   ``workflow_run_id`` and ``task_instance_id``) regardless of the host structlog
   renderer.

2. **Console neutrality**: Host applications retain sole control of console
   formatting when they render structlog events directly. Jobmon must not introduce
   duplicate or reformatted console output.

3. **Exception fidelity**: Errors logged via ``logger.exception`` or ``exc_info``
   must propagate their traceback information into the OTLP payload.

4. **Isolation guarantees**: Non-Jobmon namespaces must remain free of Jobmon
   telemetry keys unless the host opts in. The solution cannot regress metadata
   isolation.

5. **Configuration compatibility**: The mechanism must work with the existing
   YAML-based logging templates and JobmonConfig overrides used in stage/dev
   installations.

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
* When a direct renderer is detected, Jobmon wraps the host's
  ``PrintLoggerFactory`` with a light-weight proxy that adds a ``.name``
  attribute to the resulting logger while delegating every other behaviour to
  the host factory. This preserves fully-qualified logger names inside Jobmon's
  processors without altering console rendering.

Client logging configuration
----------------------------
* ``configure_client_logging`` loads the default template and, for
  direct-rendering hosts, removes every handler except those whose class lives
  in ``jobmon.core.otlp``. The intention is to keep OTLP handlers while avoiding
  duplicate console output.
* When stdlib integration is detected, Jobmon applies the template unchanged
  (console + OTLP handlers).
* After pruning, Jobmon attaches ``JobmonOTLPStructlogHandler`` directly to
  any ``jobmon.*`` logger that ends up without handlers, ensuring the OTLP
  fallback remains in place even when ``dictConfig`` drops the handler entries.
* Console neutrality is enforced by pruning Jobmon-supplied metadata from the
  event dictionary before returning control to the host renderer. Host-provided
  keys (such as FHS' ``[logger : function]`` segment) remain untouched, while
  Jobmon context keys (``workflow_run_id`` et al.) and diagnostic fields are
  only present in the OTLP payload.
* The shared OTLP manager exposes ``flush_and_shutdown()`` and installs
  best-effort ``atexit``/signal hooks so every short-lived component (client,
  distributor, worker node) pushes its final telemetry batch before exiting.

Host integration checklist
--------------------------
The following conditions must be satisfied by any host that renders structlog
events directly:

* Call ``jobmon.client.logging.configure_client_logging()`` *after* the host has
  invoked ``structlog.configure``.
* Avoid mutating ``jobmon.*`` loggers after Jobmon configuration. Jobmon relies
  on the handler set to detect whether OTLP capture is active.
* When binding workflow-level context, call ``set_jobmon_context`` as soon as a
  ``WorkflowRun`` identifier is available (in Jobmon client code this now happens
  immediately after the run object is created). This ensures downstream loggers
  and OTLP exports share the same ``workflow_run_id``.
* Leave the ``jobmon.core.otlp`` handlers intact in the final logging
  configuration.

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
* ``test_print_logger_factory_adds_logger_name`` verifies that the wrapper
  factory delivers fully-qualified logger names to the OTLP processors even when
  the host uses ``PrintLoggerFactory``.
* ``test_direct_rendering_console_output_is_message_only`` asserts that console
  renderers see only host-provided keys (event, level, timestamp, logger) while
  the OTLP stub receives the full structured payload with Jobmon metadata.

Observed Behaviour
==================

* Local docker-compose environments that mimic FHS' structlog configuration now
  emit every ``jobmon.client`` log to OTLP with the expected context metadata and
  fully-qualified logger names.
* Console output in direct-rendering hosts retains the host's human-readable
  format while omitting Jobmon telemetry keys, preventing duplicate context
  fragments from being shown to operators.
* Distributor, server, and worker node components continue to deliver OTLP
  telemetry with their existing pipelines.
* Worker nodes, which run as short-lived processes, now flush their final
  telemetry batch via the shared OTLP manager before exiting.

Debugging guidance
==================

1. Inspect ``structlog.get_config()`` after Jobmon configuration to confirm that
   the wrapper around ``PrintLoggerFactory`` is active (``__jobmon_named_factory__``)
   and the processor chain includes ``_ensure_logger_name``.
2. Check Elasticsearch or another OTLP sink for ``labels.logger`` values in the
   ``jobmon.client.*`` namespace and ensure ``numeric_labels.workflow_run_id`` is
   present.
3. When debugging console output, add a temporary processor to print the
   event dict immediately before the host renderer; Jobmon's pruning should have
   removed all keys beginning with ``jobmon_`` and any registered context keys.

Future work
===========

* Perform staging validation to ensure the new wrapper and console pruning
  behave identically under FHS load. Capture debug snapshots and OTLP samples to
  confirm parity with local results.
* Investigate whether progress counters (``newly_completed``/``percent_done``)
  should be exposed to console renderers behind a configuration flag for hosts
  that want them.
* Review other Jobmon components (distributor, worker, CLI tools) to determine
  whether the named ``PrintLogger`` wrapper should be reused there for
  consistency.


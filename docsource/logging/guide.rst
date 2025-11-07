=================
Logging Guide
=================

Jobmon ships with a :mod:`structlog`-based logging stack that captures
telemetry metadata by default while keeping host applications in control of
console rendering.  This guide focuses on day-to-day behaviour, integration
patterns, and configuration options.

For the detailed architectural blueprint refer to :doc:`architecture`.

Core Principles
===============

* **Automatic telemetry** – workflow metadata is collected as soon as Jobmon
  binds context; no explicit initialisation is required.
* **Explicit console output** – ``workflow.run()`` is silent unless you
  request logging (for example ``workflow.run(configure_logging=True)`` or by
  wiring up custom handlers).
* **Metadata isolation** – telemetry fields stay on loggers that belong to the
  Jobmon namespace (``jobmon.*`` by default) so the host application's output
  is not polluted.
* **Direct-rendering friendly** – when hosts render events themselves (for
  example via ``structlog.PrintLoggerFactory``), Jobmon mirrors the structured
  event into stdlib handlers so OTLP exporters still see the full payload.
* **Safe integration** – host :mod:`structlog` configuration remains in
  control; Jobmon only prepends the processors it requires.

All telemetry metadata uses the ``telemetry_`` prefix for automatic namespacing,
including identifiers such as ``telemetry_workflow_run_id``, ``telemetry_task_instance_id``,
``telemetry_array_id`` and ``telemetry_tool_version_id``. The prefix is added automatically
by ``set_jobmon_context`` and ``@bind_context``, and stripped when exporting to OTLP.

Quick Start – Host Applications
===============================

The logging stack is designed to co-operate with existing host configuration.

FHS-style Application
---------------------

.. code-block:: python

   import logging
   import structlog

   # Host logging configuration
   structlog.configure(
       processors=[my_metadata_stamper, my_fhs_renderer],
       wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
   )

   from jobmon.client.api import Tool

   wf = Tool("my_tool").create_workflow("my_workflow")
   wf.bind()

   wf.run()                       # Silent – telemetry captured only
   wf.run(configure_logging=True) # Console output rendered by host processors + OTLP

What you see:

* Application logs are unchanged.
* Jobmon console output only appears when explicitly enabled and uses your
  renderer.
* Telemetry is captured and exported whenever OTLP logging is enabled—even for
  direct-rendering hosts—because Jobmon forwards structured events to the OTLP
  handler on your behalf.

Lazy Configuration
------------------

Jobmon defers structlog setup until the first workflow operation.  Whether the
host configures structlog before or after importing Jobmon, the behaviour is
identical.

.. code-block:: python

   structlog.configure(processors=[...])
   import jobmon.client
   workflow.run()  # ✅ adapts to host config

   import jobmon.client
   structlog.configure(processors=[...])
   workflow.run()  # ✅ still adapts

Quick Start – Jobmon Developers
===============================

Bind telemetry context with the helpers provided in
``jobmon.core.structlog_utils`` or ``jobmon.core.logging``.

.. code-block:: python

   from jobmon.core.structlog_utils import bind_context

   @bind_context("workflow_run_id", "task_instance_id")
   def launch_task(workflow_run_id: int, task_instance_id: int):
       logger = structlog.get_logger(__name__)
       logger.info("Launching task")

Manual control:

.. code-block:: python

   from jobmon.core.logging import set_jobmon_context, unset_jobmon_context

   set_jobmon_context(workflow_run_id=123, task_instance_id=456)
   try:
       logger = structlog.get_logger(__name__)
       logger.info("Processing task")
   finally:
       unset_jobmon_context("workflow_run_id", "task_instance_id")

Telemetry & Console Behaviour
=============================

* ``set_jobmon_context()`` stores metadata in structlog's context variables with
  automatic ``telemetry_`` prefixing.
* Both ``set_jobmon_context`` and ``bind_jobmon_context`` share the same
  normalization rules, so ``None`` values are dropped automatically and keys are
  always prefixed consistently.
* ``create_telemetry_isolation_processor()`` injects metadata into loggers whose
  names start with configured prefixes (``["jobmon."]`` by default) and removes
  the metadata for other namespaces.
* ``_prune_event_dict_for_console()`` strips all keys starting with ``telemetry_``
  from console output while preserving them for OTLP exports.
* OTLP handlers strip the ``telemetry_`` prefix before export for backward compatibility.
* ``_store_event_dict_for_otlp`` copies the event dictionary to thread-local
  storage when OTLP handlers are active.
* Console logging is disabled by default; enable via
  ``workflow.run(configure_logging=True)`` or custom log configuration.

Configuration Examples
======================

Enable OTLP Telemetry
---------------------

.. code-block:: yaml

   telemetry:
     logging:
       enabled: true
       log_exporter: otlp_http
       exporters:
         otlp_http:
           endpoint: "https://otelcol.example.com"
           timeout: 30

Custom Console Logging
----------------------

.. code-block:: yaml

   logging:
     client_logconfig_file: "/path/to/custom_logging.yaml"

.. code-block:: yaml

   version: 1
   handlers:
     custom_console:
       class: logging.StreamHandler
       formatter: custom_format
   formatters:
     custom_format:
       format: "%(asctime)s [%(name)s] %(message)s"
   loggers:
     jobmon.client:
       handlers: [custom_console]
       level: DEBUG

Configure Component Name
-------------------------

.. code-block:: python

   from jobmon.core.config.structlog_config import configure_structlog

   configure_structlog(component_name="client")

Add custom processors without rebuilding the Jobmon defaults::

   from jobmon.core.config.structlog_config import configure_structlog

   configure_structlog(
       component_name="client",
       extra_processors=[my_custom_processor],
   )

FAQ
===

Why are Jobmon logs formatted like my application logs?
    Jobmon prepends its processors but leaves your renderer at the end of
    the chain, so your format applies to every log entry.

Can I surface ``workflow_run_id`` in host logs?
    Not by default. Fields with the ``telemetry_`` prefix are automatically
    stripped from console output to keep telemetry separate from user-facing logs.
    Configure your host renderer to show keys with the ``telemetry_`` prefix if needed.

Does Jobmon slow down logging?
    Typical overhead is ~3 microseconds per log call (context merge + isolation
    + OTLP capture when enabled).

Can I use Jobmon without OTLP?
    Yes.  Telemetry capture runs regardless of the exporter; if no OTLP handler
    is present the extra processing is skipped.

Testing & Support
=================

Unit tests cover context binding, metadata isolation, custom prefixes, and
integration with FHS-style renderers (per
``tests/pytest/core/test_jobmon_context.py``).

Integration suites verify stdlib versus direct rendering hosts, OTLP export, and
lazy configuration paths.  For additional help open an issue in the Jobmon
repository or continue with :doc:`architecture` for implementation details.


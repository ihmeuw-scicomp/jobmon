=================
Logging Guide
=================

Jobmon ships with a :mod:`structlog`-based logging stack that captures
telemetry metadata by default while keeping host applications in control of
console rendering.

For implementation details, see :doc:`/developers_guide/logging_architecture`.

Key Behaviors
=============

* **Silent by default** – ``workflow.run()`` produces no console output unless 
  you explicitly enable logging.
* **Easy to enable** – Use ``workflow.run(configure_logging=True)`` to enable
  console output with sensible defaults.
* **OTLP telemetry** – When configured, workflow metadata is automatically
  exported to your telemetry backend.
* **Host-friendly** – If your application already configures structlog, Jobmon
  integrates without disrupting your setup.

Quick Start
===========

Enable Console Logging
----------------------

The simplest way to see Jobmon logs:

.. code-block:: python

   from jobmon.client.api import Tool

   tool = Tool("my_tool")
   wf = tool.create_workflow("my_workflow")
   # ... add tasks ...
   
   wf.run(configure_logging=True)  # Enables console output

Without ``configure_logging=True``, the workflow runs silently (telemetry is
still captured if OTLP is configured).

Host Application Integration
----------------------------

If your application already configures structlog, Jobmon adapts automatically:

.. code-block:: python

   import structlog

   # Your application's structlog configuration
   structlog.configure(
       processors=[my_custom_processor, my_renderer],
       wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
   )

   from jobmon.client.api import Tool
   
   wf = Tool("my_tool").create_workflow("my_workflow")
   wf.run()  # Jobmon adapts to your structlog config

Jobmon prepends its processors to your chain but leaves your renderer in
control, so logs appear in your preferred format.

Configuration
=============

Enable OTLP Log Export
----------------------

Configure OTLP log export in your ``jobmonconfig.yaml``:

.. code-block:: yaml

   telemetry:
     deployment_environment: prod
     
     logging:
       enabled: true
       log_exporter: http_log
       
       exporters:
         http_log:
           module: opentelemetry.exporter.otlp.proto.http._log_exporter
           class: OTLPLogExporter
           endpoint: "https://otelcol.example.com/v1/logs"
           timeout: 30

Custom Logconfig File
---------------------

For advanced customization, provide your own logconfig file:

.. code-block:: yaml

   # In jobmonconfig.yaml
   logging:
     client_logconfig_file: "/path/to/custom_logging.yaml"

Example custom logconfig:

.. code-block:: yaml

   # custom_logging.yaml
   version: 1
   disable_existing_loggers: false
   
   formatters:
     structlog_event_only:
       '()': jobmon.core.config.structlog_formatters.JobmonStructlogEventOnlyFormatter
   
   handlers:
     console:
       class: logging.StreamHandler
       level: INFO
       formatter: structlog_event_only
     
     otlp:
       class: jobmon.core.otlp.JobmonOTLPLoggingHandler
       level: INFO
       exporter: {}  # Uses telemetry.logging.exporters config
   
   loggers:
     jobmon.client:
       handlers: [console, otlp]
       level: INFO
       propagate: false

FAQ
===

Why don't I see any Jobmon logs?
    Console logging is disabled by default. Use 
    ``workflow.run(configure_logging=True)`` to enable it.

Why are Jobmon logs formatted like my application logs?
    Jobmon integrates with your existing structlog configuration, so your
    formatter controls the output style.

Can I use Jobmon without OTLP?
    Yes. OTLP is optional. Without it configured, Jobmon just doesn't export
    telemetry—everything else works normally.

My OTLP logs are missing workflow metadata
    Ensure ``telemetry.logging.enabled: true`` in your jobmonconfig and that
    the OTLP handler is properly configured on ``jobmon.*`` loggers.

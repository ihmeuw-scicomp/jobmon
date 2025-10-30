==========================
Jobmon Configuration System
==========================

This guide covers the configuration system for jobmon, including template-based logging configurations and user override capabilities.

Overview
--------

The jobmon configuration system provides:

1. **Shared Templates**: Reusable logging patterns in ``templates/``
2. **Default Configurations**: Component-specific logconfigs using templates
3. **User Override System**: Flexible customization via ``JobmonConfig``
4. **Environment Variable Support**: All overrides support env vars

Template System
---------------

Shared Templates (``templates/``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- ``formatters.yaml``: Console, OTLP, and structlog formatters
- ``otlp_exporters.yaml``: OTLP/gRPC exporter configurations  
- ``handlers.yaml``: Logging handler patterns

These templates are used by client, server, and requester configurations to ensure consistency.

Template Usage
^^^^^^^^^^^^^^

Templates are referenced using ``!template`` directives:

.. code-block:: yaml

   formatters: !template formatters
   handlers:
     console:
       class: logging.StreamHandler
       formatter: console_default
     otlp_client:
       class: jobmon.core.otlp.JobmonOTLPLoggingHandler
       exporter: !template otlp_grpc_exporter

User Override System
--------------------

Users can customize logging configurations in multiple ways:

1. File-Based Overrides (Highest Precedence)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Replace the entire logging configuration with a custom file:

**YAML Configuration:**

.. code-block:: yaml

   logging:
     client_logconfig_file: "/path/to/custom/client_config.yaml"
     server_logconfig_file: "/path/to/custom/server_config.yaml"
     requester_logconfig_file: "/path/to/custom/requester_config.yaml"

**Environment Variables:**

.. code-block:: bash

   export JOBMON__LOGGING__CLIENT_LOGCONFIG_FILE="/path/to/custom/client_config.yaml"
   export JOBMON__LOGGING__SERVER_LOGCONFIG_FILE="/path/to/custom/server_config.yaml"
   export JOBMON__LOGGING__REQUESTER_LOGCONFIG_FILE="/path/to/custom/requester_config.yaml"

2. Section-Based Overrides
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Override specific sections while keeping template defaults:

**YAML Configuration:**

.. code-block:: yaml

   logging:
     client:
       formatters:
         custom:
           format: "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
           datefmt: "%Y-%m-%d %H:%M:%S"
       handlers:
         file:
           class: logging.FileHandler
           filename: "/var/log/jobmon_client.log"
           formatter: custom
           level: INFO
       loggers:
         jobmon.client.workflow:
           handlers: [console, file]
           level: DEBUG
           propagate: false
           
     server:
       loggers:
         myapp:
           handlers: [console_structlog]
           level: INFO
           propagate: false
           
     requester:
       handlers:
         custom_otlp:
           class: jobmon.core.otlp.JobmonOTLPLoggingHandler
           level: DEBUG
           exporter: !template otlp_grpc_exporter

**Environment Variables:**

.. code-block:: bash

   # Add custom formatter
   export JOBMON__LOGGING__CLIENT__FORMATTERS__CUSTOM__FORMAT="%(name)s: %(message)s"

   # Set logger level
   export JOBMON__LOGGING__SERVER__LOGGERS__MYAPP__LEVEL="DEBUG"

   # Add handlers to logger
   export JOBMON__LOGGING__CLIENT__LOGGERS__JOBMON_CLIENT_WORKFLOW__HANDLERS='["console", "file"]'

3. Configuration Precedence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The system follows this precedence order:

1. **Explicit Parameters**: ``dict_config`` or ``file_config`` parameters
2. **File Overrides**: ``logging.{component}_logconfig_file``
3. **Section Overrides**: ``logging.{component}.{section}``
4. **Default Templates**: Component-specific template files
5. **Fallback**: Basic hardcoded configuration

Component-Specific Configurations
----------------------------------

Client Logging
^^^^^^^^^^^^^^

**Default Template**: ``jobmon_client/src/jobmon/client/config/logconfig_client.yaml``

**Configured Loggers**:

- ``jobmon.client``: General client logging
- ``jobmon.client.workflow``: Workflow execution
- ``jobmon.client.task``: Task operations
- ``jobmon.client.tool``: Tool management
- ``jobmon``: Overall jobmon logging

**Usage**:

.. code-block:: python

   from jobmon.client.logging import configure_client_logging
   configure_client_logging()  # Automatically applies user overrides

Server Logging
^^^^^^^^^^^^^^

**Default Template**: 
- ``jobmon_server/src/jobmon/server/web/config/logconfig_server.yaml``

**Features**:

- OTLP configuration via file overrides (``server_logconfig_file``)
- Structlog integration for server components
- Support for both console and JSON output

**Usage**:

.. code-block:: python

   from jobmon.server.web.log_config import configure_logging
   configure_logging()  # Automatically applies user overrides and OTLP selection

Requester Logging
^^^^^^^^^^^^^^^^^

**Configuration**: Handled by general client logging configuration

**Purpose**: HTTP request/response tracing integrated with client logging

**Features**:

- ``jobmon.core.requester`` logger follows client logging configuration
- OTLP integration available through client logconfig files
- HTTP request tracing controlled by ``telemetry.tracing.requester_enabled``

**Usage**: Logging is configured by ``configure_client_logging()``. Tracing is enabled when ``telemetry.tracing.requester_enabled`` is true.

Component Logging (Distributor, Worker, Server)
------------------------------------------------

Overview
^^^^^^^^

Jobmon components (distributor, worker_node, server) support automatic logging configuration that integrates seamlessly with the existing template and override system. Components automatically configure console logging by default during startup without manual intervention. OTLP logging can be enabled via configuration overrides when telemetry is desired.

Supported Components
^^^^^^^^^^^^^^^^^^^^

- **Distributor**: Long-running subprocess that distributes jobs to clusters
- **Worker**: Short-lived subprocess that executes individual tasks  
- **Server**: Web application serving the Jobmon API
- **Client**: Interactive commands and workflow operations

Configuration Methods
^^^^^^^^^^^^^^^^^^^^^^

**Method 1: Template-Based (Default)**

Components use default templates with console logging in their local package directories:

- ``jobmon.distributor/config/logconfig_distributor.yaml`` - Console logging with OTLP examples
- ``jobmon.worker_node/config/logconfig_worker.yaml`` - Console logging with OTLP examples  
- ``jobmon.server.web/config/logconfig_server.yaml`` - Console logging with OTLP examples
- ``jobmon.client/config/logconfig_client.yaml`` - Console logging with OTLP examples

**Default Behavior**: All components log to console by default. OTLP is disabled unless explicitly enabled via configuration overrides.

**Method 2: File Override**

.. code-block:: yaml

   # jobmonconfig.yaml
   logging:
     distributor_logconfig_file: /path/to/custom/distributor_config.yaml
     worker_logconfig_file: /path/to/custom/worker_config.yaml
     client_logconfig_file: /path/to/custom/client_config.yaml

**Method 3: Section Override**

.. code-block:: yaml

   # jobmonconfig.yaml
   logging:
     distributor:
       handlers:
         otlp_distributor:
           class: jobmon.core.otlp.JobmonOTLPLoggingHandler
           level: INFO
           exporter:
             module: opentelemetry.exporter.otlp.proto.grpc._log_exporter
             class: OTLPLogExporter
             endpoint: https://production-otlp.company.com:4317
             timeout: 30
             insecure: false
             max_export_batch_size: 16
             export_timeout_millis: 5000
             schedule_delay_millis: 2000
             max_queue_size: 4096
       loggers:
         jobmon.distributor:
           handlers: [otlp_distributor]
           level: INFO
           propagate: false

Configuration Precedence
^^^^^^^^^^^^^^^^^^^^^^^^^

Component logging follows the same precedence as client/server logging:

1. **File Override**: ``logging.{component}_logconfig_file``
2. **Template**: ``logconfig_{component}.yaml``
3. **Section Override**: ``logging.{component}.*``
4. **No Logging**: Component starts with no logging if no configuration found

Automatic Behavior
^^^^^^^^^^^^^^^^^^^

- **Zero Configuration**: Components work out-of-box with console logging
- **Graceful Degradation**: Components start successfully even if logging fails
- **OTLP Opt-In**: OTLP logging disabled by default, enabled via configuration overrides
- **Library-Safe**: Only configures jobmon-specific loggers, never touches the root logger
- **Performance Optimized**: When OTLP is enabled, different batching settings per component type
- **Resource Attributes**: When OTLP is enabled, generic "jobmon" service with process-level attributes

Usage
^^^^^

Component logging is automatically enabled when CLIs are created:

.. code-block:: python

   # Distributor CLI automatically enables logging
   from jobmon.distributor.cli import DistributorCLI

   cli = DistributorCLI()  # Automatic logging configured
   cli.main()  # Distributor runs with logging

   # Worker node CLI automatically enables logging
   from jobmon.worker_node.cli import WorkerNodeCLI

   worker_cli = WorkerNodeCLI()  # Automatic logging configured
   worker_cli.main()  # Worker runs with logging

   # Client CLI automatically enables logging
   from jobmon.client.cli import ClientCLI
   
   client_cli = ClientCLI()  # Automatic logging configured
   client_cli.main()  # Client commands run with logging

   # Workflow operations with component logging
   workflow.run(configure_logging=True)  # Uses client component logging

Examples
--------

Example 1: Add File Logging to Client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

   # ~/.jobmon.yaml
   logging:
     client:
       formatters:
         file_formatter:
           format: "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
           datefmt: "%Y-%m-%d %H:%M:%S"
       handlers:
         file:
           class: logging.FileHandler
           filename: "/var/log/jobmon_client.log"
           formatter: file_formatter
           level: INFO
       loggers:
         jobmon.client:
           handlers: [console, file]  # Add file handler to existing console

Example 2: Custom Server Logging for Production
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

   # /etc/jobmon/config.yaml
   logging:
     server:
       formatters:
         custom_json:
           format: '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
       handlers:
         json_file:
           class: logging.handlers.RotatingFileHandler
           filename: "/var/log/jobmon_server.json"
           maxBytes: 10485760  # 10MB
           backupCount: 5
           formatter: custom_json
       loggers:
         jobmon.server.web:
           handlers: [console_structlog, json_file]
           level: INFO
         sqlalchemy:
           handlers: [json_file]
           level: WARN

Example 3: Custom OTLP Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

   # Custom OTLP endpoint and settings
   logging:
     requester:
       handlers:
         custom_otlp:
           class: jobmon.core.otlp.JobmonOTLPLoggingHandler
           level: DEBUG
           exporter:
             module: opentelemetry.exporter.otlp.proto.grpc._log_exporter
             class: OTLPLogExporter
             endpoint: "https://my-otel-collector:4317"
             options:
               - ["grpc.max_send_message_length", 33554432]  # 32MB
               - ["grpc.max_receive_message_length", 33554432]
             max_export_batch_size: 16
             export_timeout_millis: 10000
       loggers:
         jobmon.core.requester:
           handlers: [console, custom_otlp]

Example 4: Environment Variable Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # Set custom client logconfig file
   export JOBMON__LOGGING__CLIENT_LOGCONFIG_FILE="/path/to/custom_client.yaml"

   # Override specific logger level
   export JOBMON__LOGGING__CLIENT__LOGGERS__JOBMON_CLIENT_WORKFLOW__LEVEL="DEBUG"

   # Add custom formatter
   export JOBMON__LOGGING__SERVER__FORMATTERS__PRODUCTION__FORMAT="%(asctime)s %(name)s %(levelname)s %(message)s"

Configuration Examples
----------------------

See ``docsource/examples/`` for comprehensive configuration examples including:

- ``component_logging_examples.yaml``: File-based and section-based configuration for all components
- ``distributor_logging_examples.yaml``: Detailed distributor configuration patterns

These examples include:

- File-based configuration for Docker/local development
- Section-based configuration for production deployments  
- Mixed configuration for enterprise environments
- Performance tuning guidelines

Migration from Legacy System
----------------------------

The old ``JobmonLoggerConfig`` has been replaced. If you were using:

.. code-block:: python

   # OLD
   from jobmon.client.logging import JobmonLoggerConfig
   JobmonLoggerConfig.attach_default_handler("my.logger", logging.INFO)

   # NEW
   from jobmon.client.logging import configure_client_logging
   configure_client_logging()  # Configures all client loggers with overrides

For custom configurations, use the override system instead of manual configuration.

Troubleshooting
---------------

Configuration Not Applied
^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Check file paths exist and are readable
2. Verify YAML syntax in custom config files
3. Check environment variable names (double underscores: ``JOBMON__SECTION__KEY``)
4. Ensure configuration is loaded before logging calls

Template Errors
^^^^^^^^^^^^^^^^

1. Check that template files exist in ``jobmon_core/src/jobmon/core/config/templates/``
2. Verify ``!template`` references use correct names
3. Check cross-package template loading paths

OTLP Issues
^^^^^^^^^^^

1. Verify OTLP dependencies are installed
2. Check ``otlp.web_enabled`` and ``otlp.http_enabled`` settings
3. Verify OTLP endpoint accessibility
4. Check exporter configuration in templates

Component Logging Issues
^^^^^^^^^^^^^^^^^^^^^^^^^

**Component not logging**: Check if template exists and configuration is valid:

.. code-block:: python

   from jobmon.core.config.logconfig_utils import _get_component_template_path
   import os

   # Check if component template exists
   distributor_template = _get_component_template_path("distributor")
   print(f"Distributor template path: {distributor_template}")
   print(f"Template exists: {os.path.exists(distributor_template) if distributor_template else False}")

**OTLP not working**: Verify OTLP dependencies and endpoint configuration:

.. code-block:: bash

   # Check OTLP availability
   python -c "from jobmon.core.otlp import OTLP_AVAILABLE; print(OTLP_AVAILABLE)"

**Performance issues**: Adjust batching settings for your environment:

- Development: Small batches, fast export (responsive debugging)
- Production: Large batches, slower export (efficient throughput)

Development
-----------

Library Logging Best Practices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Important**: Jobmon follows proper library logging practices:

- **Never configures the root logger**: This is the application's responsibility
- **Only configures jobmon-specific loggers**: ``jobmon.*`` namespace only
- **Propagation enabled by default**: Allows application-level control
- **No forced handlers**: Applications can override or disable all logging

This ensures Jobmon integrates cleanly with any application's logging setup without conflicts.

Adding New Templates
^^^^^^^^^^^^^^^^^^^^

1. Add template YAML files to ``templates/`` directory
2. Use YAML anchors (``&name``) for reusable patterns
3. Reference with ``!template template_name`` in configurations
4. Update template loader if new template files added
5. **Never include root logger configuration** in templates

Testing Overrides
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from jobmon.core.config.logconfig_utils import get_logconfig_examples
   examples = get_logconfig_examples()
   print(examples["section_override_example"])
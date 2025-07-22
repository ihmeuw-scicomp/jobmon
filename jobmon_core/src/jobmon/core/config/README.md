# Jobmon Configuration System

This directory contains the configuration system for jobmon, including template-based logging configurations and user override capabilities.

## Overview

The jobmon configuration system provides:

1. **Shared Templates**: Reusable logging patterns in `templates/`
2. **Default Configurations**: Component-specific logconfigs using templates
3. **User Override System**: Flexible customization via `JobmonConfig`
4. **Environment Variable Support**: All overrides support env vars

## Template System

### Shared Templates (`templates/`)

- `formatters.yaml`: Console, OTLP, and structlog formatters
- `otlp_exporters.yaml`: OTLP/gRPC exporter configurations  
- `handlers.yaml`: Logging handler patterns

These templates are used by client, server, and requester configurations to ensure consistency.

### Template Usage

Templates are referenced using `!template` directives:

```yaml
formatters: !template formatters
handlers:
  console:
    class: logging.StreamHandler
    formatter: console_default
  otlp_client:
    class: jobmon.core.otlp.JobmonOTLPLoggingHandler
    exporter: !template otlp_grpc_exporter
```

## User Override System

Users can customize logging configurations in multiple ways:

### 1. File-Based Overrides (Highest Precedence)

Replace the entire logging configuration with a custom file:

**YAML Configuration:**
```yaml
logging:
  client_logconfig_file: "/path/to/custom/client_config.yaml"
  server_logconfig_file: "/path/to/custom/server_config.yaml"
  requester_logconfig_file: "/path/to/custom/requester_config.yaml"
```

**Environment Variables:**
```bash
export JOBMON__LOGGING__CLIENT_LOGCONFIG_FILE="/path/to/custom/client_config.yaml"
export JOBMON__LOGGING__SERVER_LOGCONFIG_FILE="/path/to/custom/server_config.yaml"
export JOBMON__LOGGING__REQUESTER_LOGCONFIG_FILE="/path/to/custom/requester_config.yaml"
```

### 2. Section-Based Overrides

Override specific sections while keeping template defaults:

**YAML Configuration:**
```yaml
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
        formatter: otlp_default
        exporter: !template otlp_grpc_exporter
```

**Environment Variables:**
```bash
# Add custom formatter
export JOBMON__LOGGING__CLIENT__FORMATTERS__CUSTOM__FORMAT="%(name)s: %(message)s"

# Set logger level
export JOBMON__LOGGING__SERVER__LOGGERS__MYAPP__LEVEL="DEBUG"

# Add handlers to logger
export JOBMON__LOGGING__CLIENT__LOGGERS__JOBMON_CLIENT_WORKFLOW__HANDLERS='["console", "file"]'
```

### 3. Configuration Precedence

The system follows this precedence order:

1. **Explicit Parameters**: `dict_config` or `file_config` parameters
2. **File Overrides**: `logging.{component}_logconfig_file`
3. **Section Overrides**: `logging.{component}.{section}`
4. **Default Templates**: Component-specific template files
5. **Fallback**: Basic hardcoded configuration

## Component-Specific Configurations

### Client Logging

**Default Template**: `jobmon_client/src/jobmon/client/config/logconfig_client.yaml`

**Configured Loggers**:
- `jobmon.client`: General client logging
- `jobmon.client.workflow`: Workflow execution
- `jobmon.client.task`: Task operations
- `jobmon.client.tool`: Tool management
- `jobmon`: Overall jobmon logging

**Usage**:
```python
from jobmon.client.logging import configure_client_logging
configure_client_logging()  # Automatically applies user overrides
```

### Server Logging

**Default Templates**: 
- `jobmon_server/src/jobmon/server/web/config/logconfig_server.yaml` (OTLP disabled)
- `jobmon_server/src/jobmon/server/web/config/logconfig_server_otlp.yaml` (OTLP enabled)

**Features**:
- Automatic OTLP selection based on `otlp.web_enabled`
- Structlog integration for server components
- Support for both console and JSON output

**Usage**:
```python
from jobmon.server.web.log_config import configure_logging
configure_logging()  # Automatically applies user overrides and OTLP selection
```

### Requester Logging

**Default Template**: `jobmon_core/src/jobmon/core/config/logconfig_requester_otlp.yaml`

**Purpose**: HTTP request/response tracing with OTLP

**Features**:
- Focused on `jobmon.core.requester` logger
- Automatic OTLP configuration when enabled
- Integration with request instrumentation

**Usage**: Automatically configured when `Requester` is instantiated with OTLP enabled.

## Examples

### Example 1: Add File Logging to Client

```yaml
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
```

### Example 2: Custom Server Logging for Production

```yaml
# /etc/jobmon/config.yaml
logging:
  server:
    handlers:
      json_file:
        class: logging.handlers.RotatingFileHandler
        filename: "/var/log/jobmon_server.json"
        maxBytes: 10485760  # 10MB
        backupCount: 5
        formatter: structlog_json
    loggers:
      jobmon.server.web:
        handlers: [console_structlog, json_file]
        level: INFO
      sqlalchemy:
        handlers: [json_file]
        level: WARN
```

### Example 3: Custom OTLP Configuration

```yaml
# Custom OTLP endpoint and settings
logging:
  requester:
    handlers:
      custom_otlp:
        class: jobmon.core.otlp.JobmonOTLPLoggingHandler
        level: DEBUG
        formatter: otlp_default
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
```

### Example 4: Environment Variable Configuration

```bash
# Set custom client logconfig file
export JOBMON__LOGGING__CLIENT_LOGCONFIG_FILE="/path/to/custom_client.yaml"

# Override specific logger level
export JOBMON__LOGGING__CLIENT__LOGGERS__JOBMON_CLIENT_WORKFLOW__LEVEL="DEBUG"

# Add custom formatter
export JOBMON__LOGGING__SERVER__FORMATTERS__PRODUCTION__FORMAT="%(asctime)s %(name)s %(levelname)s %(message)s"
```

## Migration from Legacy System

The old `JobmonLoggerConfig` has been replaced. If you were using:

```python
# OLD
from jobmon.client.logging import JobmonLoggerConfig
JobmonLoggerConfig.attach_default_handler("my.logger", logging.INFO)

# NEW
from jobmon.client.logging import configure_client_logging
configure_client_logging()  # Configures all client loggers with overrides
```

For custom configurations, use the override system instead of manual configuration.

## Troubleshooting

### Configuration Not Applied

1. Check file paths exist and are readable
2. Verify YAML syntax in custom config files
3. Check environment variable names (double underscores: `JOBMON__SECTION__KEY`)
4. Ensure configuration is loaded before logging calls

### Template Errors

1. Check that template files exist in `jobmon_core/src/jobmon/core/config/templates/`
2. Verify `!template` references use correct names
3. Check cross-package template loading paths

### OTLP Issues

1. Verify OTLP dependencies are installed
2. Check `otlp.web_enabled` and `otlp.http_enabled` settings
3. Verify OTLP endpoint accessibility
4. Check exporter configuration in templates

## Development

### Adding New Templates

1. Add template YAML files to `templates/` directory
2. Use YAML anchors (`&name`) for reusable patterns
3. Reference with `!template template_name` in configurations
4. Update template loader if new template files added

### Testing Overrides

```python
from jobmon.core.config.logconfig_utils import get_logconfig_examples
examples = get_logconfig_examples()
print(examples["section_override_example"])
``` 
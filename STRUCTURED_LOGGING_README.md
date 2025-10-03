# Structured Logging with Context Binding

Complete implementation of structured logging for Jobmon with automatic context binding and APM integration.

## Quick Start

### Using the Decorator

```python
import structlog
from jobmon.core.structlog_utils import bind_context

logger = structlog.get_logger(__name__)

@bind_context(task_instance_id="task_instance.task_instance_id")
def launch_task(self, task_instance):
    logger.info("Task launched", distributor_id=dist_id)
    # task_instance_id automatically in all logs
```

### Global Context

```python
# Set once
structlog.contextvars.bind_contextvars(
    cluster_name="slurm",
    workflow_run_id=123
)

# Available in all subsequent logs
logger.info("Processing batch")
# Includes cluster_name and workflow_run_id automatically
```

## Configuration

All components support the same configuration pattern:

```yaml
logging:
  client_logconfig_file: /path/to/logconfig.yaml
  server_logconfig_file: /path/to/logconfig.yaml
  distributor_logconfig_file: /path/to/logconfig.yaml
  worker_logconfig_file: /path/to/logconfig.yaml
```

Valid components: `client`, `server`, `distributor`, `worker`

## APM/Kibana Queries

Fields are categorized by type in APM:
- **Numbers**: `numeric_labels.workflow_run_id`, `numeric_labels.task_instance_id`
- **Strings**: `labels.component`, `labels.cluster_name`

### Example Queries

```
// All distributor logs
labels.component: "distributor"

// Specific workflow
numeric_labels.workflow_run_id: 123

// Combined
labels.component: "distributor" AND numeric_labels.workflow_run_id: 123

// Find errors
labels.component: "distributor" AND log.level: "ERROR"

// Performance issues
numeric_labels.duration_seconds > 60
```

For complete query reference, see: `design/APM_QUERY_GUIDE.md`

## Testing

```bash
# Test OTLP attribute extraction
docker exec jobmon-jobmon_client-1 python /app/test_scripts/emit_single_log.py

# Then search in APM using the printed values
```

## Implementation Details

For technical architecture and implementation details, see:
- `design/STRUCTURED_LOGGING_IMPLEMENTATION.md` - Architecture
- `design/APM_QUERY_GUIDE.md` - Query reference  
- `design/CHANGES_SUMMARY.md` - Complete file changes

## Status

✅ Production ready
- 0 linter errors
- OTLP attributes verified in APM
- All components configured
- All tests passing


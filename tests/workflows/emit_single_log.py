#!/usr/bin/env python
"""Test script to verify structured logging and OTLP attribute extraction.

Usage:
    From client container:
        docker exec jobmon-jobmon_client-1 python /app/test_scripts/emit_single_log.py
    
    From server container:
        docker exec jobmon-jobmon_backend-1 python -c "exec(open('/app/test_scripts/emit_single_log.py').read())"

This emits a single test log with unique searchable values to verify:
1. Structlog context binding works
2. OTLP attributes are extracted (not JSON in message)
3. Fields are searchable in APM/Kibana
"""

import time
import structlog
from jobmon.core.config.logconfig_utils import configure_component_logging
from jobmon.core.config.structlog_config import configure_structlog

# Configure logging with OTLP support
configure_component_logging("client")
configure_structlog(component_name="test")

# Bind unique context values
test_id = int(time.time())
structlog.contextvars.bind_contextvars(
    test_unique_id=test_id,
    test_search_key=555555,
    test_marker="APM_VERIFICATION"
)

# Emit test log
logger = structlog.get_logger("jobmon.client.test")
logger.info("TEST_LOG_FOR_APM_SEARCH", test_value=999)

# Print search instructions
print("="*70)
print(f"✅ Test log emitted at {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("="*70)
print()
print("Search in APM/Kibana for:")
print()
print(f"  numeric_labels.test_unique_id: {test_id}")
print("  numeric_labels.test_search_key: 555555")
print('  labels.test_marker: "APM_VERIFICATION"')
print("  numeric_labels.test_value: 999")
print('  message: "TEST_LOG_FOR_APM_SEARCH"')
print()
print("Or use time filter:")
print('  service.name: "jobmon" AND service.environment: "local"')
print(f'  @timestamp: {time.strftime("%Y-%m-%dT%H:%M")}*')
print()
print("Expected in APM:")
print("  ✓ message field contains ONLY: \"TEST_LOG_FOR_APM_SEARCH\"")
print("  ✓ Separate fields: test_unique_id, test_search_key, test_marker, test_value")
print("  ✓ NOT a JSON dict in message")
print("="*70)


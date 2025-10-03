#!/usr/bin/env python
"""Test context propagation through actual HTTP requests.

This makes REAL requests using the Requester to verify:
1. Client binds context
2. Requester captures and sends via header
3. Server receives and binds context
4. Server logs include client context
"""

import time
import structlog
from jobmon.client.api import Tool
from jobmon.core.config.logconfig_utils import configure_component_logging
from jobmon.core.config.structlog_config import configure_structlog

# Configure client
configure_component_logging("client")
configure_structlog(component_name="client")

# Bind unique client context
test_id = int(time.time())
structlog.contextvars.bind_contextvars(
    propagation_test_id=test_id,
    client_component="test_client",
    test_marker="REAL_PROPAGATION"
)

logger = structlog.get_logger("jobmon.client")
logger.info("Starting context propagation test")

# Create tool - this makes HTTP requests
tool = Tool()

# This triggers HTTP request to server
# Server should receive and log with client context
tool.set_default_compute_resources_from_dict(
    cluster_name="sequential",
    compute_resources={"queue": "null.q"}
)

logger.info("Tool created, context should be on server now")

print("="*70)
print("✅ Context Propagation Test - Real HTTP Requests")
print("="*70)
print(f"Propagation Test ID: {test_id}")
print()
print("The client made HTTP requests with these context fields:")
print(f"  - propagation_test_id: {test_id}")
print('  - client_component: "test_client"')
print('  - test_marker: "REAL_PROPAGATION"')
print()
print("Search in APM for SERVER logs (component='server') that include:")
print()
print(f"  numeric_labels.propagation_test_id: {test_id}")
print('  labels.client_component: "test_client"')
print('  labels.test_marker: "REAL_PROPAGATION"')
print('  labels.component: "server"')
print()
print("If you find server logs with these client fields, propagation works!")
print("="*70)


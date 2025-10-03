#!/usr/bin/env python
"""Test that structlog context propagates from client through requests to server.

This demonstrates automatic context correlation:
1. Client binds context (workflow_run_id, cluster_name, etc.)
2. Requester automatically captures and sends via header
3. Server receives and binds context
4. Server logs include client's context automatically!
"""

import time
import structlog
from jobmon.client.api import Tool

# Configure
from jobmon.core.config.logconfig_utils import configure_component_logging
from jobmon.core.config.structlog_config import configure_structlog

configure_component_logging("client")
configure_structlog(component_name="client")

# Bind client-side context
test_id = int(time.time())
structlog.contextvars.bind_contextvars(
    client_test_id=test_id,
    client_marker="PROPAGATION_TEST",
    custom_context="FROM_CLIENT"
)

logger = structlog.get_logger("jobmon.client")
logger.info("Client creating tool")

# Create tool (will make requests to server with context)
tool = Tool()

logger.info("Client setting compute resources")
tool.set_default_compute_resources_from_dict(
    cluster_name="sequential",
    compute_resources={"queue": "null.q"}
)

logger.info("Client getting task template")
task_template = tool.get_task_template(
    template_name="context_prop_test",
    command_template="echo 'Test'",
    node_args=[],
    task_args=[],
    op_args=[]
)

logger.info("Client creating task")
task = task_template.create_task()

logger.info("Client creating workflow")
workflow = tool.create_workflow(name=f"context_propagation_test_{test_id}")
workflow.add_tasks([task])

logger.info("Client binding workflow")
workflow.bind()  # This makes HTTP requests to server

print("="*70)
print("✅ Context Propagation Test Complete")
print("="*70)
print(f"Client Test ID: {test_id}")
print(f"Workflow ID: {workflow.workflow_id}")
print()
print("Search in APM for SERVER logs that include CLIENT context:")
print()
print(f"  numeric_labels.client_test_id: {test_id}")
print('  labels.client_marker: "PROPAGATION_TEST"')
print('  labels.custom_context: "FROM_CLIENT"')
print(f"  numeric_labels.workflow_id: {workflow.workflow_id}")
print()
print("Server logs for workflow creation should include:")
print("  - client_test_id (from client)")
print("  - client_marker (from client)")
print("  - custom_context (from client)")
print("  - component: 'server' (from server)")
print()
print("This proves context propagates through HTTP requests!")
print("="*70)


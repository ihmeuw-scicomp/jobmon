"""Commands to check for workflow and task status (from CLI).

This module re-exports functions from the new modular command structure
for backward compatibility. New code should import directly from:
- jobmon.client.commands.workflow
- jobmon.client.commands.task
- jobmon.client.commands.resources
- jobmon.client.commands.config
- jobmon.client.commands.validation
"""

# Re-export all public functions from the new modular structure
# for backward compatibility

from jobmon.client.commands.config import (
    update_config_value,
)
from jobmon.client.commands.resources import (
    _create_yaml,
    _get_yaml_data,
    create_resource_yaml,
    task_template_resources,
)
from jobmon.client.commands.task import (
    get_sub_task_tree,
    get_task_dependencies,
    task_status,
    update_task_status,
)
from jobmon.client.commands.validation import chunk_ids as _chunk_ids
from jobmon.client.commands.validation import (
    validate_username,
    validate_workflow,
)
from jobmon.client.commands.workflow import (
    concurrency_limit,
    get_filepaths,
    resume_workflow_from_id,
    workflow_reset,
    workflow_status,
    workflow_tasks,
)

__all__ = [
    # Workflow commands
    "workflow_status",
    "workflow_tasks",
    "workflow_reset",
    "concurrency_limit",
    "get_filepaths",
    "resume_workflow_from_id",
    # Task commands
    "task_status",
    "update_task_status",
    "get_task_dependencies",
    "get_sub_task_tree",
    # Resource commands
    "task_template_resources",
    "create_resource_yaml",
    "_get_yaml_data",
    "_create_yaml",
    # Config commands
    "update_config_value",
    # Validation helpers
    "validate_username",
    "validate_workflow",
    "_chunk_ids",
]

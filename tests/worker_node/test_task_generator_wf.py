import pytest
import os
from typing import Any, List, Optional, Tuple
from unittest.mock import Mock

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

def test_simple_task(client_env, monkeypatch):
    # Set up function
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    from tests.worker_node.task_generator_funcs import simple_function
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="dummy", compute_resources={"queue": "null.q"}
    )
    compute_resources = {}
    wf = tool.create_workflow()
    for i in range(5):
        task = simple_function.create_task(compute_resources=compute_resources, foo=i)
        wf.add_tasks([task])
    r = wf.run()
    assert r == "D"
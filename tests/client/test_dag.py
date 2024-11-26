import logging
import random
from typing import List

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.client.task import Task
from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow
from jobmon.client.workflow_run import WorkflowRun, WorkflowRunFactory
from jobmon.core.constants import MaxConcurrentlyRunning, WorkflowRunStatus
from jobmon.core.exceptions import (
    WorkflowAlreadyComplete,
    DuplicateNodeArgsError,
    WorkflowAlreadyExists,
    NodeDependencyNotExistError,
)

def test_dag_validation(tool, db_engine):
    # test upstream validation
    t1 = tool.active_task_templates["phase_1"].create_task(arg="sleep 1")
    t2 = tool.active_task_templates["phase_2"].create_task(
        arg="sleep 2", upstream_tasks=[t1]
    )

    # initial workflow should run to completion
    wf1 = tool.create_workflow(name="upstream_validation")
    wf1.add_tasks([t2])
    # test if NodeDependencyNotExistError error
    with pytest.raises(NodeDependencyNotExistError, match=r"Upstream|task_template_version_id|dat.Node"):
        wf1.bind()

    # test downstream validation
    _ = tool.active_task_templates["phase_3"].create_task(
        arg="sleep 3", upstream_tasks=[t2]
    )
    wf2 = tool.create_workflow(name="downstream_validation")
    wf2.add_tasks([t1, t2])
    # test if NodeDependencyNotExistError error
    with pytest.raises(NodeDependencyNotExistError, match=r"Downstream|task_template_version_id|dat.Node"):
        wf2.bind()

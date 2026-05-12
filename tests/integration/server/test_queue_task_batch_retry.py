"""Regression test for queue_task_batch rollback that wiped the gate.

Bug: ``record_array_batch_num`` called ``gate_tasks_for_queueing`` outside the
TI-creation retry loop. When the ``SELECT ... FOR UPDATE NOWAIT`` on
``task_instance.array_batch_num`` raised ``OperationalError`` under concurrent
load, the ``except`` branch's ``db.rollback()`` reverted both the partial TI
work AND the gate's ``G->Q`` transitions. The retry then ran the
``INSERT ... FROM SELECT`` filtered on ``Task.status == QUEUED``, which matched
zero rows because the gate had been rolled back. The route committed nothing,
returned 200 with all tasks reporting ``REGISTERING``. The client had already
dequeued these tasks from ``ready_to_run``, so they evaporated and the
orchestrator declared a premature exit.

Fix: gate inside the retry loop so the gate and the TI insert stay in the
same atomic unit; the rollback wipes both and the next attempt re-runs both.

Observed in prod on Emu wfrs 386417 (85 stuck tasks) and 386675 (10,423 stuck
tasks). Confirmed via the ``_dump_stuck_registering_tasks`` diagnostic
showing ``num_upstreams_done == num_upstreams`` (counter fine, enqueue
dropped) coincident with ``DB error during TI creation ... NOWAIT`` log lines.

The test calls the route handler directly with a fresh ``Session`` bound to
the test engine (not the wrapped ``dbsession`` fixture), because the route
issues its own ``db.commit()``/``db.rollback()`` and those would fight with
``dbsession``'s outer transactional wrap.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskStatus
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_resources_type import TaskResourcesType
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.routes.v3.fsm.array import record_array_batch_num
from jobmon.server.web.services.transition_service import TransitionService


@pytest.fixture
def array_with_registering_tasks(db_engine):
    """Build the minimum DB graph needed to call ``record_array_batch_num``.

    Uses ``db_engine`` directly (not the ``dbsession`` fixture) so that
    commits from the route under test land in the real test database and the
    final ``SELECT`` for the response can see them. Cleans up rows it owns at
    teardown so the suite remains isolated.
    """
    session = Session(bind=db_engine)

    cluster_type = ClusterType(name="ct_queue_retry")
    session.add(cluster_type)
    session.flush()

    cluster = Cluster(name="c_queue_retry", cluster_type_id=cluster_type.id)
    session.add(cluster)
    session.flush()

    queue = Queue(name="q_queue_retry", cluster_id=cluster.id, parameters="{}")
    session.add(queue)
    session.flush()

    task_resources_type = TaskResourcesType(id="O", label="ORIGINAL")
    session.merge(task_resources_type)
    session.flush()

    tool = Tool(name="tool_queue_retry")
    session.add(tool)
    session.flush()

    tool_version = ToolVersion(tool_id=tool.id)
    session.add(tool_version)
    session.flush()

    task_template = TaskTemplate(tool_version_id=tool_version.id, name="tt_queue_retry")
    session.add(task_template)
    session.flush()

    task_template_version = TaskTemplateVersion(
        task_template_id=task_template.id,
        command_template="echo {arg}",
        arg_mapping_hash="arg_hash_queue_retry",
    )
    session.add(task_template_version)
    session.flush()

    node = Node(
        task_template_version_id=task_template_version.id,
        node_args_hash="node_hash_queue_retry",
    )
    session.add(node)
    session.flush()

    dag = Dag(hash="dag_hash_queue_retry")
    session.add(dag)
    session.flush()

    workflow = Workflow(
        tool_version_id=tool_version.id,
        dag_id=dag.id,
        name="wf_queue_retry",
        workflow_args_hash="wf_hash_queue_retry",
        task_hash="task_hash_queue_retry",
        max_concurrently_running=10,
        status="G",
    )
    session.add(workflow)
    session.flush()

    array = Array(
        workflow_id=workflow.id,
        task_template_version_id=task_template_version.id,
        name="array_queue_retry",
        max_concurrently_running=10,
    )
    session.add(array)
    session.flush()

    workflow_run = WorkflowRun(
        workflow_id=workflow.id,
        status="R",
        user="test_user",
    )
    session.add(workflow_run)
    session.flush()

    task_resources = TaskResources(
        queue_id=queue.id,
        task_resources_type_id="O",
        requested_resources="{}",
    )
    session.add(task_resources)
    session.flush()

    tasks = []
    for i in range(3):
        task = Task(
            workflow_id=workflow.id,
            node_id=node.id,
            array_id=array.id,
            task_args_hash=f"hash_queue_retry_{i}",
            name=f"task_queue_retry_{i}",
            command=f"echo task {i}",
            status=TaskStatus.REGISTERING,
            max_attempts=3,
            task_resources_id=task_resources.id,
        )
        session.add(task)
        tasks.append(task)
    session.commit()

    task_ids = [t.id for t in tasks]
    array_id = array.id
    workflow_id = workflow.id
    workflow_run_id = workflow_run.id
    task_resources_id = task_resources.id

    yield {
        "array_id": array_id,
        "task_ids": task_ids,
        "workflow_run_id": workflow_run_id,
        "task_resources_id": task_resources_id,
    }

    # Teardown: roll back to the original REGISTERING state and delete owned
    # rows. Order matters for FK integrity.
    cleanup = Session(bind=db_engine)
    cleanup.execute(
        delete(TaskInstance).where(TaskInstance.workflow_run_id == workflow_run_id)
    )
    cleanup.execute(
        delete(TaskStatusAudit).where(TaskStatusAudit.workflow_id == workflow_id)
    )
    cleanup.execute(delete(Task).where(Task.workflow_id == workflow_id))
    cleanup.execute(delete(WorkflowRun).where(WorkflowRun.id == workflow_run_id))
    cleanup.execute(delete(TaskResources).where(TaskResources.id == task_resources_id))
    cleanup.execute(delete(Array).where(Array.id == array_id))
    cleanup.execute(delete(Workflow).where(Workflow.id == workflow_id))
    cleanup.commit()
    cleanup.close()
    session.close()


def _build_fake_request(payload: dict):
    """Build a Request-like object whose ``.json()`` returns ``payload``."""
    request = MagicMock()
    request.json = AsyncMock(return_value=payload)
    return request


@pytest.mark.asyncio
async def test_queue_task_batch_re_gates_after_nowait_rollback(
    db_engine, array_with_registering_tasks, monkeypatch
):
    """NOWAIT during TI creation must re-run the gate on retry.

    Forces a NOWAIT OperationalError on the first SELECT FOR UPDATE call inside
    the route's retry loop. The handler rolls back (which reverts the gate);
    the next attempt must re-gate and successfully insert TaskInstances.
    """
    fixture = array_with_registering_tasks
    array_id = fixture["array_id"]
    task_ids = fixture["task_ids"]
    workflow_run_id = fixture["workflow_run_id"]
    task_resources_id = fixture["task_resources_id"]

    # Run the route with its own session bound to the real engine, so its
    # ``db.commit()`` / ``db.rollback()`` calls work normally.
    db = Session(bind=db_engine)

    # Spy on the gate so we can assert it was invoked twice (once per attempt).
    # ``gate_tasks_for_queueing`` is a classmethod; the route calls it as
    # ``TransitionService.gate_tasks_for_queueing(...)``, so we replace with a
    # staticmethod wrapper to avoid double-binding ``cls``.
    original_gate = TransitionService.gate_tasks_for_queueing
    gate_calls = {"count": 0}

    def spy_gate(*args, **kwargs):
        gate_calls["count"] += 1
        return original_gate(*args, **kwargs)

    monkeypatch.setattr(
        TransitionService, "gate_tasks_for_queueing", staticmethod(spy_gate)
    )

    # Fault-inject OperationalError("nowait") on the first SELECT FOR UPDATE
    # call. SQLite's compiler silently drops the NOWAIT clause from compiled
    # SQL, so detect via the Select object's ``_for_update_arg.nowait`` flag
    # instead of substring-matching the compiled SQL.
    original_execute = db.execute
    nowait_calls = {"count": 0}

    def faulty_execute(stmt, *args, **kwargs):
        fua = getattr(stmt, "_for_update_arg", None)
        if fua is not None and getattr(fua, "nowait", False):
            nowait_calls["count"] += 1
            if nowait_calls["count"] == 1:
                raise OperationalError(
                    "SELECT ... FOR UPDATE NOWAIT",
                    {},
                    Exception(
                        "Statement aborted because lock(s) could not be "
                        "acquired immediately and NOWAIT is set."
                    ),
                )
        return original_execute(stmt, *args, **kwargs)

    monkeypatch.setattr(db, "execute", faulty_execute)

    request = _build_fake_request(
        {
            "task_ids": task_ids,
            "task_resources_id": task_resources_id,
            "workflow_run_id": workflow_run_id,
        }
    )

    try:
        response = await record_array_batch_num(
            array_id=array_id, request=request, db=db
        )
    finally:
        db.close()

    # NOWAIT-style FOR UPDATE was attempted twice: once on the failed first
    # attempt (we raise), once on the successful retry (we let it through).
    assert nowait_calls["count"] == 2, (
        f"Expected NOWAIT path to fire twice (1 failed + 1 retry); fired "
        f"{nowait_calls['count']}"
    )
    # Gate must have been invoked twice: once on the failed attempt, once on
    # the successful retry. With the bug (gate outside the loop), this would
    # be 1.
    assert gate_calls["count"] == 2, (
        "Gate must be re-run inside the retry loop. With the pre-fix code "
        "(gate outside the loop), the rollback wipes the gate and the retry "
        "issues a 0-row INSERT, leaving tasks at REGISTERING and silently "
        f"dropping them on the client side. gate_calls={gate_calls['count']}"
    )

    # The response status code must be 200 and report tasks at QUEUED.
    assert response.status_code == 200
    body = json.loads(response.body)
    assert (
        TaskStatus.QUEUED in body["tasks_by_status"]
    ), f"Expected QUEUED in response, got: {list(body['tasks_by_status'].keys())}"
    assert set(body["tasks_by_status"][TaskStatus.QUEUED]) == set(task_ids)

    # Verify the DB committed the gate + TI insert successfully. Read via a
    # fresh session so we see the route's committed work.
    verify = Session(bind=db_engine)
    try:
        for task_id in task_ids:
            status = verify.execute(
                select(Task.status).where(Task.id == task_id)
            ).scalar()
            assert (
                status == TaskStatus.QUEUED
            ), f"Task {task_id} should be QUEUED after retry, got {status}"

        ti_count = verify.execute(
            select(TaskInstance).where(TaskInstance.task_id.in_(task_ids))
        ).all()
        assert len(ti_count) == len(task_ids), (
            f"Expected {len(task_ids)} TaskInstance rows after retry, "
            f"got {len(ti_count)}"
        )
    finally:
        verify.close()

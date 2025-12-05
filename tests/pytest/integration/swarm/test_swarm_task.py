import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import WorkflowRunStatus
from tests.pytest.integration.swarm.swarm_test_utils import (
    create_test_context,
    set_adjusted_task_resources,
    set_validated_task_resources,
)


@pytest.mark.parametrize(
    "scales",
    [
        {"cores": 0.5},
        {"cores": iter([12, 18])},
        {"cores": lambda x: x * 1.5},
    ],
)
def test_swarmtask_resources_integration(scales, tool, task_template, db_engine):
    """Check that taskresources defined in task are passed to swarmtask appropriately"""

    workflow = tool.create_workflow(default_cluster_name="multiprocess")

    # Create tasks
    task = task_template.create_task(
        arg="echo qux",
        compute_resources={"cores": 8, "queue": "null.q"},
        resource_scales=scales,
        cluster_name="multiprocess",
    )

    # Add to workflow, bind and create wfr
    workflow.add_task(task)
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # Move workflow and wfr through Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # Build swarm state
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )

    # Check swarmtask resources
    swarmtask = state.tasks[task.task_id]
    initial_resources = swarmtask.current_task_resources
    assert initial_resources.requested_resources == {
        "cores": 8,
    }

    # Queue the task. TRs should then be validated
    set_validated_task_resources(orchestrator, swarmtask)
    # No change in resource values, so type id stays the same
    assert id(swarmtask.current_task_resources) == id(initial_resources)

    # Move task to adjusting
    with Session(bind=db_engine) as session:
        sql = """
            UPDATE task
            SET status = :status
            WHERE id = :id
        """
        session.execute(text(sql), {"status": "A", "id": swarmtask.task_id})
        session.commit()

    # Call adjust.
    set_adjusted_task_resources(orchestrator, swarmtask)
    scaled_params = swarmtask.current_task_resources
    assert id(scaled_params) != id(initial_resources)
    assert scaled_params.requested_resources == {
        "cores": 12,
    }

    # Call adjust a second time.
    set_adjusted_task_resources(orchestrator, swarmtask)
    scaled_params = swarmtask.current_task_resources
    assert id(scaled_params) != id(initial_resources)
    assert scaled_params.requested_resources == {
        "cores": 18,
    }


@pytest.mark.parametrize("scales", [None, {}])
def test_swarmtask_resources_integration_no_scales(
    scales, tool, task_template, db_engine
):
    """Check that empty resource_scales are a no-op."""

    workflow = tool.create_workflow(default_cluster_name="multiprocess")

    # Create tasks
    task = task_template.create_task(
        arg="echo qux",
        compute_resources={"cores": 8, "queue": "null.q"},
        resource_scales=scales,
        cluster_name="multiprocess",
    )

    # Add to workflow, bind and create wfr
    workflow.add_task(task)
    workflow.bind()
    workflow._bind_tasks()
    factory = WorkflowRunFactory(workflow.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # Move workflow and wfr through Instantiating -> Launched
    wfr._update_status(WorkflowRunStatus.INSTANTIATED)
    wfr._update_status(WorkflowRunStatus.LAUNCHED)

    # Build swarm state
    state, gateway, orchestrator = create_test_context(
        workflow, wfr.workflow_run_id, workflow.requester
    )

    # Check swarmtask resources
    swarmtask = state.tasks[task.task_id]
    initial_resources = swarmtask.current_task_resources
    assert initial_resources.requested_resources == {
        "cores": 8,
    }

    # Queue the task. TRs should then be validated
    set_validated_task_resources(orchestrator, swarmtask)
    # No change in resource values, so type id stays the same
    assert id(swarmtask.current_task_resources) == id(initial_resources)

    # Move task to adjusting
    with Session(bind=db_engine) as session:
        sql = """
            UPDATE task
            SET status = :status
            WHERE id = :id
        """
        session.execute(text(sql), {"status": "A", "id": swarmtask.task_id})
        session.commit()

    # Call adjust.
    set_adjusted_task_resources(orchestrator, swarmtask)
    scaled_params = swarmtask.current_task_resources
    assert id(scaled_params) == id(initial_resources)
    assert scaled_params.requested_resources == {
        "cores": 8,
    }

    # Call adjust a second time.
    set_adjusted_task_resources(orchestrator, swarmtask)
    scaled_params = swarmtask.current_task_resources
    assert id(scaled_params) == id(initial_resources)
    assert scaled_params.requested_resources == {
        "cores": 8,
    }

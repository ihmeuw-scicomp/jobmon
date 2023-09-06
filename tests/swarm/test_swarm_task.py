import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.core.constants import WorkflowRunStatus
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun


@pytest.mark.parametrize(
    "scales",
    [
        {"cores": 0.5},
        {"cores": iter([15])},
        {"cores": lambda x: x + 5},
    ],
)
def test_swarmtask_resources_integration(scales, tool, task_template, db_engine):
    """Check that taskresources defined in task are passed to swarmtask appropriately"""

    workflow = tool.create_workflow(default_cluster_name="multiprocess")

    # Create tasks
    task = task_template.create_task(
        arg="echo qux",
        compute_resources={"cores": 10, "queue": "null.q"},
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

    # swarm calls
    swarm = SwarmWorkflowRun(
        workflow_run_id=wfr.workflow_run_id,
        requester=workflow.requester,
    )
    swarm.from_workflow(workflow)
    # Check swarmtask resources
    swarmtask = swarm.tasks[task.task_id]
    initial_resources = swarmtask.current_task_resources
    assert initial_resources.requested_resources == {
        "cores": 10,
    }

    # Queue the task. TRs should then be validated
    swarm._set_validated_task_resources(swarmtask)
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
    swarm._set_adjusted_task_resources(swarmtask)
    scaled_params = swarmtask.current_task_resources
    assert id(scaled_params) != id(initial_resources)
    assert scaled_params.requested_resources == {
        "cores": 15,
    }

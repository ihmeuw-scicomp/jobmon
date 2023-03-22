from jobmon.server.web.models.api import Batch
from jobmon.distributor.distributor_task_instance import DistributorTaskInstance

import pytest


@pytest.mark.skip("TODO")
def test_batch_completion(client_env):

    requester = Requester(client_env)

    ti1 = TaskInstance()
    ti2 = TaskInstance()

    requester.send_request(
        "/batch/queue_task_instance_batch",
        message={'task_instance_ids': [ti1.task_instance_id, ti2.task_instance_id],
                 'workflow_run_id': 1},
        request_type="put"
    )

    session = SessionLocal()
    with session.begin():
        total_tis = session.execute(
            select(Batch.total_task_instances).where(Batch.workflow_run_id == 1)
        ).one()

        assert total_tis == 2

    # Mark the task instances as done
    # TODO: possible design, any way to force race conditions?
    ti1.update_status("D")
    ti2.update_status("D")

    with session.begin():
        num_completed = session.execute(
            select(Batch.completed_task_instances).where(Batch.workflow_run_id == 1)
        ).one()

        assert num_completed == 2

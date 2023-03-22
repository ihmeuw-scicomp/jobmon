from jobmon.distributor.distributor_instance import DistributorInstance as ClientDistributor
from jobmon.server.web.models.api import (
    Batch as ServerBatch,
    TaskInstance,
)

import pytest
from sqlalchemy.orm import Session


def test_active_workflow_run_sync(requester_in_memory, requester_no_retry, distributor_crud):

    distributor_id = distributor_crud['distributor_ids'][0]

    distributor = ClientDistributor(clusters=[], requester=requester_no_retry)
    distributor._distributor_instance_id = distributor_id

    distributor.refresh_status_from_db(status="Q")
    # Should have appended a "make task instances" function tot he distributor chain
    command = next(distributor._distributor_commands)
    command()

    task_instance_ids = distributor_crud['task_instance_ids'][:3]

    assert task_instance_ids[0] in distributor._task_instances
    assert task_instance_ids[1] in distributor._task_instances
    assert task_instance_ids[2] not in distributor._task_instances


@pytest.mark.skip("TODO")
def test_batch_selection_sync(distributor_crud):

    batch1 = ServerBatch(distributor_instance_id=distributor.id)

    ti1 = TaskInstance(status="Q", batch_id=batch1.id)
    ti2 = TaskInstance(status="Q", batch_id=batch1.id)
    ti3 = TaskInstance(status="Q", batch_id=999999)

    distributor.refresh_status_from_db(status="Q")
    assert ti1.id in distributor._task_instances
    assert ti2.id in distributor._task_instances
    assert ti3.id not in distributor._task_instances


@pytest.mark.skip("TODO")
def test_status_change_sync():

    # Pretend that since last we checked, all task instances were queued.
    # 2 still in queued, one in launched, one expired
    ti1 = TaskInstance(status="Q")
    ti2 = TaskInstance(status="Q")
    ti3 = TaskInstance(status="O")
    ti4 = TaskInstance(status="D")

    distributor = DistributorInstance()
    for task_instance in (ti1, ti2, ti3, ti4):
        distributor._task_instances[task_instance.id] = task_instance
        distributor._task_instance_status_map["Q"].add(task_instance)

    distributor.refresh_status_from_db(status="Q")
    assert ti1 in distributor._task_instance_status_map["Q"]
    assert ti2 in distributor._task_instance_status_map["Q"]
    assert ti3 in distributor._task_instance_status_map["O"]
    # Tasks that are done should be completely expunged from memory
    assert ti4.id not in distributor._task_instances
    assert ti4 not in distributor._task_instance_status_map["Q"]


@pytest.mark.skip("TODO")
def test_task_instance_creation():

    ti1 = TaskInstance(status="Q")
    ti2 = TaskInstance(status="Q")

    distributor = DistributorInstance()

    distributor.refresh_status_from_db(status="Q")

    distributor_commands = list(distributor._distributor_commands)
    assert len(distributor_commands) == 1  # One batched add task instance calls

    command = distributor_commands.pop()
    command()

    assert len(distributor._task_instances) == 2


@pytest.mark.skip("TODO")
def test_batch_creation():

    batch1 = ServerBatch()
    batch2 = ServerBatch()

    # 3 task instances belonging to 2 batches
    ti1 = TaskInstance(batch_id=batch1.id, status="Q")
    ti2 = TaskInstance(batch_id=batch1.id, status="Q")
    ti3 = TaskInstance(batch_id=batch2.id, status="Q")

    distributor = DistributorInstance()
    distributor.refresh_status_from_db("Q")

    # Add them to distributor registry
    for command in distributor._distributor_commands:
        command()

    assert len(distributor._task_instances) == 3
    for ti in distributor._task_instances.values():
        # Batch metadata isn't loaded on initialization
        assert ti.batch is None

    # Fetch batch data
    batches = distributor._create_batches(distributor._task_instance_status_map["Q"])
    assert len(batches) == 2

    assert {batch.id for batch in batches} == {batch1.id, batch2.id}


@pytest.mark.skip("TODO")
def test_launched_task_instance_sync():
    """Check that launched/running task instances inherited by a prior DI are loaded gracefully."""

    instance1 = DistributorInstance()
    instance2 = DistributorInstance()

    instance1.register()
    instance2.register()

    batch1 = ServerBatch(distributor_instance_id=instance1.distributor_instance_id)
    batch2 = ServerBatch(distributor_instance_id=instance2.distributor_instance_id)

    ti1 = TaskInstance(batch_id=batch1.id, status="O", workflow_run_id=1)
    # A task instance with a new heartbeat
    ti2 = TaskInstance(batch_id=batch2.id, status="O", report_by_date=func.now() + 100, workflow_run_id=1)
    # A task instance that's missed a heartbeat, and moved to triaging by its swarm
    ti3 = TaskInstance(batch_id=batch2.id, status="T", report_by_date=func.now() - 100, workflow_run_id=1)

    instance1.refresh_status_from_db("O")
    [command() for command in instance1._distributor_commands]

    assert ti1.id in instance1._task_instances
    assert len(instance1._task_instance_status_map["O"]) == 2
    assert len(instance1._task_instance_status_map["T"]) == 1
    
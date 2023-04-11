from jobmon.distributor.distributor_instance import DistributorInstance as ClientDistributor
from jobmon.server.web.models.api import (
    Batch as ServerBatch,
    TaskInstance,
)

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session


def test_batch_creation(
    requester_in_memory, distributor_crud, initialize_distributor, db_engine
):
    database_ids = distributor_crud()
    distributor = initialize_distributor(database_ids['distributor_ids'][0])

    task_instance_ids = database_ids['task_instance_ids'][:3]

    assert task_instance_ids[0] in distributor._task_instances
    assert task_instance_ids[1] in distributor._task_instances
    assert task_instance_ids[2] in distributor._task_instances

    for ti in distributor._task_instances.values():
        # Batch metadata isn't loaded on initialization
        assert ti.batch is None

    # Fetch batch data
    batches = distributor._create_batches(distributor._task_instance_status_map["Q"])
    assert len(batches) == 2
    assert {batch.batch_id for batch in batches} == set(database_ids['batch_ids'][:2])


def test_status_change_sync(
    db_engine, requester_in_memory, distributor_crud, initialize_distributor
):
    database_ids = distributor_crud()
    # Pretend that since last we checked, all task instances were queued.
    # 1 still in queued, one launched, one expired
    distributor = initialize_distributor(database_ids['distributor_ids'][0])
    task_instance_ids = database_ids['task_instance_ids'][:3]

    task_instances = [distributor._task_instances[tid] for tid in task_instance_ids]

    with Session(bind=db_engine) as session:
        session.execute(
            update(TaskInstance).where(TaskInstance.id == task_instance_ids[1])
            .values(status="O")
        )
        session.execute(
            update(TaskInstance).where(TaskInstance.id == task_instance_ids[2])
            .values(status="D")
        )
        session.commit()

    distributor.refresh_status_from_db(status="Q")
    assert task_instances[0] in distributor._task_instance_status_map["Q"]
    assert task_instances[1] in distributor._task_instance_status_map["O"]
    # Tasks that are done should be completely expunged from memory
    assert task_instance_ids[2] not in distributor._task_instances
    assert task_instances[2] not in distributor._task_instance_status_map["Q"]


def test_distributor_reassignment(
    db_engine, requester_in_memory, distributor_crud, initialize_distributor
):
    """Check that launched/running task instances inherited by a prior DI
    are loaded gracefully."""

    database_ids = distributor_crud()

    distributor_2 = initialize_distributor(database_ids['distributor_ids'][1])

    # Distributor 2 shouldn't be assigned anything at start
    assert len(distributor_2._task_instances) == 0

    with Session(bind=db_engine) as session:
        # Reassign distributor 1 ids to 2
        # In normal operations, this is done by the swarm.
        batch_ids = database_ids['batch_ids']

        session.execute(
            update(ServerBatch)
            .values(distributor_instance_id=database_ids['distributor_ids'][1])
            .where(ServerBatch.id.in_(batch_ids))
        )
        session.commit()

    # Synchronize state and ensure the task instances are picked up
    distributor_2.refresh_status_from_db(status="Q")
    command = next(distributor_2._distributor_commands)
    command()

    assert len(distributor_2._task_instances) == 3
    assert len(distributor_2._task_instance_status_map["Q"]) == 3
    
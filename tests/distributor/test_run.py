import datetime
import time

from sqlalchemy import select
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskInstanceStatus
from jobmon.distributor.distributor_instance import DistributorInstance
from jobmon.server.web.models.api import TaskInstance, DistributorInstance as ServerDistributor


def test_distributor_end_to_end(db_engine, distributor_crud, initialize_distributor,
                                requester_no_retry, requester_in_memory):
    database_ids = distributor_crud()
    distributor = initialize_distributor(database_ids['distributor_ids'][0])
    # The distributor should be able to move all registered tasks through to Done state
    # Mock the keep_running method. Works for this particular unit tests since no active
    # swarm exists to create additional task instances
    start = time.time()
    distributor.keep_running = lambda: len(distributor._task_instances) > 0 and \
        time.time() - start < 60  # Set a fail-safe timeout to prevent infinite looping

    distributor.run()
    with Session(bind=db_engine) as session:
        task_instance_statuses = session.execute(
            select(TaskInstance.status)
            .where(TaskInstance.id.in_(database_ids['task_instance_ids']))
        ).scalars().all()

        assert task_instance_statuses == [TaskInstanceStatus.DONE] * 3

    assert len(distributor._task_instances) == 0
    assert sum([len(tasks) for _, tasks in distributor._task_instance_status_map.items()]) == 0


def test_distributor_registration(db_engine, client_env):
    """Test that the CLI logic implemented correctly creates a distributor instance."""
    instance = DistributorInstance(cluster_name='sequential')
    instance.register()
    assert isinstance(instance.distributor_instance_id, int)

    with Session(bind=db_engine) as session:
        server_instance = session.execute(
            select(ServerDistributor)
            .where(ServerDistributor.id == instance.distributor_instance_id)
        ).scalar()
        assert server_instance.workflow_run_id is None
        assert server_instance.report_by_date > datetime.datetime.now()

    # Test binding workflow-local instance
    local_instance = DistributorInstance(cluster_name='sequential', workflow_run_id=1)
    local_instance.register()

    with Session(bind=db_engine) as session:
        local_server_instance = session.execute(
            select(ServerDistributor)
            .where(ServerDistributor.id == local_instance.distributor_instance_id)
        ).scalar()

        assert local_server_instance.workflow_run_id == 1
        assert local_server_instance.report_by_date > datetime.datetime.now()

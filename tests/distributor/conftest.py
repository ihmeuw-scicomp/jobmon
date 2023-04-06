import pytest
from sqlalchemy.orm import Session

from jobmon.distributor.distributor_instance import DistributorInstance as ClientDistributor
from jobmon.core.constants import TaskInstanceStatus, TaskStatus
from jobmon.server.web.models.api import (
    Array,
    Batch as ServerBatch,
    DistributorInstance as ServerDistributorInstance,
    Task,
    TaskInstance,
    TaskResources,
    WorkflowRun,

)


@pytest.fixture
def distributor_crud(db_engine):
    """Note that this fixture uses the default pytest scope, which is 'function'.

    Every unit test that utilizes this fixture or the initialized_distributor fixture below
    will re-insert a new set of objects. This might be potentially time consuming but is the
    only way to ensure consistent state when using multiprocessing. Else, test A may  modify
    state that causes an expectation in test B to fail.

    """
    def _crud(task_instance_status=TaskInstanceStatus.QUEUED):
        with Session(bind=db_engine) as session:

            server_distributor = ServerDistributorInstance()
            distributor_2 = ServerDistributorInstance()  # Distributor not assigned anything

            wfr1 = WorkflowRun(status="R", workflow_id=-1)
            wfr2 = WorkflowRun(status="R", workflow_id=-1)

            session.add_all([wfr1, wfr2, server_distributor, distributor_2])
            session.flush()

            array = Array(name="distributor_array")
            session.add(array)
            session.flush()

            # Add task associations
            t1 = Task(status=TaskStatus.QUEUED, command='echo 1', name='t1', workflow_id=-1)
            t2 = Task(status=TaskStatus.QUEUED, command='echo 2', name='t2', workflow_id=-1)
            t3 = Task(status=TaskStatus.QUEUED, command='echo 3', name='t3', workflow_id=-1)
            session.add_all([t1, t2, t3])
            session.flush()

            # Add a task resources object - default to dummy null.q
            task_resources = TaskResources(
                queue_id=1,
                task_resources_type_id="O",
                requested_resources='{"foo": "bar"}'
            )
            session.add(task_resources)
            session.flush()

            batch1 = ServerBatch(
                distributor_instance_id=server_distributor.id,
                workflow_run_id=wfr1.id,
                cluster_id=1,  # Known id of 1 from init_db method corresponding to dummy cluster,
                array_id=array.id,
                task_resources_id=task_resources.id,
            )
            batch2 = ServerBatch(
                distributor_instance_id=server_distributor.id,
                workflow_run_id=wfr2.id,
                cluster_id=1,
                array_id=array.id,
                task_resources_id=task_resources.id,
            )
            session.add_all([batch1, batch2])
            session.flush()

            # 2 tis in batch1, 1 in batch2. shouldn't materialize batch2 task instances ever
            ti1 = TaskInstance(
                workflow_run_id=wfr1.id, batch_id=batch1.id,
                status=task_instance_status, task_id=t1.id,
            )
            ti2 = TaskInstance(
                workflow_run_id=wfr1.id, batch_id=batch1.id,
                status=task_instance_status, task_id=t2.id,
            )
            ti3 = TaskInstance(
                workflow_run_id=wfr2.id, batch_id=batch2.id,
                status=task_instance_status, task_id=t3.id,
            )
            session.add_all([ti1, ti2, ti3])
            session.flush()

            task_instance_ids = [ti1.id, ti2.id, ti3.id]

            session.commit()

            # Return the relevant IDs
            return_dict = {
                'task_instance_ids': task_instance_ids,
                'task_ids': [t1.id, t2.id, t3.id],
                'array_ids': [array.id],
                'distributor_ids': [server_distributor.id, distributor_2.id],
                'batch_ids': [batch1.id, batch2.id],
                'workflow_run_ids': [wfr1.id, wfr2.id],
                'task_resources_ids': [task_resources.id],
            }

        return return_dict

    return _crud


@pytest.fixture
def initialize_distributor(requester_no_retry):

    def _initialize(distributor_id):
        # Initialize a distributor object

        distributor = ClientDistributor(cluster_name='dummy', requester=requester_no_retry,
                                        raise_on_error=True)
        distributor._distributor_instance_id = distributor_id

        distributor.refresh_status_from_db(status="Q")

        # Should have appended a "make task instances" function to the distributor command chain
        try:
            command = next(distributor._distributor_commands)
            command()
        except StopIteration:
            pass
        return distributor

    return _initialize

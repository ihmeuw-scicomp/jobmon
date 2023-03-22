import pytest
from sqlalchemy.orm import Session

from jobmon.server.web.models.api import (
    Batch as ServerBatch,
    DistributorInstance as ServerDistributorInstance,
    TaskInstance,
    WorkflowRun,

)


@pytest.fixture(scope='module')
def distributor_crud(db_engine):
    with Session(bind=db_engine) as session:

        # This is the current idea for a reusable test fixture.
        # Unit tests that need additional edge cases can add CRUD to this method or
        # add their own objects for reference

        server_distributor = ServerDistributorInstance()
        distributor_2 = ServerDistributorInstance()  # Distributor not assigned anything

        wfr1 = WorkflowRun(status="R")  # Active
        wfr2 = WorkflowRun(status="E")  # Inactive

        session.add_all([wfr1, wfr2, server_distributor, distributor_2])
        session.flush()
        batch1 = ServerBatch(
            distributor_instance_id=server_distributor.id,
            workflow_run_id=wfr1.id,
            cluster_id=1,  # Known id of 1 from init_db method corresponding to dummy cluster,
            total_task_instances=1
        )
        batch2 = ServerBatch(
            distributor_instance_id=server_distributor.id,
            workflow_run_id=wfr2.id,
            cluster_id=1,
            total_task_instances=1,
        )
        session.add_all([batch1, batch2])
        session.flush()

        # 2 tis in batch1, 1 in batch2. shouldn't materialize batch2 task instances ever
        ti1 = TaskInstance(workflow_run_id=wfr1.id, batch_id=batch1.id, status="Q")
        ti2 = TaskInstance(workflow_run_id=wfr1.id, batch_id=batch1.id, status="Q")
        ti3 = TaskInstance(workflow_run_id=wfr1.id, batch_id=batch2.id, status="Q")
        session.add_all([ti1, ti2, ti3])
        session.flush()

        task_instance_ids = [ti1.id, ti2.id, ti3.id]

        session.commit()

        # Return the relevant IDs
        return_dict = {
            'task_instance_ids': task_instance_ids,
            'distributor_ids': [server_distributor.id, distributor_2.id],
            'batch_ids': [batch1.id, batch2.id],
            'workflow_run_ids': [wfr1.id, wfr2.id],
        }

    yield return_dict

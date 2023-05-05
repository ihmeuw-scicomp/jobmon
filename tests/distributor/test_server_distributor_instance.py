from sqlalchemy import select
from sqlalchemy.orm import Session

from jobmon.server.web.models.api import DistributorInstance


def test_distributor_registration(requester_in_memory, requester_no_retry, db_engine):

    # Register a distributor instance
    rc, resp = requester_no_retry.send_request(
        app_route="/distributor_instance/register",
        message={
            'cluster_id': 1,
            'next_report_increment': 100,
        },
        request_type='post'
    )

    assert rc == 200
    distributor_instance_id = resp['distributor_instance_id']

    # Check that workflowrun attribute is None
    with Session(bind=db_engine) as session:
        instance = session.execute(
            select(DistributorInstance)
            .where(DistributorInstance.id == distributor_instance_id)
        ).one()
        assert instance.workflow_run_id is None


def test_heartbeat(requester_in_memory, requester_no_retry, db_engine):
    # Register a distributor instance that fails to log a timely heartbeat
    _, resp = requester_no_retry.send_request(
        app_route="/distributor_instance/register",
        message={
            'cluster_id': 1,
            'next_report_increment': 0,
        },
        request_type='post'
    )
    distributor_instance_id = resp['distributor_instance_id']

    # Check that the expunge route cleans up this distributor
    requester_no_retry.send_request(
        app_route="/distributor_instance/expunge",
        message={'cluster_id': 1},
        request_type="put"
    )

    with Session(bind=db_engine) as session:
        distributor = session.execute(
            select(DistributorInstance).where(DistributorInstance.id == distributor_instance_id)
        ).scalar()
        assert distributor.expunged


def test_distributor_instance_selection(requester_in_memory, requester_no_retry):
    """Check that we randomly select an appropriate distributor instance."""

    def register_instance(cluster_id, report_by=100):
        _, resp = requester_no_retry.send_request(
            app_route="/distributor_instance/register",
            message={
                'cluster_id': cluster_id,
                'next_report_increment': report_by,
            },
            request_type='post'
        )
        return resp['distributor_instance_id']

    distributor_instance_args = [
        # 2 cluster ids, id 1 has an expunged instance
        (100,),
        (100,),
        (100, 0),
        (101,),
        (101,)
    ]

    distributor_instance_ids = [register_instance(*args) for args in distributor_instance_args]
    requester_no_retry.send_request(
        app_route="/distributor_instance/expunge",
        message={'cluster_id': 100},
        request_type="put"
    )

    # Looking for instances belonging to cluster 100 should
    # always return one of instance 1 or 2
    for _ in range(4):
        _, resp = requester_no_retry.send_request(
            "/distributor_instance/100/get_active_distributor_instance_id",
            {},
            "get"
        )
        assert resp['distributor_instance_id'] in distributor_instance_ids[:2]

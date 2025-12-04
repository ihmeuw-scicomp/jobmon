from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.core.requester import Requester
from jobmon.core.serializers import SerializeCluster, SerializeQueue


def test_cluster_queue(db_engine, client_env):
    """test cluster_type, cluster and queue structure"""
    requester = Requester(client_env)

    # now set everything to error fail
    with Session(bind=db_engine) as session:
        # fake a new cluster_type zzzzCLUSTER_TYPE
        session.execute(
            text(
                """
                INSERT INTO cluster_type (name)
                VALUES ('{n}')""".format(
                    n="zzzzCLUSTER_TYPE"
                )
            )
        )
        session.commit()
        # fake 3 new cluster zzzzCLUSTER1, zzzzCLUSTER2, and zzzz我的新集群3 under zzzzCLUSTER_TYPE;
        # and to test out Unicode charset briefly.
        session.execute(
            text(
                """
                INSERT INTO cluster (name, cluster_type_id)
                SELECT '{n}', id
                FROM cluster_type
                WHERE name = '{ct_name}'""".format(
                    n="zzzzCLUSTER1", ct_name="zzzzCLUSTER_TYPE"
                )
            )
        )
        session.execute(
            text(
                """
                INSERT INTO cluster (name, cluster_type_id)
                SELECT '{n}', id
                FROM cluster_type
                WHERE name = '{ct_name}'""".format(
                    n="zzzzCLUSTER2", ct_name="zzzzCLUSTER_TYPE"
                )
            )
        )
        session.execute(
            text(
                """
                INSERT INTO cluster (name, cluster_type_id)
                SELECT '{n}', id
                FROM cluster_type
                WHERE name = '{ct_name}'""".format(
                    n="zzzz我的新集群3", ct_name="zzzzCLUSTER_TYPE"
                )
            )
        )
        session.commit()

        # fake 2 new queues for zzzzCluster2
        session.execute(
            text(
                """
                INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
                SELECT 'all.q', c.id, "{{'cust': 'param 1'}}" AS `parameters`
                FROM cluster c
                WHERE c.name = '{n}'""".format(
                    n="zzzzCLUSTER2"
                )
            )
        )
        session.execute(
            text(
                """
                INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
                SELECT 'long.q', c.id, "{{'cust': 'param 2'}}" AS `parameters`
                FROM cluster c
                WHERE c.name = '{n}'""".format(
                    n="zzzzCLUSTER2"
                )
            )
        )
        session.commit()

    # make sure that a single pull of one of the 3 clusters logged above gets 1 record back.
    rc, response = requester.send_request(
        app_route="/cluster/zzzzCLUSTER2", message={}, request_type="get"
    )
    cluster2 = SerializeCluster.kwargs_from_wire(response["cluster"])
    assert cluster2["cluster_type_name"] == "zzzzCLUSTER_TYPE"

    # make sure that a single pull of one of the 2 queues logged above gets 1 record back.
    rc, response = requester.send_request(
        app_route=f'/cluster/{cluster2["id"]}/queue/all.q',
        message={},
        request_type="get",
    )
    all_q = SerializeQueue.kwargs_from_wire(response["queue"])
    assert all_q["parameters"]["cust"] == "param 1"

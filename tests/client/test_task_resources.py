from contextlib import nullcontext
import pytest
from sqlalchemy.orm import Session

from jobmon.client.task_resources import TaskResources


def test_task_resources_hash(client_env):
    class MockQueue:
        queue_name: str = "mock-queue"

    # keys purposefully out of order
    resources_dict = {"d": "string datatype", "b": 123, "a": ["list", "data", "type"]}

    # same values, different representation order. Should not affect object equality.
    resource_dict_sorted = {key: resources_dict[key] for key in sorted(resources_dict)}

    # resources 1 and 2 should be equal, 3 should be different
    resource_1 = resources_dict
    resource_2 = resource_dict_sorted
    resource_3 = dict(resources_dict, b=100)

    tr1 = TaskResources(requested_resources=resource_1, queue=MockQueue())
    tr2 = TaskResources(requested_resources=resource_2, queue=MockQueue())
    tr1_clone = TaskResources(resource_1, queue=MockQueue())

    tr3 = TaskResources(resource_3, MockQueue())

    assert tr1 == tr1_clone
    assert tr1 == tr2
    assert tr1 != tr3

    assert len({tr1, tr2, tr3}) == 2

    # Equality instance check - should be false if other is not a ConcreteResource object
    class FakeResource:
        def __hash__(self):
            return hash(resource_1)

    assert not resource_1 == FakeResource()


def test_task_resource_bind(db_engine, tool, task_template):
    resources = {"queue": "null.q"}
    task_template.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources=resources
    )

    t1 = task_template.create_task(cluster_name="sequential", arg="echo 1")
    t2 = task_template.create_task(cluster_name="sequential", arg="echo 2")
    t3 = task_template.create_task(arg="echo 3")

    wf = tool.create_workflow()
    wf.add_tasks([t1, t2, t3])

    wf.bind()
    wf._bind_tasks()

    with Session(bind=db_engine) as session:
        q = f"""
        SELECT DISTINCT tr.id
        FROM task t
        JOIN task_resources tr ON tr.id = t.task_resources_id
        WHERE t.id IN {tuple(set([t.task_id for t in [t1, t2, t3]]))}
        """
        res = session.execute(q).fetchall()
        session.commit()
        assert len(res) == 1

    tr1, tr2, tr3 = [t.original_task_resources for t in wf.tasks.values()]
    assert tr1 is tr2
    assert tr1 is tr3
    assert tr1.id == res[0].id


def test_defaults_pass_down_and_overrides(tool, task_template):
    # test resource_scales == {runtime: 0.5, memory: 0.5} for unspecified
    resources = {"queue": "null.q"}
    task_template.set_default_compute_resources_from_dict(
        cluster_name="dummy", compute_resources=resources
    )
    t = task_template.create_task(cluster_name="dummy", arg="echo 1")
    wf = tool.create_workflow()
    wf.add_tasks([t])
    assert t.resource_scales["runtime"] == 0.5
    assert t.resource_scales["memory"] == 0.5

    # test from multiple clusters with resources/scales sets, and select a single one.
    resources = {"queue": "null.q", "memory": 34, "runtime": 56}
    scales = {"runtime": 0.7, "cores": 0.6, "memory": 0.8}
    task_template.set_default_compute_resources_from_dict(
        cluster_name="sequential",
        compute_resources=resources,
    )
    task_template.set_default_resource_scales_from_dict(
        cluster_name="sequential",
        resource_scales=scales,
    )

    resources_m = {"queue": "null.q", "memory": 9999, "runtime": 9999}
    scales_m = {"runtime": 0.9999, "cores": 0.9999, "memory": 0.9999}
    task_template.set_default_compute_resources_from_dict(
        cluster_name="multiprocess", compute_resources=resources_m
    )
    task_template.set_default_resource_scales_from_dict(
        cluster_name="multiprocess", resource_scales=scales_m
    )

    # later default setting will take precedence.
    assert task_template.default_cluster_name == "multiprocess"

    # pointing only to the sequential set
    t1 = task_template.create_task(cluster_name="sequential", arg="echo 1")
    t2 = task_template.create_task(
        cluster_name="sequential",
        arg="echo 2",
        compute_resources={"queue": "null.q", "memory": 22, "runtime": 222},
        resource_scales={"runtime": 0.2, "cores": 0.22, "memory": 0.222},
    )
    wf = tool.create_workflow()
    wf.add_tasks([t1, t2])

    assert t1.cluster_name == "sequential"
    assert t2.cluster_name == "sequential"

    assert t1.compute_resources["memory"] == 34
    assert t1.compute_resources["runtime"] == 56
    assert t1.resource_scales["runtime"] == 0.7
    assert t1.resource_scales["cores"] == 0.6
    assert t1.resource_scales["memory"] == 0.8

    assert t2.compute_resources["memory"] == 22
    assert t2.compute_resources["runtime"] == 222
    assert t2.resource_scales["runtime"] == 0.2
    assert t2.resource_scales["cores"] == 0.22
    assert t2.resource_scales["memory"] == 0.222


@pytest.mark.parametrize(
    "time_str,expected,exception",
    [
        ("24:30:10", 24 * 3600 + 30 * 60 + 10, nullcontext()),
        ("10:00:00", 10 * 3600, nullcontext()),
        ("1h", 1 * 3600, nullcontext()),
        ("30m", 30 * 60, nullcontext()),
        ("25s", 25, nullcontext()),
        (30, 30, nullcontext()),
        ("10h30m", None, pytest.raises(ValueError)),
    ],
)
def test_timeunit_convert(time_str, expected, exception):
    with exception:
        assert TaskResources.convert_runtime_to_s(time_str) == expected

from sqlalchemy.orm import Session
from sqlalchemy import select, update
import getpass
import pandas as pd

from jobmon.client.api import Tool
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import (
    MaxConcurrentlyRunning,
    TaskStatus,
    WorkflowRunStatus,
    WorkflowStatus,
)
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow

load_model()


def test_get_task_template_version(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )
    tt2 = t.get_task_template(
        template_name="tt2",
        command_template="echo {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task_1 = tt1.create_task(arg=1)
    task_2 = tt1.create_task(arg=2)
    task_3 = tt2.create_task(arg=3)
    wf.add_tasks([task_1, task_2, task_3])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # Test getting task template for task
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"task_id": task_1.task_id}, request_type="get"
    )
    # msg = {'task_template_version_ids': [{'id': 1, 'name': 'bash_task'}]}
    assert len(msg) == 1
    assert "task_template_version_ids" in msg.keys()
    assert len(msg["task_template_version_ids"]) == 1
    assert "id" in msg["task_template_version_ids"][0].keys()
    assert msg["task_template_version_ids"][0]["name"] == "tt1"

    # Test getting task template for workflow
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"workflow_id": wf.workflow_id}, request_type="get"
    )
    # msg = {'task_template_version_ids': [{'id': 1, 'name': 'tt1'}, {'id': 2, 'name': 'tt2'}]}
    assert len(msg) == 1
    assert "task_template_version_ids" in msg.keys()
    assert len(msg["task_template_version_ids"]) == 2
    for i in msg["task_template_version_ids"]:
        if i["id"] == tt1._active_task_template_version.id:
            assert i["name"] == "tt1"
        else:
            assert i["name"] == "tt2"


def test_get_requested_cores(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    # Get task template for workflow
    app_route = "/get_task_template_version"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"workflow_id": wf.workflow_id}, request_type="get"
    )
    ttvis = msg["task_template_version_ids"][0]["id"]
    # Test getting requested cores
    app_route = "/get_requested_cores"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_template_version_ids": f"({ttvis})"},
        request_type="get",
    )
    # msg = {'core_info': [{'avg': 2, 'id': 1, 'max': 3, 'min': 1}]}
    assert len(msg["core_info"]) == 1
    assert msg["core_info"][0]["id"] == ttvis
    assert msg["core_info"][0]["min"] == 2
    assert msg["core_info"][0]["max"] == 4
    assert msg["core_info"][0]["avg"] == 3


def test_most_popular_queue(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_q_1", command_template="echo {arg}", node_args=["arg"]
    )
    tt2 = t.get_task_template(
        template_name="tt_q_2", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t2 = tt1.create_task(
        arg=2, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t3 = tt2.create_task(
        arg=3, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t4 = tt2.create_task(
        arg=4, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    t5 = tt2.create_task(
        arg=5, cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf.add_tasks([t1, t2, t3, t4, t5])
    wf.run()

    app_route = "/get_most_popular_queue"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={
            "task_template_version_ids": f"({tt1._active_task_template_version.id}, "
            f"{tt2._active_task_template_version.id})"
        },
        request_type="get",
    )
    assert len(msg["queue_info"]) == 2
    for i in msg["queue_info"]:
        assert i["queue"] == "null.q"


def test_get_workflow_validation_status(db_engine, tool):
    t = tool
    wf1 = t.create_workflow(name="i_am_a_fake_wf")
    wf2 = t.create_workflow(name="i_am_another_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf1.add_tasks([t1])
    wf1.bind()
    wf1._bind_tasks()
    wf2.add_tasks([t2])
    wf2.bind()
    wf2._bind_tasks()

    app_route = "/workflow_validation"
    return_code, msg = wf1.requester.send_request(
        app_route=app_route,
        message={"task_ids": [t1.task_id, t2.task_id]},
        request_type="post",
    )
    assert return_code == 200
    assert msg["validation"] is False


def test_get_workflow_tasks(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="yiyayiyayou")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._bind_tasks()

    app_route = f"/workflow/{wf.workflow_id}/workflow_tasks"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"limit": 5, "status": "PENDING"},
        request_type="get",
    )
    assert return_code == 200
    result = pd.read_json(msg["workflow_tasks"])
    assert len(result) == 2

    app_route = f"/workflow/{wf.workflow_id}/workflow_tasks"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"limit": 1, "status": "PENDING"},
        request_type="get",
    )
    assert return_code == 200
    result = pd.read_json(msg["workflow_tasks"])
    assert len(result) == 1


def test_get_workflow_user_validation(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    factory.create_workflow_run()

    app_route = f"/workflow/{wf.workflow_id}/validate_username/whoever"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert return_code == 200
    assert msg["validation"] is False

    app_route = f"/workflow/{wf.workflow_id}/validate_username/{getpass.getuser()}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert return_code == 200
    assert msg["validation"] is True


def test_get_workflow_run_for_workflow_reset(db_engine, tool, web_server_in_memory):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    factory.create_workflow_run()

    app_route = f"/workflow/{wf.workflow_id}/validate_for_workflow_reset/whoever"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert return_code == 200
    assert msg["workflow_run_id"] is None


def test_reset_workflow(db_engine, tool, web_server_in_memory):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run(workflow_run_heartbeat_interval=0)
    wfr._update_status(WorkflowRunStatus.BOUND)
    wfr._update_status(WorkflowRunStatus.COLD_RESUME)

    # Transition task1 to done
    with Session(bind=db_engine) as session:
        update_query = update(Task).where(Task.id == t1.task_id).values(status="D")
        session.execute(update_query)
        session.commit()

    app_route = f"/workflow/{wf.workflow_id}/reset"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"partial_reset": True}, request_type="put"
    )
    assert return_code == 200

    with Session(bind=db_engine) as session:
        wf_status = (
            session.execute(
                select(Workflow.status).where(Workflow.id == wf.workflow_id)
            )
            .scalars()
            .all()
        )
        assert set(wf_status) == {WorkflowStatus.REGISTERING}

        task_statuses = (
            session.execute(
                select(Task.status).where(Task.workflow_id == wf.workflow_id)
            )
            .scalars()
            .all()
        )
        # With a partial reset, the done task should remain in that state
        assert set(task_statuses) == {TaskStatus.REGISTERING, TaskStatus.DONE}

    # Check that the workflow is resumable
    return_code, response = wf.requester.send_request(
        app_route=f"/workflow/{wf.workflow_id}/is_resumable",
        message={},
        request_type="get",
    )

    workflow_is_resumable = bool(response.get("workflow_is_resumable"))
    assert workflow_is_resumable

    # Signal for a full reset
    _ = wf.requester.send_request(
        app_route=app_route, message={"partial_reset": False}, request_type="put"
    )

    with Session(bind=db_engine) as session:
        task_statuses = (
            session.execute(
                select(Task.status).where(Task.workflow_id == wf.workflow_id)
            )
            .scalars()
            .all()
        )
        # With a full reset, all tasks should be registering
        assert set(task_statuses) == {TaskStatus.REGISTERING}


def test_get_workflow_status(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.bind()
    wf._bind_tasks()

    factory = WorkflowRunFactory(wf.workflow_id)
    factory.create_workflow_run()

    app_route = f"/workflow_status"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"workflow_id": [wf.workflow_id]},
        request_type="get",
    )
    assert return_code == 200
    result = pd.read_json(msg["workflows"])
    assert len(result) == 1

    # Create a second workflow, check that ordering returns second one correctly
    wf2 = t.create_workflow(name="fake_workflow_2")
    wf2.add_task(t1)
    wf2.bind()
    wf2._bind_tasks()
    factory2 = WorkflowRunFactory(wf2.workflow_id)
    factory2.create_workflow_run()

    _, msg2 = wf.requester.send_request(
        app_route=app_route,
        message={"user": getpass.getuser(), "limit": 1},
        request_type="get",
    )
    result2 = pd.read_json(msg2["workflows"])
    assert len(result2) == 1
    assert result2.WF_ID[0] == wf2.workflow_id


def test_get_task_status(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.run()

    app_route = f"/task_status"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_ids": [t1.task_id, t2.task_id]},
        request_type="get",
    )
    assert return_code == 200
    result = pd.read_json(msg["task_instance_status"])
    assert len(result) == 2
    assert result["task_status"][0] == result["task_status"][1] == "D"
    assert result["STATUS"][0] == result["STATUS"][1] == "DONE"


def test_get_array_task_instances(db_engine, tool):
    tt = tool.get_task_template(
        template_name="dummy_template",
        command_template="echo {arg1} {arg2}",
        node_args=["arg1", "arg2"],
        task_args=[],
        op_args=[],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )
    tasks = tt.create_tasks(
        arg1=[1, 2], arg2=[3, 4], compute_resources={"queue": "null.q"}
    )
    array = tasks[0].array
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.run()

    with Session(bind=db_engine) as session:
        query = """UPDATE task_instance
                    SET stdout="/cool/filepath.o",
                    stderr="/cool/filepath.e"
                """
        session.execute(query)
        session.commit()
    app_route = f"/array/{wf.workflow_id}/get_array_tasks"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"array_name": array.name}, request_type="get"
    )
    assert return_code == 200
    assert len(msg["array_tasks"]) == 4
    assert msg["array_tasks"][0]["ERROR_PATH"] == "/cool/filepath.e"
    assert msg["array_tasks"][0]["OUTPUT_PATH"] == "/cool/filepath.o"


def test_get_task_template_resource_usage(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_template_resource",
        command_template="echo {arg}",
        node_args=["arg"],
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    wf.add_tasks([t1, t2])
    wf.run()

    # two rows
    app_route = f"/task_template_resource_usage"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"task_template_version_id": tt1.active_task_template_version.id},
        request_type="post",
    )
    assert return_code == 200
    assert msg[0] == 2

    # two rows
    app_route = f"/task_template_resource_usage"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={
            "task_template_version_id": tt1.active_task_template_version.id,
            "workflows": [wf.workflow_id],
        },
        request_type="post",
    )
    assert return_code == 200
    assert msg[0] == 2

    # two rows
    app_route = f"/task_template_resource_usage"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={
            "task_template_version_id": tt1.active_task_template_version.id,
            "node_args": {"arg": ["1", "2"]},
        },
        request_type="post",
    )
    assert return_code == 200
    assert msg[0] == 2

    # one row
    app_route = f"/task_template_resource_usage"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={
            "task_template_version_id": tt1.active_task_template_version.id,
            "node_args": {"arg": ["1"]},
        },
        request_type="post",
    )
    assert return_code == 200
    assert msg[0] == 1

    # 0 row
    app_route = f"/task_template_resource_usage"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={
            "task_template_version_id": tt1.active_task_template_version.id,
            "node_args": {"arg": ["3"]},
        },
        request_type="post",
    )
    assert return_code == 200
    assert msg[0] is None


def test_node_dependencies(tool):
    t = tool
    wf_1 = tool.create_workflow(name="some_random_workflow_1")
    tt1 = t.get_task_template(
        template_name="random_1", command_template="echo {arg}", node_args=["arg"]
    )
    tt2 = t.get_task_template(
        template_name="random_2", command_template="sleep {arg}", node_args=["arg"]
    )
    tt3 = t.get_task_template(
        template_name="random_3", command_template="echo hello {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg="hello world",
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    t2 = tt2.create_task(
        arg=5,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
        upstream_tasks=[t1],
    )
    t3 = tt3.create_task(
        arg="random",
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
        upstream_tasks=[t2],
    )
    t4 = tt3.create_task(
        arg="random_2",
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
        upstream_tasks=[t2],
    )
    wf_1.add_tasks([t1, t2, t3, t4])
    wf_1.bind()
    wf_1._bind_tasks()
    app_route = f"/task_dependencies/{t2.task_id}"
    return_code, msg = wf_1.requester.send_request(
        app_route=app_route,
        message={},
        request_type="get",
    )
    assert return_code == 200
    assert msg["down"][0][0]["id"] == t3.task_id
    assert msg["down"][1][0]["id"] == t4.task_id
    assert msg["up"][0][0]["id"] == t1.task_id


def test_get_workflow_status_viz(tool):
    t = tool
    wfids = []
    for i in [1, 2]:
        wf = t.create_workflow(name=f"i_am_a_fake_wf_{i}")
        tt1 = t.get_task_template(
            template_name="tt_core", command_template="echo {arg}", node_args=["arg"]
        )
        t1 = tt1.create_task(
            arg=1,
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 2},
        )
        t2 = tt1.create_task(
            arg=2,
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 4},
        )
        wf.add_tasks([t1, t2])
        wf.bind()
        wf._bind_tasks()
        wfids.append(wf.workflow_id)

    app_route = "/workflow_status_viz"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"workflow_ids[]": wfids}, request_type="get"
    )
    assert return_code == 200

    for wfid in wfids:
        assert str(wfid) in msg.keys()
        assert msg[str(wfid)]["tasks"] == 2
        assert msg[str(wfid)]["PENDING"] == 2
        assert msg[str(wfid)]["RUNNING"] == 0
        assert msg[str(wfid)]["FATAL"] == 0
        assert msg[str(wfid)]["DONE"] == 0
        assert msg[str(wfid)]["MAXC"] == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING


def test_get_workflow_tt_status_viz(client_env, db_engine):
    """This test case covers:
    1. When the Array entry of wf is empty (older wfs)
    2. When there are more than one wf using the same tt
    3. When the wf contains multiple tt
    4. When a tt in a wf contains tasks in more than one status
    """
    t = Tool(name="gui_tt_progress_test")
    wf = t.create_workflow(name=f"i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_1", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    t3 = tt1.create_task(
        arg=3,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 4},
    )
    tt2 = t.get_task_template(
        template_name="tt_2", command_template="{arg}", node_args=["arg"]
    )
    t4 = tt2.create_task(
        arg="pwd",
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    wf.add_tasks([t1, t2, t3, t4])
    wf.bind()
    wf._bind_tasks()
    # set one task to F to test TT with more than one task status
    with Session(bind=db_engine) as session:
        session.execute(
            f"""
            UPDATE task
            SET status="F"
            WHERE id={t2.task_id}
            """
        )
        session.commit()
    app_route = f"/workflow_tt_status_viz/{wf.workflow_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert msg[str(tt1._task_template_id)]["tasks"] == 3
    assert msg[str(tt1._task_template_id)]["PENDING"] == 2
    assert msg[str(tt1._task_template_id)]["DONE"] == 0
    assert msg[str(tt1._task_template_id)]["FATAL"] == 1
    assert msg[str(tt1._task_template_id)]["RUNNING"] == 0
    assert (
        msg[str(tt1._task_template_id)]["MAXC"]
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    assert msg[str(tt1._task_template_id)]["name"] == "tt_1"

    assert msg[str(tt2._task_template_id)]["tasks"] == 1
    assert msg[str(tt2._task_template_id)]["PENDING"] == 1
    assert msg[str(tt2._task_template_id)]["DONE"] == 0
    assert msg[str(tt2._task_template_id)]["FATAL"] == 0
    assert msg[str(tt2._task_template_id)]["RUNNING"] == 0
    assert (
        msg[str(tt1._task_template_id)]["MAXC"]
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    assert msg[str(tt2._task_template_id)]["name"] == "tt_2"

    # test two wf with same tt
    wf2 = t.create_workflow(name=f"i_am_another_fake_wf")
    t5 = tt1.create_task(
        arg=5,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    wf2.add_tasks([t5])
    wf2.bind()
    wf2._bind_tasks()
    app_route = f"/workflow_tt_status_viz/{wf2.workflow_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert msg[str(tt1._task_template_id)]["tasks"] == 1
    assert msg[str(tt1._task_template_id)]["PENDING"] == 1
    assert msg[str(tt1._task_template_id)]["DONE"] == 0
    assert msg[str(tt1._task_template_id)]["FATAL"] == 0
    assert msg[str(tt1._task_template_id)]["RUNNING"] == 0
    assert (
        msg[str(tt1._task_template_id)]["MAXC"]
        == MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING
    )
    assert msg[str(tt1._task_template_id)]["name"] == "tt_1"

    # test 3.0 records
    with Session(bind=db_engine) as session:
        session.execute(
            """
            DELETE FROM array
            """
        )
        session.commit()
    app_route = f"/workflow_tt_status_viz/{wf.workflow_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert msg[str(tt1._task_template_id)]["tasks"] == 3
    assert msg[str(tt1._task_template_id)]["PENDING"] == 2
    assert msg[str(tt1._task_template_id)]["DONE"] == 0
    assert msg[str(tt1._task_template_id)]["FATAL"] == 1
    assert msg[str(tt1._task_template_id)]["RUNNING"] == 0
    assert msg[str(tt1._task_template_id)]["MAXC"] == "NA"
    assert msg[str(tt1._task_template_id)]["name"] == "tt_1"

    assert msg[str(tt2._task_template_id)]["tasks"] == 1
    assert msg[str(tt2._task_template_id)]["PENDING"] == 1
    assert msg[str(tt2._task_template_id)]["DONE"] == 0
    assert msg[str(tt2._task_template_id)]["FATAL"] == 0
    assert msg[str(tt2._task_template_id)]["RUNNING"] == 0
    assert msg[str(tt1._task_template_id)]["MAXC"] == "NA"
    assert msg[str(tt2._task_template_id)]["name"] == "tt_2"


def test_get_tt_error_log_viz(client_env, db_engine):
    t = Tool(name="gui_tt_error_log")

    # test no error
    wf1 = t.create_workflow(name=f"i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt_1", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    wf1.add_tasks([t1])
    wf1.run()
    app_route = f"/tt_error_log_viz/{wf1.workflow_id}/{tt1.id}"
    return_code, msg = wf1.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert len(msg) == 0

    # test error
    wf2 = t.create_workflow(name=f"i_am_another_fake_wf")
    tt2 = t.get_task_template(
        template_name="tt_2", command_template="{arg}", node_args=["arg"]
    )
    t2 = tt2.create_task(
        arg="abc",
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
        max_attempts=1,
    )
    wf2.add_tasks([t2])
    wf2.run()
    app_route = f"/tt_error_log_viz/{wf2.workflow_id}/{tt2.id}"
    return_code, msg = wf1.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert len(msg) == 1
    assert msg[0]["task_id"] == t2.task_id
    assert "command not found" in msg[0]["error"]


def test_task_details_by_wf_id(client_env, db_engine):
    t = Tool(name="task_detail_tool")
    wfids = []
    wf = t.create_workflow(name="i_am_a_fake_wf_vv")
    tt1 = t.get_task_template(
        template_name="tt_test", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    wf.add_tasks([t1])
    wf.bind()
    wf._bind_tasks()
    wfids.append(wf.workflow_id)
    app_route = f"/task_table_viz/{wf.workflow_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"tt_name": "tt_test"}, request_type="get"
    )
    assert return_code == 200
    tasks = msg["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["task_command"] == "echo 1"
    assert tasks[0]["task_name"] == "tt_test_arg-1"
    assert tasks[0]["task_status"] == "PENDING"


def test_workflow_details_viz(client_env, db_engine):
    t = Tool(name="task_detail_tool")
    wf = t.create_workflow(name="i_am_another_fake_wf_vv")
    tt1 = t.get_task_template(
        template_name="tt_test", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    wf.add_tasks([t1])
    wf.bind()
    wf._bind_tasks()
    app_route = f"/workflow_details_viz/{wf.workflow_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert return_code == 200
    assert msg[0]["wf_status"] == "G"
    assert msg[0]["wf_status_desc"] == "Workflow is being validated."


def test_workflow_overview_viz(client_env, db_engine):
    tool_name = "task_detail_tool"
    t = Tool(name=tool_name)
    wf = t.create_workflow(
        name="another_fake_wf", workflow_attributes={"test_attribute": "test"}
    )
    tt1 = t.get_task_template(
        template_name="tt_test", command_template="echo {arg}", node_args=["arg"]
    )
    t1 = tt1.create_task(
        arg=1,
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    wf.add_tasks([t1])
    wf.run()
    app_route = f"/workflow_overview_viz"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"tool": "task_detail_tool", "attribute": "test"},
        request_type="get",
    )

    assert return_code == 200
    assert msg["workflows"][0]["wf_tool"] == tool_name

import getpass
import random
import string
from io import StringIO

import pandas as pd
import pytest
from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from jobmon.client.api import Tool
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import (
    MaxConcurrentlyRunning,
    TaskInstanceStatus,
    TaskStatus,
    WorkflowRunStatus,
    WorkflowStatus,
)
from jobmon.core.exceptions import InvalidRequest
from jobmon.server.web.models import load_model
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.workflow import Workflow

load_model()


def test_get_task_template_details_for_workflow(db_engine, tool):
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

    # Test fetching task template details for workflow (Valid case)
    app_route = "/get_task_template_details"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"workflow_id": wf.workflow_id, "task_template_id": tt1.id},
        request_type="get",
    )

    assert return_code == 200
    assert len(msg) == 3
    assert "task_template_id" in msg.keys()
    assert msg["task_template_id"] == tt1.id
    assert "task_template_name" in msg.keys()
    assert msg["task_template_name"] == "tt1"
    assert "task_template_version_id" in msg.keys()
    assert msg["task_template_version_id"] == tt1._active_task_template_version.id

    # Test fetching task template details for workflow (Invalid case - non-existent workflow id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": 99999, "task_template_id": tt1.id},
            request_type="get",
        )

    assert "Client error with status code 404" in str(exc_info.value)
    assert "Task Template not found for the given workflow." in str(exc_info.value)

    # Test fetching task template details for workflow (Invalid case - non-existent task template id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": wf.workflow_id, "task_template_id": 99999},
            request_type="get",
        )

    assert "Client error with status code 404" in str(exc_info.value)
    assert "Task Template not found for the given workflow." in str(exc_info.value)

    # Test fetching task template details for workflow (Invalid case - missing workflow id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": None, "task_template_id": tt1.id},
            request_type="get",
        )

    assert "Client error with status code 422" in str(exc_info.value)

    # Test fetching task template details for workflow (Invalid case - missing task template id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": wf.workflow_id, "task_template_id": None},
            request_type="get",
        )

    assert "Client error with status code 422" in str(exc_info.value)

    # Test fetching task template details for workflow (Invalid case - negative workflow id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": -1, "task_template_id": tt1.id},
            request_type="get",
        )

    assert "Client error with status code 422" in str(exc_info.value)

    # Test fetching task template details for workflow (Invalid case - non-integer workflow id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": 3.14, "task_template_id": tt1.id},
            request_type="get",
        )

    assert "Client error with status code 422" in str(exc_info.value)

    # Test fetching task template details for workflow (Invalid case - string task template id)
    app_route = "/get_task_template_details"
    with pytest.raises(InvalidRequest) as exc_info:
        wf.requester.send_request(
            app_route=app_route,
            message={"workflow_id": wf.workflow_id, "task_template_id": "tt1"},
            request_type="get",
        )

    assert "Client error with status code 422" in str(exc_info.value)


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
    result = pd.read_json(StringIO(msg["workflow_tasks"]))
    assert len(result) == 2

    app_route = f"/workflow/{wf.workflow_id}/workflow_tasks"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"limit": 1, "status": "PENDING"},
        request_type="get",
    )
    assert return_code == 200
    result = pd.read_json(StringIO(msg["workflow_tasks"]))
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


def test_get_workflow_run_for_workflow_reset(db_engine, tool):
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


def test_reset_workflow(db_engine, tool):
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
    params = {
        "workflow_id": [wf.workflow_id],  # This should be a list
    }
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message=params,
        request_type="get",
    )
    assert return_code == 200
    result = pd.read_json(StringIO(msg["workflows"]))
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
    result2 = pd.read_json(StringIO(msg2["workflows"]))
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
        cluster_name="dummy",
        compute_resources={"queue": "null.q", "num_cores": 2},
    )
    t2 = tt1.create_task(
        arg=2,
        cluster_name="dummy",
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
    result = pd.read_json(StringIO(msg["task_instance_status"]))
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
        session.execute(text(query))
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
    import time

    # Use unique names to avoid conflicts with other tests
    unique_id = str(int(time.time() * 1000))  # milliseconds timestamp

    t = tool
    wf = t.create_workflow(name=f"i_am_a_fake_wf_{unique_id}")
    tt1 = t.get_task_template(
        template_name=f"tt_template_resource_{unique_id}",
        command_template=f"echo {unique_id} {{arg}}",
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
    assert msg["num_tasks"] == 2

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
    assert msg["num_tasks"] == 2

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
    assert msg["num_tasks"] == 2

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
    assert msg["num_tasks"] == 1

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
    assert msg["num_tasks"] == 0


def test_node_dependencies(client_env):
    tool = Tool(name="node_dependencies")
    # Generate a random 10-character string excluding digits
    random_name = "".join(random.choice(string.ascii_letters) for _ in range(10))

    wf_1 = tool.create_workflow(name="some_random_workflow_1")
    tt1 = tool.get_task_template(
        template_name=f"{random_name}_1",
        command_template="echo {arg}",
        node_args=["arg"],
    )
    tt2 = tool.get_task_template(
        template_name=f"{random_name}_2",
        command_template="sleep {arg}",
        node_args=["arg"],
    )
    tt3 = tool.get_task_template(
        template_name=f"{random_name}_3",
        command_template="echo hello {arg}",
        node_args=["arg"],
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
    down_ids = set([node["id"] for sublist in msg["down"] for node in sublist])
    up_ids = set([node["id"] for sublist in msg["up"] for node in sublist])
    assert down_ids == set([t3.task_id, t4.task_id])
    assert up_ids == set([t1.task_id])


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
        app_route=app_route, message={"workflow_ids": wfids}, request_type="get"
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
            text(
                f"""
                UPDATE task
                SET status="F"
                WHERE id={t2.task_id}
                """
            )
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
            text(
                """
                DELETE FROM array
                """
            )
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
    assert len(msg["error_logs"]) == 0

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
    _, msg = wf2.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert len(msg["error_logs"]) == 1
    assert msg["error_logs"][0]["task_id"] == t2.task_id
    assert "not found" in msg["error_logs"][0]["error"]

    # Validate error_time field format and type (datetime validation)
    error_log = msg["error_logs"][0]
    assert "error_time" in error_log
    assert error_log["error_time"] is not None
    assert isinstance(
        error_log["error_time"], str
    ), f"error_time should be string, got {type(error_log['error_time'])}"
    # Verify it's a valid datetime string format
    import datetime

    try:
        datetime.datetime.fromisoformat(error_log["error_time"].replace("Z", "+00:00"))
    except ValueError:
        # Try parsing as ISO format with different separators
        datetime.datetime.strptime(error_log["error_time"], "%Y-%m-%d %H:%M:%S")


def test_get_tt_error_log_viz_with_clustering(client_env, db_engine):
    """Test error log visualization with clustering enabled to catch validation errors."""
    t = Tool(name="gui_tt_error_log_clustering")

    # Create workflow with multiple tasks that will fail with similar errors
    wf = t.create_workflow(name=f"i_am_a_clustering_test_wf")
    tt = t.get_task_template(
        template_name="tt_clustering", command_template="{arg}", node_args=["arg"]
    )

    # Create multiple tasks with different arguments but same failing command to test clustering
    tasks = []
    for i in range(3):
        task = tt.create_task(
            arg=f"nonexistent_command_that_will_fail_{i}",
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 2},
            max_attempts=1,
        )
        tasks.append(task)

    wf.add_tasks(tasks)
    wf.run()

    # Test without clustering (should work as before)
    app_route = f"/tt_error_log_viz/{wf.workflow_id}/{tt.id}"
    _, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert len(msg["error_logs"]) == 3  # Should have 3 individual error logs
    assert (
        msg["error_logs"][0]["task_id"] is not None
    )  # Individual error fields should be present

    # Test with clustering enabled (this was failing before the fix)
    app_route_with_clustering = (
        f"/tt_error_log_viz/{wf.workflow_id}/{tt.id}?cluster_errors=true"
    )
    _, clustered_msg = wf.requester.send_request(
        app_route=app_route_with_clustering, message={}, request_type="get"
    )

    # Should have fewer clustered error logs (similar errors grouped together)
    assert len(clustered_msg["error_logs"]) <= 3
    assert len(clustered_msg["error_logs"]) > 0

    # Check that clustering fields are present
    first_clustered_error = clustered_msg["error_logs"][0]
    assert "error_score" in first_clustered_error
    assert "group_instance_count" in first_clustered_error
    assert "task_instance_ids" in first_clustered_error
    assert "task_ids" in first_clustered_error
    assert "sample_error" in first_clustered_error

    # Check that individual error fields are None for clustered data
    assert first_clustered_error["task_id"] is None
    assert first_clustered_error["task_instance_id"] is None
    assert first_clustered_error["task_instance_err_id"] is None
    assert first_clustered_error["error_time"] is None
    assert first_clustered_error["error"] is None
    assert first_clustered_error["task_instance_stderr_log"] is None

    # Validate first_error_time field format and type (datetime validation for clustering)
    assert "first_error_time" in first_clustered_error
    assert first_clustered_error["first_error_time"] is not None
    assert isinstance(
        first_clustered_error["first_error_time"], str
    ), f"first_error_time should be string, got {type(first_clustered_error['first_error_time'])}"

    # Verify first_error_time is a valid datetime string format
    import datetime

    try:
        datetime.datetime.fromisoformat(
            first_clustered_error["first_error_time"].replace("Z", "+00:00")
        )
    except ValueError:
        # Try parsing as ISO format with different separators
        datetime.datetime.strptime(
            first_clustered_error["first_error_time"], "%Y-%m-%d %H:%M:%S"
        )

    # Test that the response can be serialized without Pydantic validation errors
    # This specifically tests the fix for the datetime validation issue
    import json

    json.dumps(clustered_msg)  # This should not raise any serialization errors

    # Additional test: verify all error logs in clustered response have proper string types
    for error_log in clustered_msg["error_logs"]:
        # Individual error fields should be None for clustered data
        assert error_log["error_time"] is None
        # But first_error_time should be a valid string
        if error_log.get("first_error_time"):
            assert isinstance(error_log["first_error_time"], str)

    # Additional test: Test the exact scenario that was failing before the fix
    # Create a workflow with multiple similar errors to ensure clustering works without datetime validation errors
    wf_detailed = t.create_workflow(name=f"i_am_a_detailed_clustering_test_wf")
    tt_detailed = t.get_task_template(
        template_name="tt_detailed_clustering",
        command_template="python -c 'raise Exception(\"Test error for clustering {arg}\")'",
        node_args=["arg"],
    )

    # Create multiple tasks that will fail with similar but not identical errors
    detailed_tasks = []
    for i in range(5):
        task = tt_detailed.create_task(
            arg=f"test_{i}",
            cluster_name="sequential",
            compute_resources={"queue": "null.q", "num_cores": 2},
            max_attempts=1,
        )
        detailed_tasks.append(task)

    wf_detailed.add_tasks(detailed_tasks)
    wf_detailed.run()

    # Test clustering with similar errors (should group them together)
    app_route_detailed = f"/tt_error_log_viz/{wf_detailed.workflow_id}/{tt_detailed.id}?cluster_errors=true"
    _, detailed_clustered_msg = wf_detailed.requester.send_request(
        app_route=app_route_detailed, message={}, request_type="get"
    )

    # Should have fewer clustered error logs (similar errors should be grouped)
    assert len(detailed_clustered_msg["error_logs"]) <= 5
    assert len(detailed_clustered_msg["error_logs"]) > 0

    # Validate that all clustered error logs have proper string types for datetime fields
    for error_log in detailed_clustered_msg["error_logs"]:
        assert error_log["error_time"] is None  # Individual error_time should be None
        if error_log.get("first_error_time"):
            assert isinstance(
                error_log["first_error_time"], str
            ), f"first_error_time should be string, got {type(error_log['first_error_time'])}"
            # Verify it's a valid datetime string
            try:
                datetime.datetime.fromisoformat(
                    error_log["first_error_time"].replace("Z", "+00:00")
                )
            except ValueError:
                datetime.datetime.strptime(
                    error_log["first_error_time"], "%Y-%m-%d %H:%M:%S"
                )

    # Final validation: ensure the entire response can be JSON serialized without errors
    json.dumps(detailed_clustered_msg)


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
    wf.run()
    app_route = f"/workflow_details_viz/{wf.workflow_id}"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert return_code == 200
    assert msg[0]["wf_name"] == "i_am_another_fake_wf_vv"
    assert msg[0]["tool_name"] == "task_detail_tool"
    assert msg[0]["wf_status"] == "D"


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
        message={
            "tool": "task_detail_tool",
            "wf_attribute_key": "test_attribute",
            "wf_attribute_value": "test",
        },
        request_type="get",
    )

    assert return_code == 200
    assert msg["workflows"][0]["wf_tool"] == tool_name


def test_task_update_statuses(client_env, db_engine, tool):
    import time

    unique_id = str(int(time.time() * 1000))  # milliseconds timestamp

    def generate_workflow_and_tasks(tool):
        # Create a wf with 1 failed task
        wf = tool.create_workflow(workflow_args=f"test_cli_update_workflow_{unique_id}")
        tasks = []
        command_str = "exit -9"
        task_template = tool.get_task_template(
            template_name=f"failed_tt_{unique_id}",
            command_template=f"exit_{unique_id} {{arg}}",
            node_args=["arg"],
            task_args=[],
            op_args=[],
        )
        task = task_template.create_task(arg=command_str, name=f"task", max_attempts=1)
        tasks.append(task)
        wf.add_tasks(tasks)
        return wf, tasks

    wf, ts = generate_workflow_and_tasks(tool)
    t = ts[0]
    wf.run()
    assert t.task_id is not None
    assert wf.workflow_id is not None
    with Session(bind=db_engine) as session:
        from sqlalchemy import text

        res = session.execute(
            text(f"select status from task where id= {t.task_id}")
        ).fetchone()
        assert res[0] == "F"
        res = session.execute(
            text(f"select status from workflow where id= {wf.workflow_id}")
        ).fetchone()
        assert res[0] == "F"

    _, resp = wf.requester.send_request(
        app_route="/task/update_statuses",
        message={
            "task_ids": t.task_id,
            "new_status": "D",
            "workflow_status": "F",
            "workflow_id": wf.workflow_id,
        },
        request_type="put",
    )

    with Session(bind=db_engine) as session:
        from sqlalchemy import text

        res = session.execute(
            text(f"select status from task where id= {t.task_id}")
        ).fetchone()
        assert res[0] == "D"
        res = session.execute(
            text(f"select status from workflow where id= {wf.workflow_id}")
        ).fetchone()
        # According to the code in update_task_statuses, the workflow status should be updated to D if all tasks are done
        assert res[0] == "D"


def test_increase_resources_for_resource_error_tasks(client_env, db_engine, tool):
    """Test the increase_resources route for tasks with latest TaskInstance in RESOURCE_ERROR status."""
    import json
    import time
    from datetime import datetime

    unique_id = str(int(time.time() * 1000))

    # Create workflow with tasks that have resource scales
    wf = tool.create_workflow(workflow_args=f"test_increase_resources_{unique_id}")

    # Create task template with resource scales
    tt = tool.get_task_template(
        template_name=f"resource_test_tt_{unique_id}",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q", "memory": 1, "runtime": 60},
        default_cluster_name="sequential",
    )

    # Create tasks with different resource scales
    task1 = tt.create_task(
        arg=1,
        name=f"task1_{unique_id}",
        resource_scales={"memory": 0.5, "runtime": 0.2},  # numeric scales
        compute_resources={"memory": 2, "runtime": 120},
    )

    task2 = tt.create_task(
        arg=2,
        name=f"task2_{unique_id}",
        resource_scales={
            "memory": iter([4, 8, 16]),
            "runtime": iter([240, 480]),
        },  # iterator scales
        compute_resources={"memory": 1, "runtime": 60},
    )

    wf.add_tasks([task1, task2])
    wf.bind()
    wf._bind_tasks()

    # Get task IDs from database
    with Session(bind=db_engine) as session:
        tasks = (
            session.execute(select(Task).where(Task.workflow_id == wf.workflow_id))
            .scalars()
            .all()
        )

        task1_db = next(t for t in tasks if t.name == f"task1_{unique_id}")
        task2_db = next(t for t in tasks if t.name == f"task2_{unique_id}")

        # Set tasks to ERROR_RECOVERABLE and ERROR_FATAL status
        task1_db.status = TaskStatus.ERROR_RECOVERABLE
        task1_db.num_attempts = 1
        task2_db.status = TaskStatus.ERROR_FATAL
        task2_db.num_attempts = 2

        # Get task resources
        task1_resources = session.get(TaskResources, task1_db.task_resources_id)
        task2_resources = session.get(TaskResources, task2_db.task_resources_id)

        # Set initial requested resources
        task1_resources.requested_resources = json.dumps({"memory": 2, "runtime": 120})
        task2_resources.requested_resources = json.dumps({"memory": 1, "runtime": 60})

        # Create TaskInstances with RESOURCE_ERROR status
        now = datetime.now()
        task1_ti = TaskInstance(
            workflow_run_id=1,  # dummy workflow run
            array_id=1,  # dummy array
            task_id=task1_db.id,
            task_resources_id=task1_resources.id,
            array_batch_num=1,
            array_step_id=1,
            status=TaskInstanceStatus.RESOURCE_ERROR,
            status_date=now,
        )

        task2_ti = TaskInstance(
            workflow_run_id=1,
            array_id=1,
            task_id=task2_db.id,
            task_resources_id=task2_resources.id,
            array_batch_num=1,
            array_step_id=1,
            status=TaskInstanceStatus.RESOURCE_ERROR,
            status_date=now,
        )

        session.add_all([task1_ti, task2_ti])
        session.commit()

        # Store original values for comparison and task IDs
        task1_id = task1_db.id
        task2_id = task2_db.id
        task1_resources_id = task1_resources.id
        task2_resources_id = task2_resources.id

    # Call the increase_resources route
    app_route = f"/workflow/{wf.workflow_id}/increase_resources"
    return_code, response = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type="post",
    )

    # Verify response
    assert return_code == 200
    assert response["updated_task_count"] == 2
    assert len(response["updated_task_ids"]) == 2
    assert task1_id in response["updated_task_ids"]
    assert task2_id in response["updated_task_ids"]

    # Verify task status changes
    with Session(bind=db_engine) as session:
        updated_task1 = session.get(Task, task1_id)
        updated_task2 = session.get(Task, task2_id)

        # Both tasks should now be ERROR_RECOVERABLE
        assert updated_task1.status == TaskStatus.ERROR_RECOVERABLE
        assert updated_task2.status == TaskStatus.ERROR_RECOVERABLE

        # Verify resource scaling
        updated_task1_resources = session.get(TaskResources, task1_resources_id)
        updated_task2_resources = session.get(TaskResources, task2_resources_id)

        # Task1: memory=2, runtime=120 with scales 0.5, 0.2
        # Expected: memory = ceil(2 * (1 + 0.5)) = ceil(3) = 3
        # Expected: runtime = ceil(120 * (1 + 0.2)) = ceil(144) = 144
        task1_new_resources = json.loads(updated_task1_resources.requested_resources)
        assert task1_new_resources["memory"] == 3
        assert task1_new_resources["runtime"] == 144

        # Task2: memory=1, runtime=60 with scales iter([4,8,16]), iter([240,480])
        # num_attempts=2, so attempt_index = max(0, 2-1) = 1
        # Expected: memory = 8 (index 1 from iter([4,8,16]))
        # Expected: runtime = 480 (index 1 from iter([240,480]))
        task2_new_resources = json.loads(updated_task2_resources.requested_resources)
        assert task2_new_resources["memory"] == 8
        assert task2_new_resources["runtime"] == 480

        # Verify task_resources_type_id changed to 'A' (Adjusted)
        assert updated_task1_resources.task_resources_type_id == "A"
        assert updated_task2_resources.task_resources_type_id == "A"


def test_increase_resources_no_matching_tasks(client_env, db_engine, tool):
    """Test the increase_resources route when no tasks match the criteria."""
    import time

    unique_id = str(int(time.time() * 1000))

    # Create workflow with tasks that don't match criteria
    wf = tool.create_workflow(workflow_args=f"test_no_matching_{unique_id}")

    tt = tool.get_task_template(
        template_name=f"no_match_tt_{unique_id}",
        command_template="echo {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task = tt.create_task(arg=1, name=f"task_{unique_id}")
    wf.add_tasks([task])
    wf.bind()
    wf._bind_tasks()

    # Call the increase_resources route
    app_route = f"/workflow/{wf.workflow_id}/increase_resources"
    return_code, response = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type="post",
    )

    # Verify response for no matching tasks
    assert return_code == 200
    assert response["updated_task_count"] == 0
    assert len(response["updated_task_ids"]) == 0


def test_increase_resources_invalid_workflow_id(client_env, db_engine, tool):
    """Test the increase_resources route with invalid workflow ID."""
    invalid_workflow_id = 99999

    # Call the increase_resources route with invalid workflow ID
    app_route = f"/workflow/{invalid_workflow_id}/increase_resources"
    return_code, response = tool.requester.send_request(
        app_route=app_route,
        message={},
        request_type="post",
    )

    # Should return empty result, not error
    assert return_code == 200
    assert response["updated_task_count"] == 0
    assert len(response["updated_task_ids"]) == 0


def test_increase_resources_selective_update(client_env, db_engine, tool):
    """Test that only tasks with latest TaskInstance in Z status get updated."""
    import json
    import time
    from datetime import datetime

    unique_id = str(int(time.time() * 1000))

    # Create workflow with 3 tasks
    wf = tool.create_workflow(workflow_args=f"test_selective_update_{unique_id}")

    tt = tool.get_task_template(
        template_name=f"selective_test_tt_{unique_id}",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q", "memory": 1, "runtime": 60},
        default_cluster_name="sequential",
    )

    # Create 3 tasks with resource scales
    task1 = tt.create_task(
        arg=1,
        name=f"task1_{unique_id}",
        resource_scales={"memory": 0.5, "runtime": 0.2},
        compute_resources={"memory": 2, "runtime": 120},
    )

    task2 = tt.create_task(
        arg=2,
        name=f"task2_{unique_id}",
        resource_scales={"memory": 0.3, "runtime": 0.1},
        compute_resources={"memory": 1, "runtime": 60},
    )

    task3 = tt.create_task(
        arg=3,
        name=f"task3_{unique_id}",
        resource_scales={"memory": 0.4, "runtime": 0.3},
        compute_resources={"memory": 3, "runtime": 180},
    )

    wf.add_tasks([task1, task2, task3])
    wf.bind()
    wf._bind_tasks()

    # Get task IDs from database and set up different scenarios
    with Session(bind=db_engine) as session:
        tasks = (
            session.execute(select(Task).where(Task.workflow_id == wf.workflow_id))
            .scalars()
            .all()
        )

        task1_db = next(t for t in tasks if t.name == f"task1_{unique_id}")
        task2_db = next(t for t in tasks if t.name == f"task2_{unique_id}")
        task3_db = next(t for t in tasks if t.name == f"task3_{unique_id}")

        # Task1: DONE status with DONE instance (should NOT be updated)
        task1_db.status = TaskStatus.DONE
        task1_db.num_attempts = 1

        # Task2: ERROR_RECOVERABLE status with RESOURCE_ERROR instance (should be updated)
        task2_db.status = TaskStatus.ERROR_RECOVERABLE
        task2_db.num_attempts = 1

        # Task3: ERROR_RECOVERABLE status with ERROR instance (should NOT be updated)
        task3_db.status = TaskStatus.ERROR_RECOVERABLE
        task3_db.num_attempts = 1

        # Get task resources
        task1_resources = session.get(TaskResources, task1_db.task_resources_id)
        task2_resources = session.get(TaskResources, task2_db.task_resources_id)
        task3_resources = session.get(TaskResources, task3_db.task_resources_id)

        # Set initial requested resources
        task1_resources.requested_resources = json.dumps({"memory": 2, "runtime": 120})
        task2_resources.requested_resources = json.dumps({"memory": 1, "runtime": 60})
        task3_resources.requested_resources = json.dumps({"memory": 3, "runtime": 180})

        # Create TaskInstances with different statuses
        now = datetime.now()

        # Task1: DONE instance (should be ignored)
        task1_ti = TaskInstance(
            workflow_run_id=1,
            array_id=1,
            task_id=task1_db.id,
            task_resources_id=task1_resources.id,
            array_batch_num=1,
            array_step_id=1,
            status=TaskInstanceStatus.DONE,
            status_date=now,
        )

        # Task2: RESOURCE_ERROR instance (should be updated)
        task2_ti = TaskInstance(
            workflow_run_id=1,
            array_id=1,
            task_id=task2_db.id,
            task_resources_id=task2_resources.id,
            array_batch_num=1,
            array_step_id=1,
            status=TaskInstanceStatus.RESOURCE_ERROR,
            status_date=now,
        )

        # Task3: ERROR instance (should be ignored)
        task3_ti = TaskInstance(
            workflow_run_id=1,
            array_id=1,
            task_id=task3_db.id,
            task_resources_id=task3_resources.id,
            array_batch_num=1,
            array_step_id=1,
            status=TaskInstanceStatus.ERROR,
            status_date=now,
        )

        session.add_all([task1_ti, task2_ti, task3_ti])
        session.commit()

        # Store task IDs for verification
        task1_id = task1_db.id
        task2_id = task2_db.id
        task3_id = task3_db.id
        task2_resources_id = task2_resources.id

    # Call the increase_resources route
    app_route = f"/workflow/{wf.workflow_id}/increase_resources"
    return_code, response = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type="post",
    )

    # Verify response - only task2 should be updated
    assert return_code == 200
    assert response["updated_task_count"] == 1
    assert len(response["updated_task_ids"]) == 1
    assert task2_id in response["updated_task_ids"]
    assert task1_id not in response["updated_task_ids"]
    assert task3_id not in response["updated_task_ids"]

    # Verify task status changes
    with Session(bind=db_engine) as session:
        updated_task1 = session.get(Task, task1_id)
        updated_task2 = session.get(Task, task2_id)
        updated_task3 = session.get(Task, task3_id)

        # Task1 should remain DONE (unchanged)
        assert updated_task1.status == TaskStatus.DONE

        # Task2 should be ERROR_RECOVERABLE (updated)
        assert updated_task2.status == TaskStatus.ERROR_RECOVERABLE

        # Task3 should remain ERROR_RECOVERABLE (unchanged)
        assert updated_task3.status == TaskStatus.ERROR_RECOVERABLE

        # Verify resource scaling only applied to task2
        updated_task2_resources = session.get(TaskResources, task2_resources_id)

        # Task2: memory=1, runtime=60 with scales 0.3, 0.1
        # Expected: memory = ceil(1 * (1 + 0.3)) = ceil(1.3) = 2
        # Expected: runtime = ceil(60 * (1 + 0.1)) = ceil(66) = 66
        task2_new_resources = json.loads(updated_task2_resources.requested_resources)
        assert task2_new_resources["memory"] == 2
        assert task2_new_resources["runtime"] == 66

        # Verify task_resources_type_id changed to 'A' only for task2
        assert updated_task2_resources.task_resources_type_id == "A"

        # Verify task1 and task3 resources remain unchanged
        task1_resources = session.get(TaskResources, task1_db.task_resources_id)
        task3_resources = session.get(TaskResources, task3_db.task_resources_id)

        task1_original_resources = json.loads(task1_resources.requested_resources)
        task3_original_resources = json.loads(task3_resources.requested_resources)

        assert task1_original_resources == {"memory": 2, "runtime": 120}
        assert task3_original_resources == {"memory": 3, "runtime": 180}

    # Call the increase_resources route AGAIN
    app_route = f"/workflow/{wf.workflow_id}/increase_resources"
    return_code, response = wf.requester.send_request(
        app_route=app_route,
        message={},
        request_type="post",
    )

    # Verify response - only task2 should be updated again
    assert return_code == 200
    assert response["updated_task_count"] == 1
    assert len(response["updated_task_ids"]) == 1
    assert task2_id in response["updated_task_ids"]
    assert task1_id not in response["updated_task_ids"]
    assert task3_id not in response["updated_task_ids"]

    # Verify task2 resources increased further after second call
    with Session(bind=db_engine) as session:
        updated_task2_resources_second = session.get(TaskResources, task2_resources_id)

        # Task2: memory=2, runtime=66 with scales 0.3, 0.1 (after first increase)
        # Expected: memory = ceil(2 * (1 + 0.3)) = ceil(2.6) = 3
        # Expected: runtime = ceil(66 * (1 + 0.1)) = ceil(72.6) = 73
        task2_second_resources = json.loads(
            updated_task2_resources_second.requested_resources
        )
        assert task2_second_resources["memory"] == 3
        assert task2_second_resources["runtime"] == 73

        # Verify task1 and task3 resources still remain unchanged
        task1_resources = session.get(TaskResources, task1_db.task_resources_id)
        task3_resources = session.get(TaskResources, task3_db.task_resources_id)

        task1_final_resources = json.loads(task1_resources.requested_resources)
        task3_final_resources = json.loads(task3_resources.requested_resources)

        assert task1_final_resources == {"memory": 2, "runtime": 120}  # Still unchanged
        assert task3_final_resources == {"memory": 3, "runtime": 180}  # Still unchanged

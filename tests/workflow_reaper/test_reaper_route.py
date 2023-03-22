import pytest

from jobmon.core.constants import WorkflowRunStatus
from jobmon.client.workflow_run import WorkflowRunFactory

"""These are test cases to test if the routes are working.
Further tests in test_workflow_reaper to verify the logic.
"""


def test_fix_status_inconsistency(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task_1 = tt1.create_task(arg=1)
    wf.add_tasks([task_1])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)

    app_route = f"/workflow/{wf.workflow_id}/fix_status_inconsistency"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={"increase_step": 10}, request_type="put"
    )
    assert return_code == 200
    assert msg["wfid"] == 0


def test_workflow_name_and_args(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task_1 = tt1.create_task(arg=1)
    wf.add_tasks([task_1])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)
    app_route = f"/workflow/{wf.workflow_id}/workflow_name_and_args"
    return_code, msg = wf.requester.send_request(
        app_route=app_route, message={}, request_type="get"
    )
    assert return_code == 200
    assert msg["workflow_args"] is not None
    assert msg["workflow_name"] == "i_am_a_fake_wf"


def test_lost_workflow_run(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task_1 = tt1.create_task(arg=1)
    wf.add_tasks([task_1])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)
    app_route = f"/lost_workflow_run"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"status": "R", "version": "whatever"},
        request_type="get",
    )
    assert return_code == 200


def test_reap_workflow_run(db_engine, tool):
    t = tool
    wf = t.create_workflow(name="i_am_a_fake_wf")
    tt1 = t.get_task_template(
        template_name="tt1",
        command_template="sleep {arg}",
        node_args=["arg"],
        default_compute_resources={"queue": "null.q"},
        default_cluster_name="sequential",
    )

    task_1 = tt1.create_task(arg=1)
    wf.add_tasks([task_1])
    wf.bind()
    wf._bind_tasks()
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    wfr._update_status(WorkflowRunStatus.BOUND)
    app_route = f"/workflow_run/{wfr.workflow_run_id}/reap"
    return_code, msg = wf.requester.send_request(
        app_route=app_route,
        message={"status": "R", "version": "whatever"},
        request_type="put",
    )
    assert return_code == 200

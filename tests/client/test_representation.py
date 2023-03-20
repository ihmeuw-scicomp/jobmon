from jobmon.client.workflow_run import WorkflowRun


def test_repr_strings(tool, task_template):
    """Smoke tests to check the various client object repr strings."""

    task = task_template.create_task(arg="echo 1")
    wf = tool.create_workflow()

    tool.__repr__()
    task_template.__repr__()
    task.__repr__()
    wf.__repr__()

    wf.add_task(task)
    wf.bind()
    wf._bind_tasks()
    wfr = WorkflowRun(wf.workflow_id)
    wfr.bind()

    wfr.__repr__()
    wf._dag.__repr__()

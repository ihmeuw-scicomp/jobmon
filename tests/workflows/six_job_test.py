import os
import sys
import uuid

from jobmon.client.api import Tool
from jobmon.client.logging import configure_client_logging


thisdir = os.path.dirname(os.path.realpath(__file__))


def get_task_template(tool, template_name):
    tt = tool.get_task_template(
        template_name=template_name,
        command_template="{command}",
        node_args=['command']
    )
    return tt


def six_job_test(cluster_name: str, wf_id_file: str = None):
    """
    Creates and runs one workflow with six jobs. Used to 'smoke test' a
    new deployment of jobmon.
    """
    # Configure client logging first to enable OTLP logging from the start
    configure_client_logging()
    
    # First Tier
    # Deliberately put in on the long queue with max runtime > 1 day
    tool = Tool(name=f"Jackalope alpha testing - {cluster_name}")

    t1 = get_task_template(tool, "phase_1").create_task(
        name='t1',
        command="sleep 10"
    )

    # Second Tier, both depend on first tier
    t2 = get_task_template(tool, "phase_2").create_task(
        name='t2',
        command="sleep 20",
        upstream_tasks=[t1]
    )

    t3 = get_task_template(tool, "phase_2").create_task(
        name='t3',
        command="sleep 25",
        upstream_tasks=[t1]
    )

    # Third Tier, cross product dependency on second tier
    t4 = get_task_template(tool, "phase_3").create_task(
        name='t4',
        command="sleep 17",
        upstream_tasks=[t2, t3]
    )

    t5 = get_task_template(tool, "phase_3").create_task(
        name='t5',
        command="sleep 13",
        upstream_tasks=[t2, t3]
    )

    # Fourth Tier, ties it all back together
    t6 = get_task_template(tool, "phase_4").create_task(
        name='t6',
        command="sleep 19",
        upstream_tasks=[t4, t5]
    )

    tool.set_default_compute_resources_from_yaml(
        default_cluster_name=cluster_name,
        yaml_file=os.path.join(thisdir, "six_job_test_resources.yaml"),
        set_task_templates=True,
        ignore_missing_keys=True
    )

    wf = tool.create_workflow(
        workflow_args="six-job-test-{}".format(uuid.uuid4()),
        name='six_job_test')
    wf.add_tasks([t1, t2, t3, t4, t5, t6])
    print("Running the workflow, about 70 seconds minimum")
    wfr_status = wf.run()
    print(f"workflow_id={wf.workflow_id}")
    if wf_id_file is not None:
        f = open(wf_id_file, "w")
        f.write(str(wf.workflow_id))
        f.close()
    if wfr_status == 'D':
        print("All good, dancing pixies, with the following workflow_id.")
        print(f"workflow_id={wf.workflow_id}")
    else:
        raise ValueError(f"Workflow should be successful, not state {wfr_status}")


if __name__ == "__main__":
    cluster_name = sys.argv[1]
    wf_id_file = None
    if len(sys.argv) > 2:
        wf_id_file = sys.argv[2]
    six_job_test(cluster_name, wf_id_file)
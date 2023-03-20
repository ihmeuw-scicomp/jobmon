import getpass
import uuid

from jobmon.client.tool import Tool

"""
TODO
1. Add injectable variables for IHME specific file paths and URLs
2. Replicate this file in the training repo? 
3. R versions

Instructions:

  This workflow is similar to many IHME modelling workflows, with a class three-phase fork-and-join task flow:
  1. A data preparation phase, with one job. In a real modelling pipeline this would split a large input file
     input file into manageable pieces and clean the data. In our example this task generates dummy
     intermediate files for the next phase.
  2. A broad phase with one Task per (dummy) location_id.
  3. A summarization phase consisting of a single Task that reads the intermediate results from the individual
     location models.

  The steps in this example are:
  1. Create a tool
  2. Create a workflow using the tool from step 1
  3. Create three task templates using the tool from step 1
     a. Template for the Data Prep Task
     b. Template for the separate location tasks
     c. Template for the Summarization Phase
  4. Create tasks using the templates from step 3
     a. Add the necessary edge dependencies
  5. Add created tasks to the workflow
  6. Run the workflow

To actually run the provided example:
  Make sure Jobmon is installed in your activated conda environment, and that you're on
  the Slurm cluster in a srun session. From the root of the repo, run:
     $ python training_scripts/workflow_template_example.py
"""

user = getpass.getuser()
wf_uuid = uuid.uuid4()

# Create a tool
tool = Tool(name="quickstart_tool_python")

# Create a workflow, and set the executor
workflow = tool.create_workflow(
    name=f"quickstart_workflow_{wf_uuid}",
)

# Create task templates

data_prep_template = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 1,
        "memory": "1G",
        "runtime": "1m",
        "stdout": "/tmp",
        "stderr": "/tmp",
        "project": "my_slurm_account",
    },
    template_name="quickstart_data_prep_template",
    default_cluster_name="slurm",
    command_template="python "
                     "/code_dir/docsource/quickstart_tasks/data_prep.py "
                     "--location_set_id {location_set_id} "
                     "--root_data_dir {root_data_dir} "
                     "--log_level {log_level}",
    node_args=[],
    task_args=["location_set_id", "root_data_dir"],
    op_args=["log_level"],
)

parallel_by_location_template = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 2,
        "memory": "1G",
        "runtime": "10m",
        "stdout": "/tmp",
        "stderr": "/tmp",
        "project": "my_slurm_account"
    },
    template_name="quickstart_location_template",
    default_cluster_name="slurm",
    command_template="python "
                     "/code_dir/docsource/quickstart_tasks/one_location.py "
                     "--location_id {location_id} "
                     "--root_data_dir {root_data_dir} "
                     "--log_level {log_level} ",
    node_args=["location_id"],
    task_args=["root_data_dir"],
    op_args=["log_level"],
)

summarization_template = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 2,
        "memory": "1G",
        "runtime": "10m",
        "stdout": "/tmp",
        "stderr": "/tmp",
        "project": "my_slurm_account"
    },
    template_name="quickstart_summarization_template",
    default_cluster_name="slurm",
    command_template="python /code_dir/docsource/quickstart_tasks/summarization.py "
                     "--root_data_dir {root_data_dir} "
                     "--log_level {log_level}",
    node_args=[],
    task_args=["root_data_dir"],
    op_args=["log_level"],
)


# Create tasks
location_set_id = 5
location_set = list(range(location_set_id))
root_data_dir = f"/home/{user}/quickstart/data"

data_prep_task = data_prep_template.create_task(
    name="data_prep_task",
    upstream_tasks=[],
    root_data_dir=root_data_dir,
    location_set_id=location_set_id,
    log_level="DEBUG"
)
workflow.add_tasks([data_prep_task])

location_tasks = parallel_by_location_template.create_tasks(
    root_data_dir=root_data_dir,
    location_id=location_set,
    log_level="DEBUG"
)
workflow.add_tasks(location_tasks)

summarization_task = summarization_template.create_task(
    root_data_dir=root_data_dir,
    log_level="DEBUG"
)
workflow.add_tasks([summarization_task])

# Connect the dependencies. Notice the use of get_tasks_by_node_args

for loc_id in location_set:
    foo = {"location_id": loc_id}
    single_task = workflow.get_tasks_by_node_args("quickstart_location_template", **foo)
    # Notice it returns a set. Should only be one
    single_task[0].add_upstream(data_prep_task)
    summarization_task.add_upstream(single_task[0])

# Calling workflow.bind() first just so that we can get the workflow id
workflow.bind()
print("Workflow creation complete.")
print(f"Running workflow with ID {workflow.workflow_id}.")
print("If you have a Jobmon GUI deployed, see the Jobmon GUI for full information:")
print(f"https://jobmon-gui.mydomain.com/#/workflow/{workflow.workflow_id}/tasks")


# run workflow
status = workflow.run()
print(f"Workflow {workflow.workflow_id} completed with status {status}.")
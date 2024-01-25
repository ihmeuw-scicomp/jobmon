from jobmon.client.api import Tool

tool = Tool(tool_name='my_example_tool')

example_task_template = tool.get_task_template(
    template_name="my_example_task_template",
    command_template="python model_script.py --loc_id {location_id}",
    node_args=["location_id"],
    default_cluster_name="slurm",
    default_compute_resources={"queue": "all.q"},
)

example_tasks = example_task_template.create_tasks(
    location_id=[1, 2, 3],
)

workflow = tool.create_workflow()
workflow.add_tasks(example_tasks)
workflow.run()

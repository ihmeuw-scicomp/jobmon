library(jobmonr)

example_tool <- jobmonr::tool("example_project")

example_task_template <- jobmonr::task_template(
    template_name="my_example_task_template",
    command_template="python model_script.py --loc_id {location_id}",
    node_args=c("location_id")
)

workflow <- jobmonr::workflow(example_tool)

example_tasks <- jobmonr::array_tasks(task_template=example_task_template, location_id=1:3)

jobmonr::add_tasks(workflow, example_tasks)

status <- jobmonr::run(workflow)

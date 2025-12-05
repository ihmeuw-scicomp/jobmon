library(jobmonr)

# Create a tool
example_tool <- jobmonr::tool(name = "example_project")

# Create a task template
example_task_template <- jobmonr::task_template(
    tool = example_tool,  # Tool is required
    template_name = "my_example_task_template",
    command_template = "python model_script.py --loc_id {location_id}",
    node_args = list("location_id")
)

# Create a workflow
wf <- jobmonr::workflow(
    tool = example_tool,
    workflow_args = paste0("example_", Sys.Date())
)

# Create tasks for locations 1, 2, 3
example_tasks <- jobmonr::array_tasks(
    task_template = example_task_template,
    location_id = as.list(1:3)
)

# Add tasks and run
wf <- jobmonr::add_tasks(wf, example_tasks)

status <- jobmonr::run(
    workflow = wf,
    resume = FALSE,
    seconds_until_timeout = 3600
)

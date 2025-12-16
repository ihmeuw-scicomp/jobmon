library(jobmonr)

# =============================================================================
# Jobmon R Client Quickstart Example
# =============================================================================
# This example demonstrates a fork-join workflow pattern:
# 1. Data preparation task
# 2. Parallel processing tasks (one per location)
# 3. Summarization task that waits for all parallel tasks

# Configuration
username <- Sys.getenv("USER")
root_data_dir <- paste0("/home/", username, "/quickstart/data")
location_set_id <- as.integer(5)
location_ids <- as.list(seq(from = 0, to = location_set_id))

# =============================================================================
# 1. Create a Tool
# =============================================================================
my_tool <- jobmonr::tool(name = "r_quickstart_tool")

# Set default compute resources for the tool
my_tool <- jobmonr::set_default_tool_resources(
    tool = my_tool,
    default_cluster_name = "slurm",
    resources = list(
        cores = 1L,
        queue = "all.q",
        runtime = "2m",
        memory = "1G",
        project = "proj_scicomp"
    )
)

# =============================================================================
# 2. Create a Workflow
# =============================================================================
wf <- jobmonr::workflow(
    tool = my_tool,
    workflow_args = paste0("quickstart_workflow_", Sys.Date())
)

# =============================================================================
# 3. Create Task Templates
# =============================================================================

# Template for data preparation
data_prep_template <- jobmonr::task_template(
    tool = my_tool,
    template_name = "quickstart_data_prep_template",
    command_template = paste(
        "{python_exec}",
        "/code_dir/docsource/quickstart_tasks/data_prep.py",
        "--location_set_id {location_set_id}",
        "--root_data_dir {root_data_dir}",
        "--log_level {log_level}",
        sep = " "
    ),
    task_args = list("location_set_id", "root_data_dir"),
    op_args = list("python_exec", "log_level")
)

# Override resources for data prep (needs archive access)
data_prep_template <- jobmonr::set_default_template_resources(
    task_template = data_prep_template,
    default_cluster_name = "slurm",
    resources = list(
        queue = "all.q",
        constraints = "archive"
    )
)

# Template for parallel location processing
location_template <- jobmonr::task_template(
    tool = my_tool,
    template_name = "quickstart_location_template",
    command_template = paste(
        "{python_exec}",
        "/code_dir/docsource/quickstart_tasks/one_location.py",
        "--location_id {location_id}",
        "--root_data_dir {root_data_dir}",
        "--log_level {log_level}",
        sep = " "
    ),
    node_args = list("location_id"),
    task_args = list("root_data_dir"),
    op_args = list("python_exec", "log_level")
)

# Template for summarization
summarization_template <- jobmonr::task_template(
    tool = my_tool,
    template_name = "quickstart_summarization_template",
    command_template = paste(
        "{python_exec}",
        "/code_dir/docsource/quickstart_tasks/summarization.py",
        "--root_data_dir {root_data_dir}",
        "--log_level {log_level}",
        sep = " "
    ),
    task_args = list("root_data_dir"),
    op_args = list("python_exec", "log_level")
)

# =============================================================================
# 4. Create Tasks
# =============================================================================

# Data preparation task
data_prep_task <- jobmonr::task(
    task_template = data_prep_template,
    name = "data_prep_task",
    root_data_dir = root_data_dir,
    location_set_id = location_set_id,
    python_exec = Sys.getenv("RETICULATE_PYTHON"),
    log_level = "DEBUG"
)
wf <- jobmonr::add_tasks(wf, list(data_prep_task))

# Create all location tasks at once using array_tasks
location_tasks <- jobmonr::array_tasks(
    task_template = location_template,
    upstream_tasks = list(data_prep_task),
    location_id = location_ids,  # List of values for parallel execution
    root_data_dir = root_data_dir,
    python_exec = Sys.getenv("RETICULATE_PYTHON"),
    log_level = "DEBUG"
)
wf <- jobmonr::add_tasks(wf, location_tasks)

# Summarization task depends on all location tasks
summarization_task <- jobmonr::task(
    task_template = summarization_template,
    name = "summarization_task",
    upstream_tasks = location_tasks,
    root_data_dir = root_data_dir,
    python_exec = Sys.getenv("RETICULATE_PYTHON"),
    log_level = "DEBUG"
)
wf <- jobmonr::add_tasks(wf, list(summarization_task))

# =============================================================================
# 5. Run the Workflow
# =============================================================================
status <- jobmonr::run(
    workflow = wf,
    resume = FALSE,
    seconds_until_timeout = 7200  # 2 hours
)

if (status == "D") {
    cat("Workflow completed successfully!\n")
} else {
    stop(paste("Workflow failed with status:", status))
}
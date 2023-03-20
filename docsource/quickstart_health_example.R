
library(jobmonr)

# Create a workflow
username <- Sys.getenv("USER")

# Parameters for the workflow
root_data_dir <- paste0("/home/", username, "/quickstart/data")
location_set_id <- as.integer(5)
location_ids <- seq(from=0, to=location_set_id)

# Create a tool
my_tool <- tool(name="r_quickstart_tool_R")

# Set the tool compute resources
jobmonr::set_default_tool_resources(
    tool=my_tool,
    default_cluster_name="slurm",
    resources=list(
        "cores"=1,
        "queue"="all.q",
        "runtime"="2m",
        "memory"="1G",
        "project"="proj_scicomp"
    )
)

# Bind a workflow to the tool
wf <- workflow(my_tool, name=paste0("quickstart_workflow_", Sys.Date()))

# Create template to run our script
data_prep_template <- task_template(tool=my_tool,
                        template_name="quickstart_data_prep_template",
                        command_template=paste(
                          Sys.getenv("RETICULATE_PYTHON"),
                            "/code_dir/docsource/quickstart_tasks/data_prep.py",
                            "--location_set_id {location_set_id}",
                            "--root_data_dir {root_data_dir}",
                            "--log_level {log_level}",
                            sep=" "),
                        task_args=list("location_set_id", "root_data_dir"),
                        op_args=list("log_level"))


# Optional: default resources can be updated at the task or task template level
jobmonr::set_default_template_resources(
    task_template=data_prep_template,
    default_cluster_name="slurm",
        resources=list(
          "queue"="all.q",
          "constraints"="archive"
        )
    )

parallel_by_location_template <- task_template(tool=my_tool,
                        template_name="quickstart_location_template",
                        command_template=paste(
                          Sys.getenv("RETICULATE_PYTHON"),
                            "/code_dir/docsource/quickstart_tasks/one_location.py",
                            "--location_id {location_id}",
                            "--root_data_dir {root_data_dir}",
                            "--log_level {log_level}",
                            sep=" "),
                        node_args=list("location_id"),
                        task_args=list("root_data_dir"),
                        op_args=list("log_level"))


summarization_template <- task_template(tool=my_tool,
                        template_name="quickstart_summarization_template",
                        command_template=paste(
                          Sys.getenv("RETICULATE_PYTHON"),
                            "/code_dir/docsource/quickstart_tasks/summarization.py",
                            "--root_data_dir {root_data_dir}",
                            "--log_level {log_level}",
                            sep=" "),
                        task_args=list("root_data_dir"),
                        op_args=list("log_level"))

# Now create the tasks
data_prep_task <- task(task_template=data_prep_template,
            name="data_prep_task",
            root_data_dir=root_data_dir,
            location_set_id=location_set_id,
            log_level="DEBUG")

# Add tasks to the workflow
wf <- add_tasks(wf, list(data_prep_task))

# Create all the location tasks in one call
location_tasks <- array_tasks(
            task_template=parallel_by_location_template,
            name="location_task_",
            upstream_tasks=list(data_prep_task),
            location_id=location_ids,
            root_data_dir=root_data_dir,
            log_level="DEBUG")
wf <- add_tasks(wf, location_tasks)

summarization_task <- task(task_template=summarization_template,
            name="summarization_task",
            upstream_tasks=location_tasks,
            root_data_dir=root_data_dir,
            log_level="DEBUG")
wf <- add_tasks(wf, list(summarization_task))


# Run it
wfr <- run(
    workflow=wf,
    resume=FALSE,
    seconds_until_timeout=7200)
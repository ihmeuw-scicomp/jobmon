***************
R Client Guide
***************

The Jobmon R client (``jobmonr``) allows you to create and run Jobmon workflows 
entirely from R or RStudio. It wraps the Python client using ``reticulate``, 
providing native R functions for all core Jobmon operations.

.. note::
   For general Jobmon concepts (Tools, Workflows, Tasks, etc.), see 
   :doc:`core_concepts`. This guide focuses on R-specific usage patterns.

Installation
============

The R client is distributed separately from the Python client.

.. code-block:: r

   # Install from source (contact your administrator for the package)
   install.packages("jobmonr", repos = NULL, type = "source")

Requirements
------------

- R 4.0.0 or higher
- The ``reticulate`` package
- A Python environment with ``jobmon_client`` installed

Python Environment
------------------

By default, ``jobmonr`` uses a centrally managed Python environment. To use 
your own:

.. code-block:: r

   # Set BEFORE loading jobmonr
   Sys.setenv(RETICULATE_PYTHON = "/path/to/your/python")
   library(jobmonr)

.. warning::
   ``reticulate`` can only use one Python environment per R session. If you use 
   other ``reticulate``-based packages, you may need to create a unified 
   environment with all required packages.

Quick Start
===========

Here's a minimal example to create and run a workflow:

.. code-block:: r

   library(jobmonr)
   
   # 1. Create a Tool
   my_tool <- jobmonr::tool(name = "my_r_tool")
   
   # 2. Set default resources
   my_tool <- jobmonr::set_default_tool_resources(
     my_tool,
     default_cluster_name = "slurm",
     resources = list(
       memory = "2G",
       runtime = "30m",
       cores = 1L,
       queue = "all.q"
     )
   )
   
   # 3. Create a Workflow
   wf <- jobmonr::workflow(
     tool = my_tool,
     workflow_args = paste0("my_workflow_", Sys.Date())
   )
   
   # 4. Create a Task Template
   tt <- jobmonr::task_template(
     tool = my_tool,
     template_name = "process_data",
     command_template = "{rshell} {script} --id {data_id}",
     node_args = list("data_id"),
     op_args = list("rshell", "script")
   )
   
   # 5. Create Tasks
   tasks <- list()
   for (id in 1:10) {
     tasks[[id]] <- jobmonr::task(
       task_template = tt,
       name = paste0("process_", id),
       data_id = id,
       rshell = "Rscript",
       script = "process.R"
     )
   }
   
   # 6. Add tasks and run
   wf <- jobmonr::add_tasks(wf, tasks)
   status <- jobmonr::run(wf, resume = FALSE, seconds_until_timeout = 3600)
   
   if (status != "D") {
     stop("Workflow failed!")
   }

API Reference
=============

Tool Functions
--------------

tool()
^^^^^^

Create a Jobmon Tool to associate with your workflows.

.. code-block:: r

   tool(name = "unknown", active_tool_version_id = "latest")

**Arguments:**

- ``name`` (character): Name for your tool (e.g., "codem", "codcorrect")
- ``active_tool_version_id``: Tool version to use. Default "latest"

**Returns:** A Tool object reference

**Example:**

.. code-block:: r

   my_tool <- jobmonr::tool(name = "my_analysis")

create_new_tool_version()
^^^^^^^^^^^^^^^^^^^^^^^^^

Create a new version of an existing tool.

.. code-block:: r

   create_new_tool_version(tool)

**Arguments:**

- ``tool``: A Tool object

**Returns:** The new tool_version_id

**Example:**

.. code-block:: r

   new_version <- jobmonr::create_new_tool_version(my_tool)

set_default_tool_resources()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set default compute resources for all workflows/tasks created from this tool.

.. code-block:: r

   set_default_tool_resources(
     tool,
     default_cluster_name,
     resources = NULL,
     path_to_yaml = NULL
   )

**Arguments:**

- ``tool``: A Tool object
- ``default_cluster_name`` (character): Cluster name (e.g., "slurm")
- ``resources`` (named list): Resource key-value pairs
- ``path_to_yaml`` (character): Path to YAML file with resources

**Note:** Provide either ``resources`` OR ``path_to_yaml``, not both.

**Example:**

.. code-block:: r

   # From a list
   my_tool <- jobmonr::set_default_tool_resources(
     my_tool,
     default_cluster_name = "slurm",
     resources = list(
       memory = "10G",
       runtime = "2h",
       cores = 2L,
       queue = "all.q"
     )
   )
   
   # From YAML
   my_tool <- jobmonr::set_default_tool_resources(
     my_tool,
     default_cluster_name = "slurm",
     path_to_yaml = "/path/to/resources.yaml"
   )

Workflow Functions
------------------

workflow()
^^^^^^^^^^

Create a workflow from a tool.

.. code-block:: r

   workflow(tool, workflow_args = "", workflow_attributes = list(), ...)

**Arguments:**

- ``tool``: A Tool object
- ``workflow_args`` (character): Unique identifier for this workflow
- ``workflow_attributes`` (list): Custom attributes to track
- ``...``: Additional arguments (e.g., ``max_concurrently_running``)

**Returns:** A Workflow object reference

**Example:**

.. code-block:: r

   wf <- jobmonr::workflow(
     tool = my_tool,
     workflow_args = "version_2024_q4",
     workflow_attributes = list(
       release_id = 123,
       description = "Q4 2024 analysis"
     ),
     max_concurrently_running = 500
   )

add_tasks()
^^^^^^^^^^^

Add tasks to a workflow.

.. code-block:: r

   add_tasks(workflow, tasks)

**Arguments:**

- ``workflow``: A Workflow object
- ``tasks`` (list): List of Task objects

**Returns:** The updated Workflow object

**Example:**

.. code-block:: r
   
   wf <- jobmonr::add_tasks(wf, my_tasks)

add_arrays()
^^^^^^^^^^^^

Add arrays (and their tasks) to a workflow.

.. code-block:: r

   add_arrays(workflow, arrays)

run()
^^^^^

Execute a workflow.

.. code-block:: r

   run(workflow, resume, seconds_until_timeout, ...)

**Arguments:**

- ``workflow``: A Workflow object
- ``resume`` (logical): Whether to resume an existing workflow
- ``seconds_until_timeout`` (numeric): Maximum wait time in seconds
- ``...``: Additional arguments

**Returns:** Workflow run status ("D" for done, "E" for error, etc.)

**Example:**

.. code-block:: r

   status <- jobmonr::run(
     wf,
     resume = FALSE,
     seconds_until_timeout = 36000  # 10 hours
   )
   
   if (status != "D") {
     stop(paste("Workflow failed with status:", status))
   }

set_default_workflow_resources()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set default resources for all tasks in a workflow.

.. code-block:: r

   set_default_workflow_resources(
     workflow,
     default_cluster_name,
     resources = NULL,
     path_to_yaml = NULL
   )

Task Template Functions
-----------------------

task_template()
^^^^^^^^^^^^^^^

Create a task template for generating similar tasks.

.. code-block:: r

   task_template(
     tool,
     template_name,
     command_template,
     node_args = list(),
     task_args = list(),
     op_args = list()
   )

**Arguments:**

- ``tool``: A Tool object
- ``template_name`` (character): Name for this template
- ``command_template`` (character): Python-style format string with placeholders
- ``node_args`` (list): Arguments for parallelization (e.g., location_id)
- ``task_args`` (list): Arguments for data flow (e.g., version_id)
- ``op_args`` (list): Operational arguments (e.g., script paths)

**Returns:** A TaskTemplate object reference

**Example:**

.. code-block:: r

   model_template <- jobmonr::task_template(
     tool = my_tool,
     template_name = "model_by_location",
     command_template = "{rshell} {script} --loc {location_id} --year {year_id}",
     node_args = list("location_id"),
     task_args = list("year_id"),
     op_args = list("rshell", "script")
   )

set_default_template_resources()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set default resources for tasks created from this template.

.. code-block:: r

   set_default_template_resources(
     task_template,
     default_cluster_name,
     resources = NULL,
     path_to_yaml = NULL
   )

**Example:**

.. code-block:: r

   model_template <- jobmonr::set_default_template_resources(
     model_template,
     default_cluster_name = "slurm",
     resources = list(
       memory = "20G",
       runtime = "4h",
       cores = 4L
     )
   )

Task Functions
--------------

task()
^^^^^^

Create a single task from a template.

.. code-block:: r

   task(
     task_template,
     name,
     compute_resources = NULL,
     upstream_tasks = list(),
     task_attributes = list(),
     max_attempts = 3,
     ...
   )

**Arguments:**

- ``task_template``: A TaskTemplate object
- ``name`` (character): Unique name for this task
- ``compute_resources`` (named list): Override template resources
- ``upstream_tasks`` (list): Tasks this task depends on
- ``task_attributes`` (list): Custom attributes
- ``max_attempts`` (integer): Number of retries (default 3)
- ``...``: Values for node_args, task_args, and op_args

**Returns:** A Task object reference

**Example:**

.. code-block:: r

   my_task <- jobmonr::task(
     task_template = model_template,
     name = "model_loc_1_year_2024",
     upstream_tasks = list(prep_task),
     max_attempts = 5,
     location_id = 1,
     year_id = 2024,
     rshell = "Rscript",
     script = "model.R"
   )

array_tasks()
^^^^^^^^^^^^^

Create multiple tasks at once by providing lists of node_arg values.

.. code-block:: r

   array_tasks(
     task_template,
     upstream_tasks = list(),
     max_attempts = 3,
     compute_resources = NULL,
     cluster_name = "",
     ...
   )

**Arguments:**

- ``task_template``: A TaskTemplate object
- ``upstream_tasks`` (list): Tasks all array tasks depend on
- ``max_attempts`` (integer): Retries per task
- ``compute_resources`` (named list): Override resources
- ``cluster_name`` (character): Override cluster
- ``...``: node/task/op args (node_args as lists for parallelization)

**Returns:** A list of Task objects

**Example:**

.. code-block:: r

   # Create tasks for 3 locations × 2 sexes = 6 tasks
   tasks <- jobmonr::array_tasks(
     task_template = model_template,
     upstream_tasks = list(prep_task),
     location_id = list(1L, 2L, 3L),  # node_arg as list
     sex_id = list(1L, 2L),           # node_arg as list
     year_id = 2024,                  # task_arg as single value
     rshell = "Rscript",
     script = "model.R"
   )

get_tasks_by_node_args()
^^^^^^^^^^^^^^^^^^^^^^^^

Retrieve specific tasks from an array by their node argument values.

.. code-block:: r

   get_tasks_by_node_args(workflow, task_template_name, ...)

**Example:**

.. code-block:: r

   # Get all tasks for location 1
   loc1_tasks <- jobmonr::get_tasks_by_node_args(wf, "model_template", location_id = 1)
   
   # Get the specific task for location 1, sex 1
   specific_task <- jobmonr::get_tasks_by_node_args(
     wf, "model_template", 
     location_id = 1, 
     sex_id = 1
   )

Utility Functions
-----------------

jobmon_help()
^^^^^^^^^^^^^

Display the Python docstring for a Jobmon object.

.. code-block:: r

   jobmon_help(object)

**Arguments:**

- ``object`` (character): One of "tool", "task_template", "task", "workflow"

**Example:**

.. code-block:: r

   jobmonr::jobmon_help("workflow")

YAML Configuration
==================

You can define compute resources in YAML files for easier management.

Tool Resources YAML
-------------------

.. code-block:: yaml

   tool_resources:
     slurm:
       cores: 1
       memory: "2G"
       runtime: "1h"
       queue: "all.q"

Task Template Resources YAML
----------------------------

.. code-block:: yaml

   task_template_resources:
     my_template_name:
       slurm:
         cores: 4
         memory: "20G"
         runtime: "6h"
         queue: "long.q"

Complete Example
================

Here's a full example of a fork-join workflow pattern:

.. code-block:: r

   library(jobmonr)
   
   # ============================================================
   # Configuration
   # ============================================================
   VERSION_ID <- 42
   LOCATIONS <- c(1, 2, 3, 4, 5)
   DRAWS <- 1:100
   
   # ============================================================
   # Setup Tool
   # ============================================================
   my_tool <- jobmonr::tool(name = "fork_join_example")
   my_tool <- jobmonr::set_default_tool_resources(
     my_tool,
     default_cluster_name = "slurm",
     resources = list(
       memory = "5G",
       runtime = "1h",
       cores = 1L,
       queue = "all.q",
       project = "proj_my_project"
     )
   )
   
   # ============================================================
   # Create Workflow
   # ============================================================
   wf <- jobmonr::workflow(
     tool = my_tool,
     workflow_args = paste0("v", VERSION_ID, "_", Sys.Date()),
     workflow_attributes = list(version_id = VERSION_ID)
   )
   
   # ============================================================
   # Define Task Templates
   # ============================================================
   
   # Parallel modeling tasks
   model_tt <- jobmonr::task_template(
     tool = my_tool,
     template_name = "model",
     command_template = "Rscript model.R --loc {location_id} --draw {draw_id} --version {version_id}",
     node_args = list("location_id", "draw_id"),
     task_args = list("version_id"),
     op_args = list()
   )
   
   # Override resources for compute-intensive modeling
   model_tt <- jobmonr::set_default_template_resources(
     model_tt,
     default_cluster_name = "slurm",
     resources = list(memory = "10G", runtime = "2h")
   )
   
   # Summary task
   summary_tt <- jobmonr::task_template(
     tool = my_tool,
     template_name = "summarize",
     command_template = "Rscript summarize.R --version {version_id}",
     node_args = list(),
     task_args = list("version_id"),
     op_args = list()
   )
   
   summary_tt <- jobmonr::set_default_template_resources(
     summary_tt,
     default_cluster_name = "slurm",
     resources = list(memory = "50G", runtime = "4h", cores = 10L)
   )
   
   # ============================================================
   # Create Tasks
   # ============================================================
   
   # Create parallel modeling tasks using array_tasks
   model_tasks <- jobmonr::array_tasks(
     task_template = model_tt,
     location_id = as.list(LOCATIONS),
     draw_id = as.list(DRAWS),
     version_id = VERSION_ID
   )
   
   # Create summary task that waits for all modeling tasks
   summary_task <- jobmonr::task(
     task_template = summary_tt,
     name = "summarize_all",
     upstream_tasks = model_tasks,
     version_id = VERSION_ID
   )
   
   # ============================================================
   # Run Workflow
   # ============================================================
   
   wf <- jobmonr::add_tasks(wf, model_tasks)
   wf <- jobmonr::add_tasks(wf, list(summary_task))
   
   cat("Starting workflow with", length(model_tasks) + 1, "tasks\n")
   
   status <- jobmonr::run(
     wf,
     resume = FALSE,
     seconds_until_timeout = 36000  # 10 hours
   )
   
   if (status == "D") {
     cat("Workflow completed successfully!\n")
   } else {
     stop(paste("Workflow failed with status:", status))
   }

Common Patterns
===============

Resuming a Failed Workflow
--------------------------

.. code-block:: r

   # Use the same workflow_args to resume
   wf <- jobmonr::workflow(
     tool = my_tool,
     workflow_args = "my_workflow_v1"  # Same as original
   )
   # ... add same tasks ...
   
   status <- jobmonr::run(wf, resume = TRUE, seconds_until_timeout = 36000)

Setting Per-Task Resources
--------------------------

.. code-block:: r

   # Override template resources for specific tasks
   big_task <- jobmonr::task(
     task_template = model_tt,
     name = "big_location",
     compute_resources = list(
       memory = "100G",
       runtime = "24h"
     ),
     location_id = 999,
     version_id = 1
   )

Troubleshooting
===============

Python Environment Issues
-------------------------

If you see errors about missing Python modules:

.. code-block:: r

   # Check which Python is being used
   reticulate::py_config()
   
   # Verify jobmon is installed
   reticulate::py_module_available("jobmon.client")

Version Mismatch
----------------

If you see version mismatch errors, ensure ``jobmonr`` and ``jobmon_client`` 
versions are compatible (major.minor versions should match).

Connection Errors
-----------------

Ensure you're on the correct network and the Jobmon server is accessible.

See Also
========

- :doc:`core_concepts` - Understanding Jobmon concepts
- :doc:`workflows` - Workflow patterns
- :doc:`compute_resources` - Resource management
- :doc:`/advanced/troubleshooting` - Debugging issues


*************
CLI Reference
*************

The Jobmon command-line interface provides tools for monitoring workflows, 
checking statuses, and performing self-service operations.

.. note::
   This is a summary reference. For detailed examples, see 
   :doc:`/advanced/monitoring_debugging` and :doc:`/advanced/advanced_usage`.

Usage
=====

All commands follow the pattern:

.. code-block:: bash

   jobmon <command> [options]

Status Commands
===============

workflow_status
---------------

View workflow status summary.

.. code-block:: bash

   jobmon workflow_status -u <username>
   jobmon workflow_status -w <workflow_id>
   jobmon workflow_status -w <id1> <id2>  # Multiple workflows

Options:

- ``-u, --user``: Filter by username
- ``-w, --workflow_id``: Specific workflow ID(s)
- ``-n, --json``: Output as JSON

workflow_tasks
--------------

View tasks in a workflow.

.. code-block:: bash

   jobmon workflow_tasks -w <workflow_id>
   jobmon workflow_tasks -w <workflow_id> -s PENDING RUNNING

Options:

- ``-w, --workflow_id``: Workflow ID (required)
- ``-s, --status``: Filter by status (PENDING, RUNNING, DONE, FATAL)
- ``-n, --json``: Output as JSON

task_status
-----------

View task instance details.

.. code-block:: bash

   jobmon task_status -t <task_id>
   jobmon task_status -t <id1> <id2> -s done

Options:

- ``-t, --task_id``: Task ID(s) (required)
- ``-s, --status``: Filter by status
- ``-n, --json``: Output as JSON

task_dependencies
-----------------

View task upstream/downstream dependencies.

.. code-block:: bash

   jobmon task_dependencies -t <task_id>

Options:

- ``-t, --task_id``: Task ID (required)

get_filepaths
-------------

Get log file paths for tasks.

.. code-block:: bash

   jobmon get_filepaths -w <workflow_id>
   jobmon get_filepaths -w <workflow_id> -l 20

Options:

- ``-w, --workflow_id``: Workflow ID
- ``-a, --array_name``: Filter by array name
- ``-j, --job_name``: Filter by job name
- ``-l, --limit``: Number of results (default: 5)

Self-Service Commands
=====================

workflow_resume
---------------

Resume a failed workflow.

.. code-block:: bash

   jobmon workflow_resume -w <workflow_id> -c <cluster_name>
   jobmon workflow_resume -w 12345 -c slurm --reset-running-jobs

Options:

- ``-w, --workflow_id``: Workflow ID (required)
- ``-c, --cluster_name``: Cluster name (required)
- ``--reset-running-jobs``: Kill running jobs (cold resume)
- ``-t, --timeout``: Wait timeout for resumable state (default: 180s)
- ``--seconds-until-timeout``: Execution timeout (default: 36000s)

workflow_reset
--------------

Reset a workflow to REGISTERED state.

.. code-block:: bash

   jobmon workflow_reset -w <workflow_id>

Options:

- ``-w, --workflow_id``: Workflow ID (required)

.. note::
   Only works on workflows in ERROR state. Must be the workflow owner.

update_task_status
------------------

Manually update task statuses.

.. code-block:: bash

   # Mark tasks as DONE
   jobmon update_task_status -t <task_ids> -w <workflow_id> -s D
   
   # Reset tasks to REGISTERED (will rerun)
   jobmon update_task_status -t <task_ids> -w <workflow_id> -s G

Options:

- ``-t, --task_ids``: Task ID(s) (required)
- ``-w, --workflow_id``: Workflow ID (required)
- ``-s, --status``: New status (D=DONE, G=REGISTERED)

concurrency_limit
-----------------

Dynamically adjust concurrent task limit.

.. code-block:: bash

   jobmon concurrency_limit --workflow_id <id> --max_tasks <limit>

Options:

- ``--workflow_id``: Workflow ID (required)
- ``--max_tasks``: Maximum concurrent tasks (required)

Utility Commands
================

task_template_resources
-----------------------

Generate resource YAML from historical usage.

.. code-block:: bash

   jobmon task_template_resources -w <workflow_id>
   jobmon task_template_resources -w <workflow_id> -p f ~/resources.yaml

Options:

- ``-w, --workflow_id``: Workflow ID
- ``-t, --task_template_version_id``: Template version ID
- ``-p, --print_file``: Output to file
- ``-a, --node_args``: Filter by node args (JSON)

update_config
-------------

Update local configuration.

.. code-block:: bash

   jobmon update_config <key> <value>
   jobmon update_config http.retries_attempts 15
   jobmon update_config distributor.poll_interval 5

Options:

- ``key``: Configuration key (dot notation)
- ``value``: New value
- ``--config-file``: Specific config file to update

See Also
========

- :doc:`monitoring` - Monitoring overview
- :doc:`/advanced/monitoring_debugging` - Detailed monitoring guide
- :doc:`/advanced/advanced_usage` - Advanced CLI usage


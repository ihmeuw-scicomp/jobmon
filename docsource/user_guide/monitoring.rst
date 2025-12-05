**********
Monitoring
**********

Jobmon provides multiple ways to monitor your workflows: a graphical interface, 
command-line tools, and programmatic access.

.. note::
   This page summarizes monitoring options. For detailed database queries 
   and status information, see :doc:`/monitoring_debugging`.

Graphical User Interface (GUI)
==============================

The Jobmon GUI provides visual workflow monitoring:

- View workflow status and progress
- Drill down into task details
- See error messages and logs
- Check resource usage

.. note::
   GUI URL varies by installation. Check with your administrator.

Command Line Interface (CLI)
============================

Workflow Status
---------------

Check overall workflow status:

.. code-block:: bash

   # By user
   jobmon workflow_status -u $USER
   
   # By workflow ID
   jobmon workflow_status -w 12345
   
   # Multiple workflows
   jobmon workflow_status -w 12345 67890

Task Status
-----------

See tasks in a workflow:

.. code-block:: bash

   # All tasks in workflow
   jobmon workflow_tasks -w 12345
   
   # Filter by status
   jobmon workflow_tasks -w 12345 -s PENDING RUNNING
   jobmon workflow_tasks -w 12345 -s FATAL

Task Instance Details
---------------------

Check specific task execution:

.. code-block:: bash

   # Task instances for a task
   jobmon task_status -t 67890
   
   # Filter by status
   jobmon task_status -t 67890 -s ERROR

Task Dependencies
-----------------

See what a task depends on:

.. code-block:: bash

   jobmon task_dependencies -t 67890

Log File Locations
------------------

Find stdout/stderr files:

.. code-block:: bash

   jobmon get_filepaths -w 12345

JSON Output
-----------

Get machine-readable output:

.. code-block:: bash

   jobmon workflow_status -w 12345 -n

Programmatic Access
===================

Workflow Results
----------------

The ``workflow.run()`` method returns detailed results:

.. code-block:: python

   result = workflow.run()
   
   print(f"Status: {result.final_status}")
   print(f"Done: {result.done_count}/{result.total_tasks}")
   print(f"Failed: {result.failed_count}")
   print(f"Elapsed: {result.elapsed_time:.1f}s")
   
   # Get failed task IDs
   for task_id in result.failed_task_ids:
       print(f"Failed: {task_id}")

Error Logs
----------

Get errors for a workflow:

.. code-block:: python

   errors = workflow.get_errors(limit=100)
   for task_id, error_msg in errors.items():
       print(f"Task {task_id}: {error_msg}")

Resource Usage
--------------

Check actual resource consumption:

.. code-block:: python

   # Per task
   usage = task.resource_usage()
   
   # Aggregated per template
   stats = template.resource_usage(workflows=[workflow_id])

Task Statuses
=============

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Status
     - Description
   * - REGISTERED
     - Task is in the database, waiting for dependencies
   * - QUEUED
     - Dependencies complete, waiting to be scheduled
   * - RUNNING
     - Task is currently executing
   * - DONE
     - Task completed successfully
   * - ERROR_RECOVERABLE
     - Task failed but has retries remaining
   * - ERROR_FATAL
     - Task failed and exhausted all retries

For complete status documentation, see :doc:`/monitoring_debugging`.

See Also
========

- :doc:`/monitoring_debugging` - Complete monitoring reference
- :doc:`/advanced/troubleshooting` - Debugging failed workflows
- :doc:`cli_reference` - Full CLI command reference


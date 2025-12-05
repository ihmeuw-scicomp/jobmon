***************
Troubleshooting
***************

This guide helps you diagnose and resolve common issues with Jobmon workflows.

Quick Diagnostics
=================

When something goes wrong, start here:

1. **Check workflow status** in the GUI or CLI
2. **Look at task errors** in the Task Details page
3. **Review log files** (stdout/stderr paths)
4. **Check the Jobmon server** is accessible

Common Errors
=============

DistributorNotAlive Error
-------------------------

**Symptom**: ``DistributorNotAlive`` exception when running a workflow.

**Cause**: Usually occurs when running from a login node instead of a submit node.

**Solution**:

.. code-block:: bash

   # Start an interactive session first
   srun --pty bash
   
   # Then run your workflow
   python my_workflow.py

NO_DISTRIBUTOR_ID Error
-----------------------

**Symptom**: TaskInstance shows ``NO_DISTRIBUTOR_ID`` status.

**Cause**: Jobmon couldn't submit the job to the cluster. Common causes:

- Insufficient permissions for the partition/queue
- Resource requests exceed queue limits
- Invalid project code

**Solution**:

1. Check the error details in the GUI (Task Details → TaskInstances → Standard Error)
2. Verify your queue/partition access
3. Ensure resource requests are within limits
4. Check your project code is valid

Connection Refused
------------------

**Symptom**: ``ConnectionRefusedError`` or timeout when running workflow.

**Cause**: Can't connect to the Jobmon server.

**Solution**:

1. Verify network connectivity (VPN if required)
2. Check server URL in your configuration:

   .. code-block:: bash

      cat ~/.jobmon.yaml

3. Test the server directly:

   .. code-block:: bash

      curl http://your-jobmon-server:5000/health

Workflow Already Exists
-----------------------

**Symptom**: Error about workflow already existing when trying to run.

**Cause**: Trying to create a workflow with the same ``workflow_args`` as an existing one.

**Solution**:

- To resume the existing workflow: ``workflow.run(resume=True)``
- To create a new workflow: Use different ``workflow_args``

Resource Errors
===============

Out of Memory (OOM)
-------------------

**Symptom**: Task fails with ``RESOURCE_ERROR`` status, memory-related error in logs.

**Solution**:

1. Check actual memory usage in the GUI
2. Increase memory request:

   .. code-block:: python

      compute_resources={"memory": "20G", ...}

3. Enable automatic resource scaling:

   .. code-block:: python

      task = template.create_task(
          max_attempts=3,  # Will retry with more resources
          ...
      )

Timeout / Runtime Exceeded
--------------------------

**Symptom**: Task killed for exceeding runtime.

**Solution**:

1. Increase runtime:

   .. code-block:: python

      compute_resources={"runtime": "4h", ...}

2. Use a queue with longer limits
3. Or set a fallback queue:

   .. code-block:: python

      task = template.create_task(
          fallback_queues=["long.q"],
          ...
      )

Debugging Workflows
===================

Using the GUI
-------------

The Jobmon GUI is the fastest way to investigate issues:

1. Find your workflow by name or ID
2. Click to see task breakdown by status
3. Click a failed task to see:
   - Error messages
   - Resource usage
   - stdout/stderr file paths
   - Retry history

Using the CLI
-------------

.. code-block:: bash

   # Check workflow status
   jobmon workflow_status -w <workflow_id>
   
   # See task details
   jobmon workflow_tasks -w <workflow_id> -s FATAL
   
   # Check specific task
   jobmon task_status -t <task_id>
   
   # See task dependencies
   jobmon task_dependencies -t <task_id>

Reading Log Files
-----------------

Find log file paths:

.. code-block:: bash

   jobmon get_filepaths -w <workflow_id>

Or check the Task Details page in the GUI.

Common Patterns
===============

Tasks Stuck in PENDING
----------------------

**Possible causes**:

1. Upstream tasks haven't completed
2. Cluster is busy (check queue)
3. Concurrency limit reached

**Check**:

.. code-block:: bash

   jobmon task_dependencies -t <task_id>

Tasks Fail Immediately
----------------------

**Possible causes**:

1. Command not found (check PATH)
2. Missing dependencies (conda environment)
3. File not found errors

**Debug**: Run the command manually to see the actual error.

Workflow Hangs
--------------

**Possible causes**:

1. Network issues to Jobmon server
2. All tasks waiting on failed upstream
3. Workflow timeout reached

**Check**: Look at the workflow status in the GUI.

Getting More Help
=================

If you're still stuck:

1. Check the full error message and stack trace
2. Search existing issues: https://github.com/ihmeuw-scicomp/jobmon/issues
3. Ask for help with:
   - Workflow ID
   - Error message
   - What you were trying to do
   - Relevant code snippets

See Also
========

- :doc:`/user_guide/monitoring` - Monitoring workflows
- :doc:`/user_guide/cli_reference` - CLI command reference


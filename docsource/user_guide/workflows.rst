*********
Workflows
*********

A Workflow is the top-level container for your computational pipeline. It 
encompasses all your tasks and their dependencies.

.. note::
   This page is being developed. For now, see :doc:`core_concepts` for 
   workflow basics and :doc:`/advanced/advanced_usage` for advanced patterns.

Creating Workflows
==================

Basic workflow creation:

.. code-block:: python

   from jobmon.client.tool import Tool
   
   tool = Tool(name="my_application")
   
   workflow = tool.create_workflow(
       name="my_workflow",
       workflow_args="version_1"
   )

Workflow Arguments
------------------

The ``workflow_args`` parameter uniquely identifies your workflow:

- Same args = same workflow (can resume)
- Different args = different workflow (fresh start)

.. code-block:: python

   # These are two different workflows
   wf1 = tool.create_workflow(name="pipeline", workflow_args="v1")
   wf2 = tool.create_workflow(name="pipeline", workflow_args="v2")
   
   # This resumes wf1 if it exists
   wf1_resumed = tool.create_workflow(name="pipeline", workflow_args="v1")

Workflow Attributes
-------------------

Track additional metadata with your workflow:

.. code-block:: python

   workflow = tool.create_workflow(
       name="my_workflow",
       workflow_args="v1",
       workflow_attributes={
           "release_id": 123,
           "description": "Production run for Q4"
       }
   )

Running Workflows
=================

Basic run:

.. code-block:: python

   result = workflow.run()

With options:

.. code-block:: python

   status = workflow.run(
       resume=True,               # Resume if workflow exists
       fail_fast=True,            # Stop on first failure
       seconds_until_timeout=7200 # 2 hour timeout
   )

Checking Results
----------------

The ``run()`` method returns a status string:

.. code-block:: python

   from jobmon.core.constants import WorkflowRunStatus
   
   status = workflow.run()
   
   if status == WorkflowRunStatus.DONE:
       print("Workflow completed successfully!")
   elif status == WorkflowRunStatus.ERROR:
       print("Workflow failed - check task statuses")
   
   # Use CLI or GUI to inspect failures:
   # jobmon workflow_tasks -w <workflow_id> -s FATAL

Resuming Workflows
==================

Workflows can be resumed after failures:

By Recreating
-------------

.. code-block:: python

   # Use the same workflow_args
   workflow = tool.create_workflow(
       name="my_workflow", 
       workflow_args="v1"
   )
   workflow.add_tasks([...])
   workflow.run(resume=True)

By ID (CLI)
-----------

.. code-block:: bash

   jobmon workflow_resume -w <workflow_id> -c slurm

For more details, see :ref:`jobmon-resume-label` in the advanced usage guide.

Concurrency Control
===================

Limit concurrent tasks:

.. code-block:: python

   workflow = tool.create_workflow(
       name="my_workflow",
       max_concurrently_running=500
   )

See Also
========

- :doc:`core_concepts` - Fundamental Jobmon concepts
- :doc:`tasks` - Creating and managing tasks
- :doc:`/advanced/advanced_usage` - Advanced workflow patterns


*****
Tasks
*****

Tasks are the fundamental unit of work in Jobmon. Each task represents a 
single command to be executed.

.. note::
   This page is being developed. For now, see :doc:`core_concepts` for 
   task basics and :doc:`/advanced/advanced_usage` for advanced patterns.

Task Templates
==============

Before creating tasks, you define a TaskTemplate:

.. code-block:: python

   template = tool.get_task_template(
       template_name="process_data",
       command_template="python process.py --input {input_file} --output {output_file}",
       node_args=["input_file"],      # Unique per task
       task_args=["output_file"],     # Unique per workflow run
       op_args=[],                    # Operational (script paths, etc.)
   )

Understanding Arguments
-----------------------

- **node_args**: Make each task unique within a workflow. Typically parallelization 
  dimensions like ``location_id``, ``year``.
  
- **task_args**: Change between workflow runs but don't affect task uniqueness. 
  Typically data versions or release IDs.
  
- **op_args**: Operational arguments that don't affect task identity. 
  Typically script paths or log verbosity.

Creating Tasks
==============

Create tasks from templates:

.. code-block:: python

   task = template.create_task(
       name="process_location_1",
       input_file="/data/loc_1.csv",
       output_file="/output/loc_1.csv",
       compute_resources={
           "cores": 2,
           "memory": "8G",
           "runtime": "1h",
       }
   )

Task Naming
-----------

Task names should be descriptive and unique within a workflow:

.. code-block:: python

   # Good: descriptive and unique
   task = template.create_task(name=f"process_loc_{location_id}_year_{year}", ...)
   
   # Bad: not unique
   task = template.create_task(name="process", ...)

Dependencies
============

Tasks can depend on other tasks:

.. code-block:: python

   # task2 runs after task1 completes
   task2.add_upstream(task1)

Multiple dependencies:

.. code-block:: python

   # aggregate_task waits for all process_tasks
   for task in process_tasks:
       aggregate_task.add_upstream(task)

Or set during task creation:

.. code-block:: python

   task2 = template.create_task(
       name="downstream",
       upstream_tasks=[task1, task3],
       ...
   )

Task Attributes
===============

Track custom metadata:

.. code-block:: python

   task = template.create_task(
       name="my_task",
       task_attributes={
           "location_id": 1,
           "model_version": "2.1"
       },
       ...
   )

Retries
=======

Configure retry behavior:

.. code-block:: python

   task = template.create_task(
       name="my_task",
       max_attempts=3,  # Retry up to 3 times
       ...
   )

For resource-related failures, Jobmon automatically scales resources before retrying.

See :doc:`compute_resources` for details on resource scaling.

Checking Task Status
====================

After running a workflow:

.. code-block:: python

   # Get resource usage for a completed task
   usage = task.resource_usage()
   print(f"Memory used: {usage['memory']} bytes")
   print(f"Runtime: {usage['runtime']} seconds")
   print(f"Attempts: {usage['num_attempts']}")

See Also
========

- :doc:`core_concepts` - Fundamental Jobmon concepts
- :doc:`compute_resources` - Resource allocation and retries
- :doc:`workflows` - Workflow management


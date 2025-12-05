**********
Quickstart
**********

This guide will have you running your first Jobmon workflow in minutes.

Prerequisites
=============

- Python 3.9+ or R installed
- Jobmon client installed (see :doc:`installation`)
- Access to a Jobmon server (or local Docker setup)

Your First Workflow
===================

A Jobmon workflow consists of:

1. A **Tool** - identifies your application
2. A **Workflow** - contains your tasks and their dependencies  
3. **TaskTemplates** - patterns for creating tasks
4. **Tasks** - the actual work to be done

Let's create a simple workflow that runs a few commands.

.. tabs::

   .. group-tab:: Python

      .. code-block:: python

         from jobmon.client.tool import Tool
         
         # 1. Create a Tool (identifies your application)
         tool = Tool(name="quickstart_tutorial")
         tool.set_default_cluster_name("sequential")  # Run locally for this demo
         
         # 2. Create a TaskTemplate (pattern for tasks)
         template = tool.get_task_template(
             template_name="echo_template",
             command_template="echo {message}",
             node_args=["message"],
         )
         
         # 3. Create a Workflow
         workflow = tool.create_workflow(
             name="my_first_workflow",
             workflow_args="quickstart_v1"
         )
         
         # 4. Create Tasks from the template
         task1 = template.create_task(
             name="say_hello",
             message="Hello from Jobmon!"
         )
         
         task2 = template.create_task(
             name="say_goodbye",
             message="Goodbye from Jobmon!"
         )
         
         # 5. Set dependencies (task2 waits for task1)
         task2.add_upstream(task1)
         
         # 6. Add tasks to workflow
         workflow.add_tasks([task1, task2])
         
         # 7. Run the workflow
         status = workflow.run()
         print(f"Workflow completed with status: {status}")

   .. group-tab:: R

      .. code-block:: r

         library(jobmonr)
         
         # 1. Create a Tool
         my_tool <- jobmonr::tool(name = "quickstart_tutorial")
         
         # 2. Create a TaskTemplate
         template <- jobmonr::task_template(
             tool = my_tool,
             template_name = "echo_template",
             command_template = "echo {message}",
             node_args = list("message")
         )
         
         # 3. Create a Workflow
         wf <- jobmonr::workflow(
             tool = my_tool,
             workflow_args = "quickstart_v1"
         )
         
         # 4. Create Tasks
         task1 <- jobmonr::task(
             task_template = template,
             name = "say_hello",
             message = "Hello from Jobmon!"
         )
         
         task2 <- jobmonr::task(
             task_template = template,
             name = "say_goodbye",
             message = "Goodbye from Jobmon!",
             upstream_tasks = list(task1)  # 5. Set dependencies
         )
         
         # 6. Add tasks and run
         wf <- jobmonr::add_tasks(wf, list(task1, task2))
         status <- jobmonr::run(wf, resume = FALSE, seconds_until_timeout = 300)

Understanding the Code
======================

Tool
----
The ``Tool`` identifies your application in Jobmon. Use a consistent name 
across runs so you can track your workflows over time.

TaskTemplate
------------
A ``TaskTemplate`` defines a pattern for creating tasks. The ``command_template`` 
uses placeholders (like ``{message}``) that get filled in when you create tasks.

- **node_args**: Arguments that make each task unique (used for parallelization)
- **task_args**: Arguments that vary between workflow runs
- **op_args**: Operational arguments (like script paths)

Workflow
--------
The ``Workflow`` is the container for all your tasks. The ``workflow_args`` 
uniquely identify this workflow run - if you use the same args, you can 
resume a failed workflow.

Tasks and Dependencies
----------------------
Each ``Task`` is created from a template by providing values for the placeholders. 
Use ``add_upstream()`` to create dependencies between tasks.

Running on a Cluster
====================

To run on a real cluster (like Slurm), change the cluster name and add 
compute resources:

.. code-block:: python

   tool.set_default_cluster_name("slurm")
   
   task = template.create_task(
       name="my_task",
       message="Hello",
       compute_resources={
           "cores": 1,
           "memory": "2G",
           "runtime": "10m",
           "queue": "all.q",
           "project": "proj_yourproject",
       }
   )

A More Realistic Example
========================

Here's a workflow that processes data for multiple locations:

.. code-block:: python

   from jobmon.client.tool import Tool
   
   tool = Tool(name="location_processor")
   tool.set_default_cluster_name("slurm")
   
   # Template for processing one location
   process_template = tool.get_task_template(
       template_name="process_location",
       command_template="python process.py --location {location_id} --output {output_dir}",
       node_args=["location_id"],
       op_args=["output_dir"],
   )
   
   # Template for aggregating results
   aggregate_template = tool.get_task_template(
       template_name="aggregate",
       command_template="python aggregate.py --input {output_dir}",
       op_args=["output_dir"],
   )
   
   workflow = tool.create_workflow(
       name="process_all_locations",
       workflow_args="v1_2024"
   )
   
   locations = [1, 2, 3, 4, 5]
   output_dir = "/path/to/output"
   
   # Create a task for each location
   process_tasks = []
   for loc_id in locations:
       task = process_template.create_task(
           name=f"process_{loc_id}",
           location_id=loc_id,
           output_dir=output_dir,
           compute_resources={
               "cores": 2,
               "memory": "8G",
               "runtime": "1h",
           }
       )
       process_tasks.append(task)
   
   # Create aggregation task that depends on all process tasks
   agg_task = aggregate_template.create_task(
       name="aggregate_results",
       output_dir=output_dir,
       compute_resources={
           "cores": 4,
           "memory": "16G",
           "runtime": "30m",
       }
   )
   
   # Aggregation depends on all processing tasks
   for task in process_tasks:
       agg_task.add_upstream(task)
   
   # Add all tasks and run
   workflow.add_tasks(process_tasks + [agg_task])
   result = workflow.run()

What's Next?
============

- :doc:`/user_guide/core_concepts` - Deep dive into Jobmon concepts
- :doc:`/user_guide/workflows` - Advanced workflow patterns
- :doc:`/user_guide/compute_resources` - Resource management and retries
- :doc:`/user_guide/monitoring` - Monitoring and debugging workflows


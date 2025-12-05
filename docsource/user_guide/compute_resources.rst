*****************
Compute Resources
*****************

Compute resources control how much CPU, memory, and time your tasks receive 
on the cluster.

.. note::
   This page is being developed. For comprehensive resource documentation, 
   see :doc:`/advanced/advanced_usage`.

Basic Resources
===============

Specify resources when creating tasks:

.. code-block:: python

   task = template.create_task(
       name="my_task",
       compute_resources={
           "cores": 2,          # CPU cores
           "memory": "8G",      # RAM (supports G, GB, M, MB)
           "runtime": "2h",     # Wall time (supports h, m, s)
           "queue": "all.q",    # Cluster queue
           "project": "proj_x", # Accounting project
       },
       ...
   )

Resource Hierarchy
==================

Resources can be set at multiple levels:

1. **Task** - Highest priority
2. **TaskTemplate** - Default for tasks using this template
3. **Workflow** - Default for all tasks in workflow
4. **Tool** - Default for all workflows

.. code-block:: python

   # Set defaults at tool level
   tool.set_default_compute_resources({
       "queue": "all.q",
       "project": "proj_scicomp"
   })
   
   # Override at task level
   task = template.create_task(
       compute_resources={"memory": "32G"},  # Override memory only
       ...
   )

YAML Configuration
==================

Keep resources in a YAML file:

.. code-block:: yaml

   # resources.yaml
   task_template_resources:
     process_template:
       slurm:
         cores: 2
         memory: "8G"
         runtime: 3600
         queue: "all.q"

Load in code:

.. code-block:: python

   template = tool.get_task_template(
       template_name="process_template",
       yaml_file="resources.yaml",
       ...
   )

Automatic Retries
=================

Jobmon automatically retries failed tasks with scaled resources:

.. code-block:: python

   task = template.create_task(
       name="my_task",
       max_attempts=3,  # Will retry up to 3 times
       compute_resources={
           "memory": "8G",
           "runtime": "1h",
       }
   )

If a task fails due to memory or timeout, Jobmon scales resources by 50% 
and retries. Sequence: 8G → 12G → 18G

Custom Scaling
--------------

Override the default 50% scaling:

.. code-block:: python

   task = template.create_task(
       resource_scales={
           "memory": lambda x: x * 2,      # Double memory each retry
           "runtime": iter([7200, 14400]), # Explicit values: 2h, 4h
       },
       ...
   )

Fallback Queues
===============

If resources exceed queue limits after scaling, use a fallback queue:

.. code-block:: python

   task = template.create_task(
       compute_resources={
           "queue": "all.q",
       },
       fallback_queues=["long.q", "d.q"],
       ...
   )

Dynamic Resources
=================

Determine resources at runtime based on upstream results:

.. code-block:: python

   def get_resources(*args, **kwargs):
       # Read from file written by upstream task
       with open("/path/to/resource_needs.txt") as f:
           memory_gb = int(f.read())
       return {
           "memory": f"{memory_gb}G",
           "cores": 1,
           "runtime": "1h",
       }
   
   task = template.create_task(
       compute_resources=get_resources,  # Callable, not dict
       ...
   )

Checking Resource Usage
=======================

After workflow completion:

.. code-block:: python

   # Task-level usage
   usage = task.resource_usage()
   
   # Template-level aggregated usage
   stats = template.resource_usage(workflows=[workflow_id])

CLI resource prediction:

.. code-block:: bash

   jobmon task_template_resources -w <workflow_id>

See Also
========

- :doc:`/advanced/advanced_usage` - Full resource documentation
- :doc:`/advanced/performance` - Optimizing resource usage


**************
IHME Clusters
**************

This page documents the compute clusters available at IHME for running 
Jobmon workflows.

Slurm Cluster
=============

IHME's primary compute cluster runs Slurm. This is the default cluster 
for all Jobmon workflows.

Cluster Name
------------

When specifying the cluster in your code:

.. code-block:: python

   tool = Tool(name="my_tool")
   tool.set_default_cluster_name("slurm")

Available Queues
----------------

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 40

   * - Queue
     - Max Runtime
     - Max Memory
     - Use Case
   * - all.q
     - 3 days
     - 750GB
     - General purpose jobs
   * - long.q
     - 16 days
     - 750GB
     - Long-running jobs
   * - d.q
     - 24 hours
     - 1TB
     - High-memory jobs

.. note::
   Queue limits may change. Check with Scientific Computing for current values.

Default Resources
-----------------

If not specified, tasks use these defaults:

- **Cores**: 1
- **Memory**: 1GB
- **Runtime**: 10 minutes
- **Queue**: all.q

Archive Nodes
-------------

To access ``/snfs1`` (the J-drive), request an archive node:

.. code-block:: python

   task = template.create_task(
       compute_resources={
           "cores": 1,
           "memory": "10G",
           "runtime": "1h",
           "constraints": "archive"
       }
   )

Projects
--------

You must specify a project for accounting:

.. code-block:: python

   compute_resources={
       "project": "proj_scicomp",
       # ... other resources
   }

Contact your team lead for the correct project code.

Other Distributors
==================

For development and testing, you can also use:

Multiprocess Distributor
------------------------

Runs tasks locally using multiple CPU cores:

.. code-block:: python

   tool.set_default_cluster_name("multiprocess")

Sequential Distributor
----------------------

Runs tasks one at a time (useful for debugging):

.. code-block:: python

   tool.set_default_cluster_name("sequential")

Dummy Distributor
-----------------

Simulates job submission without actually running anything:

.. code-block:: python

   tool.set_default_cluster_name("dummy")

Troubleshooting
===============

Job Won't Submit
----------------

1. Check your project code is valid
2. Verify you have access to the requested queue
3. Ensure resource requests are within queue limits

Jobs Pending Too Long
---------------------

1. Check cluster utilization with ``squeue``
2. Consider using a different queue
3. Reduce resource requests if possible

For additional help, see :doc:`support`.


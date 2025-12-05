********************
IHME Quickstart
********************

This guide covers IHME-specific setup for running Jobmon workflows.

Prerequisites
=============

Before you begin, ensure you have:

1. Access to the IHME Slurm cluster
2. A valid project code for job accounting
3. Conda or a Python environment with Jobmon installed

Environment Setup
=================

Load the Jobmon Module
----------------------

On the IHME cluster, Jobmon is available as a module:

.. code-block:: bash

   module load jobmon

Or activate your conda environment with Jobmon installed.

Configuration
-------------

Your ``~/.jobmon.yaml`` should be configured for IHME:

.. code-block:: yaml

   http:
     service_url: "http://jobmon-service.ihme.washington.edu"

.. note::
   Contact Scientific Computing for the current server URL if the above 
   doesn't work.

Running Your First Workflow
===========================

1. **SSH to the cluster**:

   .. code-block:: bash

      ssh <username>@<cluster-login-node>

2. **Get an interactive session** (required for job submission):

   .. code-block:: bash

      srun --pty bash

3. **Activate your environment**:

   .. code-block:: bash

      module load jobmon
      # Or: conda activate your_env

4. **Run your workflow**:

   .. code-block:: bash

      python my_workflow.py

Compute Resources at IHME
=========================

Default resources on IHME's Slurm cluster:

- **Cores**: 1
- **Memory**: 1GB  
- **Runtime**: 10 minutes

Specify resources for your tasks:

.. code-block:: python

   task = template.create_task(
       name="my_task",
       compute_resources={
           "cores": 2,
           "memory": "10G",
           "runtime": "2h",
           "queue": "all.q",
           "project": "proj_yourproject",
       },
       ...
   )

Archive Node Access
-------------------

To access ``/snfs1`` (J-drive), request archive nodes:

.. code-block:: python

   compute_resources={
       "constraints": "archive",
       ...
   }

Monitoring at IHME
==================

Jobmon GUI
----------

Access the GUI at: https://jobmon-gui.ihme.washington.edu

- View workflow progress
- Investigate failed tasks
- Check resource usage

CLI Commands
------------

.. code-block:: bash

   # Check workflow status
   jobmon workflow_status -u $USER
   
   # See task details
   jobmon workflow_tasks -w <workflow_id>

Getting Help at IHME
====================

- **Slack**: ``#jobmon`` channel
- **Documentation**: https://jobmon.readthedocs.io
- **Office Hours**: Check IHME intranet for schedule

Next Steps
==========

- :doc:`clusters` - Detailed cluster information
- :doc:`/user_guide/core_concepts` - Understanding Jobmon concepts
- :doc:`/advanced/troubleshooting` - When things go wrong


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

Install Jobmon with IHME Configuration
--------------------------------------

The easiest way to install Jobmon at IHME is with the IHME installer package,
which automatically configures the server URL and other IHME-specific settings:

.. code-block:: bash

   pip install jobmon_installer_ihme

This installs:

- ``jobmon_client`` - the Jobmon client library
- IHME-specific configuration (server URL, etc.)

.. note::
   If you're using a shared conda environment that already has Jobmon installed,
   you may not need to install it yourself. Check with your team.

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

      conda activate your_env  # Environment with jobmon_installer_ihme installed

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

- **Slack**: ``#jobmon-users`` channel
- **GUI**: https://jobmon-gui.ihme.washington.edu
- **Documentation**: https://jobmon.readthedocs.io
- **Office Hours**: Check IHME intranet for schedule

For more help, see :doc:`support`.

Next Steps
==========

- :doc:`clusters` - Detailed cluster information
- :doc:`/user_guide/core_concepts` - Understanding Jobmon concepts
- :doc:`/advanced/troubleshooting` - When things go wrong


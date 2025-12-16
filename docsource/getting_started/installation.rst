************
Installation
************

This guide covers installing Jobmon for different use cases.

Quick Install
=============

For most users, install the Jobmon client with pip:

.. code-block:: bash

   pip install jobmon_client

This installs the Python client library you need to define and run workflows.

Installation Options
====================

Client Only
-----------

If you just need to submit workflows to an existing Jobmon server:

.. code-block:: bash

   pip install jobmon_client

Client with Server
------------------

If you need to run a local Jobmon server (for development or testing):

.. code-block:: bash

   pip install jobmon_client[server]

This includes the server components needed to run Jobmon locally.

Using Conda
-----------

If you prefer conda:

.. code-block:: bash

   # Create a new environment
   conda create -n jobmon python=3.10
   conda activate jobmon
   
   # Install Jobmon
   pip install jobmon_client

R Installation
==============

The R client is available for users who prefer R:
.. code-block:: r

   # Install from source (contact your Jobmon administrator for the package)
   install.packages("jobmon", repos = NULL, type = "source")

.. note::
   The R client requires a working Jobmon server. Contact your system 
   administrator for server connection details.

Verify Installation
===================

After installation, verify everything works:

.. tabs::

   .. group-tab:: Python

      .. code-block:: python

         import jobmon.client
         print(jobmon.client.__version__)

   .. group-tab:: R

      .. code-block:: r

         library(jobmon)
         print(packageVersion("jobmon"))

You should see the version number printed without errors.

Configuration
=============

Jobmon needs to know how to connect to the server and which cluster to use.

Environment Variables
---------------------

The simplest way to configure Jobmon is with environment variables:

.. code-block:: bash

   export JOBMON__HTTP__SERVICE_URL="http://jobmon-server.example.com:5000"

Configuration File
------------------

For persistent configuration, create a YAML file and point to it:

.. code-block:: bash

   export JOBMON__CONFIG_FILE="/path/to/your/jobmonconfig.yaml"

Example configuration file:

.. code-block:: yaml

   http:
     service_url: "http://jobmon-server.example.com:5000"
   
   # Optional: Set default cluster
   distributor:
     cluster_name: "slurm"

.. note::
   At IHME, the ``jobmon_installer_ihme`` package automatically configures
   the server URL. Contact your system administrator for the correct 
   configuration for your organization.

Development Setup
=================

For contributors who want to develop Jobmon itself, see the 
`Developer Setup <https://github.com/ihmeuw-scicomp/jobmon#developer-setup>`_ 
section in the README.

Docker Setup
------------

For local development with Docker:

.. code-block:: bash

   git clone https://github.com/ihmeuw-scicomp/jobmon.git
   cd jobmon
   docker-compose up

This starts:

- Jobmon backend server
- Jobmon GUI
- Database

Troubleshooting
===============

Import Errors
-------------

If you get import errors, ensure you have the correct Python version:

.. code-block:: bash

   python --version  # Should be 3.9 or higher

Connection Errors
-----------------

If you can't connect to the server:

1. Verify the server URL in your configuration
2. Check network connectivity to the server
3. Ensure any required VPN is connected

For more help, see :doc:`/advanced/troubleshooting`.

Next Steps
==========

- :doc:`quickstart` - Create your first workflow
- :doc:`/configuration/index` - Advanced configuration options


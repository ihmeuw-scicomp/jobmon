*************
Configuration
*************

Jobmon provides a flexible configuration system that allows you to customize 
behavior through configuration files, environment variables, or programmatic settings.

Overview
========

Configuration is loaded from multiple sources in order of precedence:

1. **Values set in code** - Parameters passed directly to Jobmon functions
2. **Environment variables** - Variables prefixed with ``JOBMON__``
3. **Configuration files** - YAML files in standard locations
4. **Default values** - Built-in defaults

Configuration Files
===================

Jobmon looks for configuration in these locations (in order):

1. Path specified by ``JOBMON_CONFIG_FILE`` environment variable
2. ``./jobmon.yaml`` in the current directory
3. ``~/.jobmon.yaml`` in your home directory
4. System-wide configuration (varies by installation)

Basic Configuration
-------------------

A minimal configuration file:

.. code-block:: yaml

   http:
     service_url: "http://jobmon-server.example.com:5000"

Common Settings
---------------

.. code-block:: yaml

   # Server connection
   http:
     service_url: "http://jobmon-server.example.com:5000"
     retries_attempts: 10
     timeout: 30
   
   # Default distributor settings
   distributor:
     cluster_name: "slurm"
     poll_interval: 10
   
   # Telemetry (optional)
   telemetry:
     logging:
       enabled: true

Environment Variables
=====================

All configuration values can be set via environment variables using the 
``JOBMON__`` prefix with double underscores separating nested keys:

.. code-block:: bash

   # Set server URL
   export JOBMON__HTTP__SERVICE_URL="http://jobmon-server.example.com:5000"
   
   # Set default cluster
   export JOBMON__DISTRIBUTOR__CLUSTER_NAME="slurm"

Sections
========

.. toctree::
   :maxdepth: 2

   configuration
   logging/index

See Also
========

- :doc:`configuration` - Complete configuration reference
- :doc:`logging/index` - Logging configuration details


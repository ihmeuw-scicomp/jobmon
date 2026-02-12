.. jobmon documentation master file

######
Jobmon
######

Jobmon is a Scientific Workflow Management system that simplifies running 
computational workflows on distributed computing systems. It provides:

- **Easy-to-use Python and R APIs** for defining workflows
- **Centralized monitoring** of jobs, including statuses and errors
- **Automatic retries** to protect against cluster failures
- **Resource-aware retries** that scale memory and runtime after failures
- **Workflow resumes** to continue from where you left off
- **Fine-grained job dependencies** including support for job arrays
- **A web-based GUI** for monitoring and debugging

Quick Links
===========

- :doc:`Getting Started <getting_started/index>` - New to Jobmon? Start here with installation and your first workflow.
- :doc:`User Guide <user_guide/index>` - Learn about workflows, tasks, resources, and monitoring.
- :doc:`Configuration <configuration/index>` - Configure Jobmon for your environment.
- :doc:`Advanced Topics <advanced/index>` - Arrays, dynamic resources, troubleshooting, and more.

#################
Table of Contents
#################

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started/index

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user_guide/index
   user_guide/core_concepts
   user_guide/workflows
   user_guide/tasks
   user_guide/compute_resources
   user_guide/monitoring
   user_guide/cli_reference

.. toctree::
   :maxdepth: 2
   :caption: Configuration

   configuration/index
   configuration/logging/index

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   advanced/index
   advanced/advanced_usage
   advanced/task_generator
   advanced/monitoring_debugging
   advanced/troubleshooting
   advanced/performance
   advanced/migration

.. toctree::
   :maxdepth: 2
   :caption: IHME Users

   ihme/index

.. toctree::
   :maxdepth: 2
   :caption: Reference

   glossary
   API Reference <autoapi/index>

.. toctree::
   :maxdepth: 2
   :caption: Developer Guide

   developers_guide/index

.. toctree::
   :maxdepth: 2
   :caption: Architecture

   architecture/index

Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

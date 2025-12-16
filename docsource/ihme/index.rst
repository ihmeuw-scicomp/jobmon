**************
IHME Users
**************

This section contains information specific to using Jobmon at the 
Institute for Health Metrics and Evaluation (IHME).

.. note::
   This section is specific to IHME's computing environment. If you're 
   using Jobmon outside of IHME, you can skip this section.

Getting Started at IHME
=======================

- :doc:`quickstart_ihme` - IHME-specific quickstart guide
- :doc:`clusters` - Information about IHME's compute clusters
- :doc:`support` - How to get help at IHME

IHME Infrastructure
===================

Jobmon Server
-------------

The IHME Jobmon server is available at:

- **GUI**: https://jobmon-gui.ihme.washington.edu
- **API**: Contact Scientific Computing for the API endpoint

Database Access
---------------

For direct database access (advanced users only), connection information 
is available at: https://jobmon-gui.ihme.washington.edu/#/jobmon_at_ihme

Slurm Cluster
-------------

IHME uses a Slurm cluster for job execution. Default compute resources:

- **Cores**: 1
- **Memory**: 1GB
- **Runtime**: 10 minutes

To request archive node access (for ``/snfs1`` / J-drive):

.. code-block:: python

   compute_resources={"constraints": "archive"}

.. toctree::
   :maxdepth: 2
   :hidden:

   quickstart_ihme
   clusters
   support


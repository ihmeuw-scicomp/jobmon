.. jobmon documentation master file, created by
   sphinx-quickstart on Fri Sep 23 09:01:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Jobmon
######

Jobmon is a Scientific Workflow Management system developed at IHME specifically for the
institute's needs. It is developed and maintained by IHME's Scientific Computing team.
Jobmon aims to reduce human pain by providing:

- An easy to use Python API and R API.
- Centralized monitoring of jobs, including the jobs' statuses and errors.
- A central SQL database with all information on past, current, and future runs.
- Automatic retries to protect against random cluster failures.
- Automatic retries following a resource failure, e.g. re-running a job with increased memory.
- Whole-of-workflow resumes to handle missing data or in-flight code fixes.
- Adds Application structure to what otherwise would be a soup of jobs.
- Fine-grained job dependencies, including for jobs within "job arrays."
- An easy-to-use GUI.

Jobmon was originally developed to augment the Univa Grid Engine (UGE)
and subsequently ported to Slurm when IHME switch from UGE to Slurm.

#################
Table of Contents
#################

.. toctree::
    :maxdepth: 2
    :caption: User Manual

    quickstart
    core_concepts
    logging/index
    monitoring_debugging
    advanced_usage
    glossary
    API Reference <api/modules>

.. toctree::
   :maxdepth: 2
   :caption: Developer's Guide

   developers_guide/index


.. toctree::
    :maxdepth: 2
    :caption: Architecture and Detailed Design

    architecture/index

Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

********
Overview
********

What is Jobmon?
===============

Jobmon is a Scientific Workflow Management system developed for managing 
complex computational workflows on distributed computing systems. It provides:

- **An easy to use Python API and R API** for defining workflows
- **Centralized monitoring** of jobs, including statuses and errors
- **A central SQL database** with all information on past, current, and future runs
- **Automatic retries** to protect against random cluster failures
- **Resource-aware retries** that automatically increase memory or runtime after failures
- **Whole-of-workflow resumes** to handle missing data or in-flight code fixes
- **Application structure** to organize what would otherwise be a soup of jobs
- **Fine-grained job dependencies**, including for jobs within "job arrays"
- **An easy-to-use GUI** for monitoring and debugging

Key Concepts
============

Before diving in, it helps to understand a few key concepts:

Workflow
--------
A **Workflow** is a collection of Tasks and their dependencies. Think of it as 
the complete plan for a computational pipeline. For example, a workflow might 
process data for multiple locations, aggregate results, and generate reports.

Task
----
A **Task** is a single executable command in your workflow. Each task runs 
independently and can depend on other tasks completing first.

TaskTemplate
------------
A **TaskTemplate** is a pattern for creating similar tasks. Instead of defining 
each task individually, you define a template and then create tasks by filling 
in the variable parts (like location IDs or dates).

Distributor
-----------
A **Distributor** is where tasks actually run. Jobmon supports multiple distributors:

- **Slurm**: For HPC clusters running Slurm
- **Multiprocess**: For running tasks locally using multiple CPU cores
- **Sequential**: For running tasks one at a time (useful for debugging)

How It Works
============

.. code-block:: text

   ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
   │  Your Python/R  │────▶│  Jobmon Server  │────▶│   Distributor   │
   │     Script      │     │   (Database)    │     │  (Slurm, etc.)  │
   └─────────────────┘     └─────────────────┘     └─────────────────┘
           │                       │                       │
           │  Define workflow      │  Track state          │  Run tasks
           │  Add tasks            │  Handle retries       │  Report status
           │  Set dependencies     │  Store results        │
           ▼                       ▼                       ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │                         Jobmon GUI                              │
   │              Monitor progress, debug failures                   │
   └─────────────────────────────────────────────────────────────────┘

1. **Define**: You write a Python or R script that defines your workflow
2. **Submit**: Jobmon validates your workflow and stores it in the database
3. **Execute**: The distributor runs your tasks on the cluster
4. **Monitor**: Track progress via CLI, GUI, or programmatically
5. **Resume**: If something fails, fix it and resume from where you left off

When to Use Jobmon
==================

Jobmon is ideal when you need to:

- Run the same analysis across many parameter combinations (locations, years, etc.)
- Manage complex dependencies between computational steps
- Automatically handle transient failures (network issues, bad nodes)
- Track resource usage and optimize future runs
- Resume failed workflows without re-running completed work
- Monitor long-running pipelines

Jobmon may be overkill if you're just running a single script or a few 
independent jobs.

Next Steps
==========

- :doc:`installation` - Get Jobmon installed
- :doc:`quickstart` - Create your first workflow
- :doc:`/user_guide/core_concepts` - Deep dive into Jobmon concepts


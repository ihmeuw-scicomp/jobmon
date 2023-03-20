**********
Quickstart
**********

Jobmon is a job-control system used for automating scientific workflows and running them on
distributed computing systems. It manages complex job and resource dependencies and manages
computing environment instability, ensuring dependably and assisting in troubleshooting when
needed. It is developed and maintained by IHME's Scientific Computing team.

Jobmon’s vision is to make it as easy as possible for everyone to run any kind of code
on any compute platform, reliably, and efficiently.
Jobmon should allow people to sleep easily on the
weekend because they do not have to manually monitor their applications.

.. include:: quickstart-ihme.rst

Getting Started
###############
The Jobmon controller script (i.e. the code defining the workflow) must be
written in Python or R. The modeling code can be in Python, R, Stata, C++, or in fact any
language.

The controller script interacts with Jobmon by creating a :ref:`jobmon-workflow-label` and
then iteratively adding :ref:`jobmon-task-label` to it. Each Workflow is uniquely defined by its
:ref:`jobmon-wf-arg-label` and its set of Tasks.

Jobmon allows you to resume workflows (see :ref:`jobmon-resume-label`). A Workflow can only
be resumed if the WorkflowArgs and all Tasks added to it are
exact matches to the previous Workflow.

Create a Workflow
#################

A Workflow is essentially a set of Tasks, their configuration details, and the
dependencies between them.
For example, a series of jobs that models one disease could be a Workflow.


A task is a single executable object in the workflow; a command that will be run.

A dependency from Task A to Task B means that B will not execute until A
has successfully completed. We say that Task A is *upstream* of Task B.
Conversely, Task B is *downstream* of Task A. If A always fails (up to its retry
limit) then B will never be started, and the Workflow as a whole will fail.

In general a task can have many upstreams. A Task will not start until all of its
upstreams have successfully completed, potentially after multiple attempts.

The Tasks and their dependencies form a
`directed-acyclic graph (DAG) <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
where the tasks are the nodes, and the edges are the dependencies.

For more about the objects go to :doc:`Core Concepts <core_concepts>`.

.. tabs::

    .. group-tab:: Python

        .. literalinclude:: ./quickstart_health_example.py
           :language: python

    .. group-tab:: R

        .. literalinclude:: ./quickstart_health_example.R
           :language: python


Constructing a Workflow and adding a few Tasks is simple:

.. note::
    Unique Workflows: If you know that your Workflow is to be used for a
    one-off project only, you may choose to use an anonymous Workflow, meaning
    you leave workflow_args blank. In this case, WorkflowArgs will default to
    a UUID which, as it is randomly generated, will be harder to remember and
    thus is not recommended for use cases outside of the one-off project. A workflow's
    uniqueness is based on its command, upstreams and downstreams, and workflow_args.

Compute Resources
#################

Compute Resources are used to allocate resources to your tasks.
You can specify memory, cores, runtime, queue, stdout, stderr, and project.

For IHME's Slurm cluster the defaults for all queues are:
* One core
* 1G memory, and
* Ten minutes runtime.

These values might change in the future.

You can specify that you want to run your jobs on an "archive" node
(i.e., a node with access to /snfs1
a.k.a "the J-drive"). Add the following key value pair to
their compute resources: ``"constraints": "archive"``.


.. note::
    By default Workflows are set to time out if all of your tasks haven't
    completed after 10 hours (36,000 seconds). If your Workflow times out
    before your tasks have finished running, those tasks will continue
    running, but you will need to restart your Workflow again. You can change
    the Workflow timeout period if your tasks combined run longer than 10 hours.


.. include:: help-ihme.rst
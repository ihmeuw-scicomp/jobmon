*********************
Requirements Analysis
*********************

Roles
=====

A Role (aka Actor) is a human or an external system that interacts with Jobmon.
Most roles are human, but some system roles exist because they initiate a use case.
For example, the Slurm scheduler is a system role because it initiates the Use Case "Launch a Job."

One person will often play the part of different Roles during the same day.
For example, at IHME a Coder will often also be an Operator or an Observer.
Therefore Roles are not job titles.

Technically, a Role is a Domain Object that can initiate a Use Case.

Human Roles
===========

Observer
  An observer is watching the progress of a Workflow, but not that fine details. Managers, Project Officers
  are typically observers, but Data Professionals and Engineers also take this role.
  For example, "Has location aggregation started yet?" is something they might ask.

  A person who is interested in how the workflow is going, and especially when it will be done,
is it running smoothly.
  For example, Project Officer of the owning team or a downstream team. Would only have read-only access

  A Distant Observer is an Observer with even less knowledge of the details,
  for example a Principal Investigator (PI). They can click on a link that is emailed to them.
  They don't have the time or knowledge to run a complex search to find what they are interested in.
  They do not understand internal structure of the modeling pipeline.
  Only interested in "Are we there yet?" and "How long will it be?"

Operator
  Someone who starts, stops, observers, and resumes workflows.
  They might find errors and debug operational issues like missing files,
  but do not debug the code.
  Cannot assume that they know how the code is written,
  but they are very familiar with what it does. Data Analysts and
  Engineers of all types are often operators, switching between this role and Implementor.

Implementer
  An implementor is someone who is implementing a research pipeline, i.e. writing code.
  Data Analysts, Research Engineers, and Software engineers spend most of their time as Implementers.

Debugger
  Someone who is debugging a research pipeline.
  This role is deliberately separate to the Implementor to emphasize that pipeline
are often debugged by people who did not write that pipeline and don't have intimate knowledge.
  Data Analysts, Research Engineers, and Software engineers are often Debuggers

Prototyper
  A special type of implementer-Operator who just wants to "get it done."
  They just want run only for this paper deadline, hoping to never run the code again
  (although it typically *is* run again next year). A typical Data Analyst activity.


System Roles
============

- Python Control Script
- R Control Script
- Slurm Distributor (it starts jobs)
- cgroups (it kills jobs)
- OOM Killer (it also kills jobs if cgroups failes)
- Cluster Distributor (Slurm, Azure, or other backends)
- The Gremlin (a synthetic System Role, it causes hardware to fail)

Domain Objects
==============

Any noun mentioned in a use case must either be a role or a domain object.
Domain Objects are capitalized to show that they are defined terms.
Domain objects might not be implemented in code. For example, Jobmon originally
had no "cluster" object, although it was added to the code
when Jobmon gained multiple Distributors.

A domain object might be implemented by several different pieces of code, depending on its
location in the deployment architecture. For example, domain objects such as Workflow
are implemented in the database schema, the sqlalchemy model objects, as a wire format,
and as a stub in the Python client and the  R client.

All domain objects are defined in the :doc:`../glossary`

Domain Objects that are mentioned in the Use Cases but are not part of Jobmon:

- Slurm job
- Linux Process
- Conda environment
- Cluster Node
- Python Application
- R Application

Use Cases
=========
Use Cases all follow the naming pattern:

*<Role> <Verb> <Domain-Object Phrase>*

For example:

- Slurm Launches Job
- Python-Application Creates Workflow
- Python-Application Runs Workflow
- Gremlin breaks a Cluster Node


In a waterfall project this Use Case section would be much bigger. Jobmon was developed using
the agile process, therefore the requirements were defined along the way.
The use cases identified here are looking forward to an operating GUI, and as examples.


Implementor Use Cases
=====================

Coder Converts a direct Cluster Job Launching Script to Jobmon
--------------------------------------------------------------

Included to emphasize the importance of usability,
this use case will describe the extra steps that are necessary.

Implementor Predicts Resource Usage
-----------------------------------

- Initially by running some examples and guessing
- Resource prediction based on historical data
  - GUI or command line or CSV or something query to return set confidence level

To support resource prediction.
In the the 2.0+ schema, this would be Tasks associated with the same Node.
This is the Node with identical node args,
e.g. Find the resource usage for all tasks for "most detailed burdenation for Canada, females, 2010"
but expressed using TaskTemplate_id plus three bound node_args.


Operator Use Cases
==================

Operator Starts Workflow
------------------------

- From Python or R code
- In future from GUI or command line by workflow ID.
  But how are the arguments passed? This use case is going to force
  the clarification between Workflow (intention to run a DAG, and
  a DAG which is a an actual exection). Or something like that.


Operator Resumes Workflow
-------------------------

- From Code
- From CLI via workflow-id
- Form GUI via-workflow-iw

Operator Debugs Workflow
------------------------

- How do they find the task statuses?
- How they view the edges between Tasks to debug dependency problems?
- How can they browse a DAG?
- How do they view inefficient resource retries
- How can the look for patterns of errors (very broad!)

Operator Stops Workflow
-----------------------

- From CLI or GUI, at API level it is via workflow-id.
  Option to let running jobs drain, or kill them immediately. Draining is like a pause.


Operator Sets Status of Set of Tasks
------------------------------------

- Given a set of selected Tasks, set their States to a given states. Useful as a precursor to resume

Operator Selects set of Tasks
-----------------------------

- Select an arbitrary set of Tasks.

  - Task Template is obvious and easy
  - By a Task or Workflow Attribute (it could be risk_id or some other "thread" through the DAG)
- Reset an entire workflow to "Not Done;" useful for debugging

Operator Adjusts Concurrency Limit
----------------------------------

- On a running workflow, or TaskTemplate within
- As part of a launch from the GUI

Operator Debugs Code
--------------------

- How do they find Errors from their own applications?
- FInd Slurm Log files
- Show Slurm log files
- Can Jobmon provide assistance with common errors?
- Summarise log files? Look for differences across a TaskTemplates set?


Observer Use Cases
==================

Observer Browses Tools
----------------------

- What is needed here? Find workflows by Tool?

Observer Finds Workflow
-----------------------

- Currently by launching user name. What if different users for weach WorkflowRun
- How else? By Tool? By date? BY a workflow Arg or attribute (eg output version number)

Observer asks "Is it Done Yet" (ie Workflow Status)
---------------------------------------------------

Technically-minded Observers can use the CLI. Distant Observers will only use the GUI.

- How many jobs are complete?
- How much time has been spent?
- How much longer will it take, given current cluster load and historical runtime?
- Is there anything that could stop it completing (I don't know what else to do here,
  but it is certainly a question that a Distant Observer would ask).


Jobmon Distributor Use Cases
============================

Jobmon submits a Job to the Distributors (Slurm, local)
-------------------------------------------------------

- For Slurm, use the Slurm REST API. It could also use the Slurm CLI but currently it does not.
  Using the CLI forces certain Jobmon Deployment units to be deployed on Slurm submit hosts.
  Posting to the Slurm REST API removes that restriction.
- Multiprocessing and Sequential Distributors use internal APIs for local execution

Slurm Use Cases
===============

Slurm Starts Job via Bash
-------------------------

1. Initial bash script
1. Passing of bash environment, including the conda environment from swarm to worker node
1. python execution wrapper
1. Call-backs to central services to show progress
1. Launching the actual application code in a sub-process
1. Need for careful exception handling


Slurm Starts Job via a Container
--------------------------------

The difference here is that the container is sealed against environment variables.
So a conda environment does not propagate.

Slurm Starts Job Array
----------------------
Anything different here?

Slurm Job finishes, with or without error
-----------------------------------------

- Catch the return code
- Call back to central service


Cgroups kills a Slurm Job for excess Resource Usage
---------------------------------------------------

- TaskInstance Heartbeat timeouts

Gremlin kills a Slurm Job without notification
-----------------------------------------------

- TaskInstance Heartbeat timeouts
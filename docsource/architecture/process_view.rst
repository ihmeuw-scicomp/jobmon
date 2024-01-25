
Asynchronous Services
*********************

Jobmon has two cron-like asynchronous services. They also are deployed in Kubernetes but do
not expose any web routes.

Usage Integrator
================

Since tasks on different cluster interfaces are not guaranteed to always complete, for accurate recording of resource
usage we need to occasionally backfill the database after a task instance exits. This is done by the usage integrator.

The usage integration code is not part of the main Jobmon server.
It is in the ``jobmon_slurm`` repository and is deployed as a separate Kubernetes service.
It runs asynchronously, working on a queue of jobs that have been launched but for which
there is no resource data. If the Jobmon database shows that the job has completed successfully,
the usage integration copies the resource data from the Slurm accounting database, although it might take some time
for that data to appear. If the job failed, the usage integrator removes it from its watch-list.

The Reaper
==========

Jobmon wraps the actual application in bash and Python. That Python calls routes
on the Jobmon server when the job starts, stops, or fails with an exception.
The wrapper is running asynchronously from the application code, so the wrapper
can send back  "I am alive" heartbeats to the Jobmon server.
However, if the node on which the Job is running goes down, then Jobmon never
receives notification that the job finished, because Jobmon's wrapper disappeared
with the node. Jobs sometime die without trace for other unknown reasons.

The Python client also sends heartbeats, announcing that the WorkflowRun is still
alive.

The Reaper is an asynchronous process that looks for WorkflowRuns
that have not sent heartbeats or finished within a specified timeout period.
These WorkflowRuns are "reaped," which means that they are moved to a Failed (for unknown reasons)
state. All that Jobmon knows is that they disappeared, and can now be resumed.

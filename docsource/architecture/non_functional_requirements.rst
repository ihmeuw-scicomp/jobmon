
Constraints and Non-functional Requirements
*******************************************

Scaling
=======

The goal is to run "all jobs" on the cluster.
The current largest workflow is the Burdenator, with about 500k jobs.
Operators have twice submitted workflows with about 1.5 million tasks,
although they are arguably over-parallelized.
On IHME's cluster Jobmon should plan for 20% annual growth in all dimensions.

Workflow Size
*************

Jobmon must be able to handle the largest Workflows at IHME because they have the
greatest need for reliability and investigation. However, most workflows only have
about 10,000 jobs. To be useful to the greatest number of people Jobmon must be
easy to use and efficient for small Workflows with as little overhead as possible.

Security
========
Security does not have to be especially high because Jobmon only has metadata on jobs.
However, it must not be possible to use
Jobmon to launch bad-actor jobs on the cluster. For example, exposing a service to the internet
that allows an external Jobmon to run jobs on the cluster would be a big security risk.
Jobmon relies on existing IHME security systems.

Jobmon stores no data apart from commands, so the cost of
a data breach would be low.

Lifetime Maintainability
========================
Plan for a 10 year lifetime.

Portability
===========
Jobmon was designed and developed as a sequence of Minimal Viable Product releases, so it was not
designed to be a cross-platform system. However, it is highly portable because it only depends
on Python, web technologies, sql, and the cluster OS is abstracted behind the Executor API.

In 2021 the Distributor layer was split out so that it could control jobs on IHME's UGE and Slurm
clusters simultaneously. The UGE plugin has not been maintained
since the UGE cluster was retired, although it would not be hard to bring it back to life.

MPI support could be difficult. Jobmon's model is that jobs can be launched independently,
whereas groups of jobs that communicate via MPI need to be launched as a group.
The TaskTemplate is probably the appropriate place to add MP support, if necessary.

GPUs can be supported if they are implemented in separate queues in the cluster OS.

Usability
=========

Usability is key, otherwise Jobmon will not be adopted.
Jobmon must offer more features and be at least as easy to use than raw Slurm, UGE,
or any container-based batch system.

Jobmon's advantages are described in various slide decks and will not be repeated here.



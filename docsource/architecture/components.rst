
Logical View (aka software layers, Component View)
**************************************************


The Jobmon Domain objects are split between the deployment units. For example, there are the following Task classes:


- jobmon.client.task
- jobmon.server.web.models.task
- jobmon.server.web.routes.task
- jobmon.server.web.fsm.task
- jobmon.serializers::SerializeSwarmTask


Most of the network plumbing is provided by Flask and the tiangolo image. That image includes:

- NGINX
- uWSGI
- Python 3.8
- Flask

For HTTP requests to the server, the client and worker_node both use the class jobmon.requestor
The requestor uses the Python tenacity package for automatic retries to smooth over transient networking and
load issues. The requestor distinguishes between 5xx errors (server errors) and 423 errors (retryable transactions).
The latter can be caused by race conditions that were detected and raised by the database as deliberate design.

Config
======

Jobmon takes configuration from four places, in order of priority:

1. Values set in customer code and passed to calls to Jobmon
#. Values set as environment variables
#. Values set in configuration files
#. Default values in code

This logic is handled by the jobmon.configuration.JobmonConfig class


Repositories
============

Choosing the correct number of repositories is tricky. Too many repositories create a complex and fragile
build system, especially when a change must be applied synchronously to multiple repositories at the same time.
The different forces acting are:

* Entities that version separately should be in separate repositories
* Testing must be easy
* Version errors must be hard to make

Jobmon-core is a repository. The pytests in that repository test the machinery of Jobmon using the dummy,
sequential, and multiprocessor distributors.

Each plugin is a repository:

* Slurm
* UGE

The entire deployment of the assembly at IHME is controlled by a final repository – jobmon_IHME-TAD.
TAD stands for "Test and Deploy." The repo contains version, test, and config information to create an
installation containing the correct versions of jobmon-core, jobmon_slurm, and (until recently) jobmon_uge.
The resulting assembly is automatically deployed to kubernetes and smoke-tested.

Any other installation should have a similar repository. A future release will include a skeletal TAD repository.



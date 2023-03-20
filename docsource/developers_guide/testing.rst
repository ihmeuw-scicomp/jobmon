*******
Testing
*******

TODO: UPDATE AFTER WE SUNSET EPHEMERA
*************************************
Nox
***
Jobmon core and both of the plugins use Nox for testing. Nox is a command-line tool that
automates testing in Python environments. To use Nox make sure that it has been installed in
your conda environment. Below are some useful Nox commands:

To run the whole test suite sequentially::

    nox --session tests -- tests

To re-use the build conda environment, add -r::

    nox -r --session tests -- tests

To run the whole test suite in parallel::

    nox -r --session tests -- tests -n <number of workers you want to use>
    e.g nox -r --session tests -- tests -n 3

To run a single file in the test suite::

    nox -r --session tests -- <file path>
    e.g. nox -r --session tests -- tests/cli/test_status_commands.py

To run a single test in the test suite::

    nox -r --session tests -- <file path>::<test_name>
    e.g. nox -r --session tests -- tests/cli/test_status_commands.py::test_workflow_status

Ephemera
********
Jobmon core and the plugins spin up an ephemera database (in memory database) for testing purposes. This database is
created at the start of the tests and is spun down at the end of the tests. The fixtures for
the tests are defined in conftest.py:

* ephemera - Creates one instance of the ephemera database returns the database connection string.
* web server process - Creates all services in Flask in a separate process.
* db_cfg - Creates all services in Flask in the same process.
* client_env - Exports FQDN and ports.

Jobmon Core
***********
Jobmon core is split in to integration and unit tests. Integration tests (end-to-end) should go all the way
through till workflow.run(). Client unit tests should only go until workflow.bind().

Unit tests are in the following folders:

* jobmon/tests/cli
* jobmon/tests/client
* jobmon/tests/distributor
* jobmon/tests/server
* jobmon/tests/swarm
* jobmon/tests/worker_node
* jobmon/tests/workflow_reaper

Integration tests are in:

* jobmon/tests/end_to_end

.. note::
    The Jobmon PR Jenkins pipeline runs the whole Jobmon test suite. A developer is not allowed
    to merge their PR until there has been a successful pipeline build i.e. all tests are passing.

UGE Plugin
**********
The UGE plugin is also split in to unit tests and integration tests (end-to-end).

Unit tests are in the following folders:

* jobmon_uge/tests/distributor
* jobmon_uge/tests/queue
* jobmon_uge/tests/worker_node

Integration tests are in the following folders:

* jobmon_uge/tests/integration

.. note::
    The Jobmon UGE PR Jenkins pipeline runs the whole Jobmon test suite. A developer is not allowed
    to merge their PR until there has been a successful pipeline build i.e. all tests are passing.

Slurm Plugin
************
The Slurm plugin is also split in to unit tests and integration tests (end-to-end).

Unit tests are in the following folders:

* jobmon_slurm/tests/distributor
* jobmon_slurm/tests/resources
* jobmon_slurm/tests/worker_node

Integration tests are in the following folders:

* jobmon_slurm/tests/integration

.. note::
    Currently, the Jobmon Slurm Jenkins pipeline is unable to run the Slurm test suite.
    Developers should make sure that they run the whole Slurm test suite locally before merging
    their PR.

Smoke Test
**********
A smoke test is a quick test for overall system functionality.

six_job_test.py is a simple smoke test that runs a small application of six jobs.
It should be used to confirm that communication between the client, services, and database are configured properly.
If it fails that indicates the services are not properly configured.

To run the six job_test: ``python ./deployment/tests/six_job_test.py {cluster_name}``

Load Test
*********
A Load Test is used to find the scaling limits of a release. Load testing is a heuristic used
to confirm that Jobmon is hitting the performance benchmarks required to run large applications
on IHME's cluster. Load testing is not covered by standard unit testing. It is not automated
and requires a human participant.

The general principle is run a fake application on a fresh deployment of Jobmon which mimics
how a large application would interface with Jobmon in order to confirm that Jobmon can handle
the load.

How to run a load test:
    1. Deploy the version of Jobmon that you want to load test to the Kubernetes "jobmon-dev" namespace.
    2. ssh onto a cluster node, srun, and activate your conda environment.
    3. Install the Jobmon version that was deployed to jobmon-dev in step 1.
    4. Set sample.yaml to reflect your desired testing preferences.
    5. python deployment/tests/multi_workflow_test.py --yaml_path deployment/tests/sample.yaml --scratch_dir {directory_for_load_test_results}
    6. Record the load testing data
        * The data is added to the HUB here: https://hub.ihme.washington.edu/pages/viewpage.action?spaceKey=DataScience&title=Jobmon+Load+Testing+General
        * Use the output from the load test for the bind time
        * Use APM to get the latency time.


Longevity Tests
***************
A longevity test is similar to a smoke test but it is run for days, with many calls,
typically searching for race conditions, memory leaks, or other rare errors or errors caused
by a build-up in resource utilization.

How to run a longevity test:
    1. Create a conda environment with Jobmon Core, and the plugin you want to use installed,
       and activate it while on a cluster node
    2. Issue the following command to point to the desired server if needed: jobmon_config
       update --web_service_fqdn 10.158.146.80 --web_service_port 80
    3. Run the following command (optionally specify "n" for how many minutes the test should run) python deployment/tests/longevity_test.py n

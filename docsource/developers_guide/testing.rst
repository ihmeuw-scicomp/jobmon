*******
Testing
*******

Jobmon uses pytest with nox for testing. Tests use SQLite databases that are
automatically created for each test session (or worker when running in parallel).

Running Tests with Nox
======================

Nox is the preferred way to run tests as it manages virtual environments automatically.

Run all tests:

.. code-block:: bash

   nox -s tests -- tests/

Re-use the existing test environment (faster):

.. code-block:: bash

   nox -r -s tests -- tests/

Run tests in parallel:

.. code-block:: bash

   nox -r -s tests -- tests/ -n 4

Run a specific test file:

.. code-block:: bash

   nox -r -s tests -- tests/pytest/client/test_workflow.py

Run a single test:

.. code-block:: bash

   nox -r -s tests -- tests/pytest/client/test_workflow.py::test_workflow_bind

Test Database Setup
===================

Tests use SQLite databases created automatically:

* Each test session (or parallel worker) gets its own database
* Databases are created in temporary directories
* The ``db_engine`` fixture creates and migrates the database
* The ``db_session`` fixture provides a transactional session per test

Key fixtures are defined in ``tests/pytest/fixtures/``:

* ``database.py`` - Database engine and session fixtures
* ``server.py`` - Web server process and client connection fixtures
* ``workflows.py`` - Tool, task template, and workflow fixtures

Test Organization
=================

Tests are organized by component:

Unit tests:

* ``tests/pytest/client/`` - Client library tests
* ``tests/pytest/server/`` - Server API tests
* ``tests/pytest/distributor/`` - Distributor tests
* ``tests/pytest/swarm/`` - Swarm component tests
* ``tests/pytest/worker_node/`` - Worker node tests
* ``tests/pytest/workflow_reaper/`` - Reaper tests

Integration tests (end-to-end):

* ``tests/pytest/end_to_end/`` - Full workflow execution tests

.. note::
   Integration tests exercise the full workflow from client through server to
   completion. They require more setup but verify system behavior end-to-end.

Other Test Commands
===================

Linting and formatting:

.. code-block:: bash

   nox -s lint      # Run linters (flake8, mypy)
   nox -s format    # Run formatters (black, isort)

Type checking:

.. code-block:: bash

   nox -s typecheck

Generate ERD diagram:

.. code-block:: bash

   nox -s schema_diagram

Load Testing
============

Load tests verify Jobmon's performance under heavy usage. These are not
automated and require manual execution.

To run a load test:

1. Deploy the Jobmon version to test
2. Configure the test parameters in ``deployment/tests/sample.yaml``
3. Run: ``python deployment/tests/multi_workflow_test.py --yaml_path deployment/tests/sample.yaml --scratch_dir {output_dir}``

Smoke Testing
=============

The ``six_job_test.py`` script is a quick smoke test that verifies basic
system functionality:

.. code-block:: bash

   python deployment/tests/six_job_test.py {cluster_name}

This confirms communication between client, server, and database is working.

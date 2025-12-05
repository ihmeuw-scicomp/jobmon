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

   nox -r -s tests -- tests/integration/client/test_workflow.py

Run a single test:

.. code-block:: bash

   nox -r -s tests -- tests/integration/client/test_workflow.py::test_workflow_bind

Test Database Setup
===================

Tests use SQLite databases created automatically:

* Each test session (or parallel worker) gets its own database
* Databases are created in temporary directories
* The ``db_engine`` fixture creates and migrates the database
* The ``db_session`` fixture provides a transactional session per test

Key fixtures are defined in ``tests/fixtures/``:

* ``database.py`` - Database engine and session fixtures
* ``server.py`` - Web server process and client connection fixtures
* ``workflows.py`` - Tool, task template, and workflow fixtures

Test Organization
=================

Tests are organized by test type:

Unit tests (``tests/unit/``):

* ``tests/unit/core/`` - Configuration, utilities, templates
* ``tests/unit/client/`` - Client library unit tests
* ``tests/unit/server/`` - Server logic unit tests
* ``tests/unit/swarm/`` - Swarm component unit tests (mocked)
* ``tests/unit/logging/`` - All logging configuration tests

Integration tests (``tests/integration/``):

* ``tests/integration/client/`` - Workflow/task binding, arrays
* ``tests/integration/server/`` - Database operations, routes
* ``tests/integration/distributor/`` - Task instantiation, triaging
* ``tests/integration/swarm/`` - Swarm execution integration
* ``tests/integration/reaper/`` - Workflow cleanup
* ``tests/integration/cli/`` - CLI command tests

End-to-end tests (``tests/e2e/``):

* Full workflow execution tests with real distributors

.. note::
   Unit tests run without a server and are fast (~16 seconds).
   Integration tests require a server and database.
   E2E tests exercise complete workflow execution.

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

****************
Developer Guide
****************

Welcome to the Jobmon Developer Guide. This section is for contributors who 
want to develop, extend, or maintain Jobmon itself.

If you're looking to **use** Jobmon for your workflows, see the 
:doc:`/user_guide/index` instead.

Getting Started
===============

New to Jobmon development? Start here:

1. :doc:`developer-start` - Set up your development environment
2. :doc:`testing` - Run and write tests
3. :doc:`continuous_integration` - CI/CD pipeline overview

Development Workflow
====================

The standard workflow for contributing to Jobmon:

1. Create a feature branch from the release branch
2. Make your changes
3. Add or modify unit tests
4. Run tests: ``nox -s tests``
5. Lint code: ``nox -s lint``
6. Type check: ``nox -s typecheck``
7. Create a pull request
8. Gain approval from reviewers

Quick Commands
==============

.. code-block:: bash

   # Run all tests
   nox -s tests -- tests/
   
   # Run specific test file
   nox -s tests -- tests/pytest/client/test_workflow.py
   
   # Lint and format
   nox -s lint
   nox -s format
   
   # Type checking
   nox -s typecheck
   
   # Generate ERD diagram
   nox -s schema_diagram

Repository Structure
====================

.. code-block:: text

   jobmon/
   ├── jobmon_core/      # Core library (config, requester, etc.)
   ├── jobmon_client/    # Python client (Tool, Workflow, Task)
   ├── jobmon_server/    # REST API server
   ├── jobmon_gui/       # React frontend
   ├── tests/            # Test suite
   ├── docsource/        # This documentation
   └── design/           # Design documents

Sections
========

.. toctree::
   :maxdepth: 2

   developer-start
   testing
   continuous_integration
   deployments-brief

See Also
========

- :doc:`/architecture/index` - System architecture and design
- `GitHub Repository <https://github.com/ihmeuw-scicomp/jobmon>`_

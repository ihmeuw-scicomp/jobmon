************************
Architecture and Design
************************

This section documents Jobmon's system architecture, design decisions, and 
technical implementation details.

Overview
========

Jobmon is a distributed workflow management system with several key components:

- **Client Libraries** (Python, R) - Define and submit workflows
- **REST API Server** - Manages state and coordinates execution
- **Database** - Persists workflow state and history
- **Distributors** - Execute tasks on various backends (Slurm, local, etc.)
- **GUI** - Web interface for monitoring and debugging

Architecture Diagram
--------------------

.. code-block:: text

   ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
   │  Python/R       │────▶│  Jobmon Server  │────▶│   Distributor   │
   │  Client         │◀────│  (REST API)     │◀────│   (Slurm)       │
   └─────────────────┘     └────────┬────────┘     └─────────────────┘
                                    │
                           ┌────────▼────────┐
                           │    Database     │
                           │   (MySQL/etc)   │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │   Jobmon GUI    │
                           │   (React)       │
                           └─────────────────┘

Key Design Principles
=====================

**Fault Tolerance**
   Workflows survive client disconnections, server restarts, and cluster failures.

**Resumability**
   Any workflow can be resumed from its last known state.

**Scalability**
   Designed to handle tens of thousands of concurrent tasks.

**Observability**
   Comprehensive logging, telemetry, and status tracking.

**Cluster Agnostic**
   Pluggable distributor architecture supports multiple backends.

Documentation Sections
======================

.. toctree::
   :maxdepth: 2

   use_cases
   non_functional_requirements
   components
   deployment_units
   process_view
   finite_state_machine

Design Documents
================

For detailed design decisions and implementation notes, see the ``design/`` 
directory in the repository:

- ``WORKFLOW_RUN_REFACTORING.md`` - Workflow execution architecture
- ``STRUCTURED_LOGGING_IMPLEMENTATION_V2.md`` - Logging system design
- ``task_status_audit_design.md`` - Task status management

See Also
========

- :doc:`/developers_guide/index` - Contributing to Jobmon
- :doc:`components` - Component architecture details
- :doc:`finite_state_machine` - State machine documentation

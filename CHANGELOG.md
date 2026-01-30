# Changelog

All notable changes to Jobmon will be documented in this file.


## [Unreleased]
### Added
- **Workflow DAG Visualization Enhancements**: Improved DAG node interactions on the Workflow Details page:
  - Clicking a DAG node (task template) now navigates to the corresponding task template details page
  - Hovering over a node highlights all connected edges (incoming and outgoing) in blue for better visual clarity
  - Existing hover behavior (status modal) is preserved
  - Node size (width and height) now scales with the number of tasks in each task template for better visual proportion
- **CLI Refactor - Click Migration**: Complete migration from argparse to Click framework with hierarchical command structure:
  - New hierarchical commands: `jobmon workflow status/tasks/reset/resume/concurrency/logs`, `jobmon workflow resources usage/yaml`, `jobmon task status/update/dependencies`, `jobmon config show/set`
  - Server CLI reorganized: `jobmon-server db init/upgrade/terminate`, `jobmon-server reaper start`
  - Proper logging configuration for both client and server CLIs using shared infrastructure
  - Comprehensive unit tests for new CLI structure
- **Force Cleanup for Stuck Task Instances**: New `--force-cleanup` flag in `jobmon workflow resume` command to manually cleanup stuck `KILL_SELF` task instances when jobs have been externally terminated (e.g., via `scancel` or node failure)

### Changed
- **CLI Entry Points**: Server CLI entry point renamed from `jobmon_server` to `jobmon-server` (hyphenated)
- Refactored `status_commands.py` monolith into domain-focused modules: `commands/workflow.py`, `commands/task.py`, `commands/resources.py`, `commands/config.py`, `commands/validation.py`

### Fixed
- **Workflow DAG (GUI)**: Task template names on the DAG viz no longer overflow node boundaries—nodes use variable width/height to fit the full name and scale with task count. Task template hover popover now appears correctly when the DAG is panned or scrolled (positioned from mouse coordinates).
- **Workflow Resume Cleanup**: Fixed multiple critical issues with workflow resume and task instance state management:
  - Fixed NO_HEARTBEAT infinite loop when parent Task is already in terminal state (DONE/ERROR_FATAL). The `validate_transition()` function now allows orphaned task instances to transition to ERROR_FATAL instead of endlessly retrying invalid state transitions.
  - Fixed duplicate error log entries caused by HTTP retries. The `_log_error()` function now validates transitions before creating error logs and includes idempotency checks.
  - Fixed `terminate_task_instances` to correctly identify and terminate task instances by `workflow_run_id`. Previously used incorrect joins through Task table that could miss instances during resume.
  - Fixed orphaned task instances during ungraceful distributor shutdowns. The reaper now properly terminates task instances that the old distributor never cleaned up.
  - Improved task instance cleanup logic: `QUEUED`/`INSTANTIATED` instances now go directly to `ERROR_FATAL` (no worker exists), while `LAUNCHED`/`RUNNING` instances go to `KILL_SELF` for worker cleanup.
  - Fixed premature workflow resume by having reaper wait for all `KILL_SELF` task instances to be cleaned up before transitioning workflow run to `TERMINATED`.
  - Enhanced `is_resumable` endpoint to return `pending_kill_self` count, preventing resume attempts while cleanup is pending.
- **State Transition Validation**: Renamed `get_transit_status()` to `validate_transition()` with detailed logging at each validation step for improved debuggability of state machine issues.

### Deprecated
- Legacy argparse-based CLI commands deprecated with warnings (will be removed in 3.0):
  - `jobmon workflow_status` → `jobmon workflow status`
  - `jobmon workflow_tasks` → `jobmon workflow tasks`
  - `jobmon workflow_reset` → `jobmon workflow reset`
  - `jobmon workflow_resume` → `jobmon workflow resume`
  - `jobmon task_status` → `jobmon task status`
  - `jobmon update_task_status` → `jobmon task update`
  - `jobmon task_dependencies` → `jobmon task dependencies`
  - `jobmon concurrency_limit` → `jobmon workflow concurrency`
  - `jobmon get_filepaths` → `jobmon workflow logs`
  - `jobmon task_template_resources` → `jobmon workflow resources usage`
  - `jobmon create_resource_yaml` → `jobmon workflow resources yaml`
  - `jobmon update_config` → `jobmon config set`


## [3.6.0] - 2025-12-02
### Added
- **WorkflowRun Refactor - New API**: Complete refactoring of the workflow run execution system with a cleaner, more testable architecture:
  - New `run_workflow()` and `resume_workflow_run()` factory functions as the recommended API for executing workflows
  - New `WorkflowRunConfig` dataclass consolidating all execution configuration (heartbeat interval, fail_fast, concurrency limits, etc.)
  - New `OrchestratorResult` dataclass providing complete execution results including task-level final statuses, counts, and timing
  - Results are now immutable snapshots instead of requiring mutable state access post-execution
- **WorkflowRun Refactor - Internal Services**: Modular service architecture replacing monolithic WorkflowRun class:
  - `ServerGateway`: Centralized HTTP communication with the Jobmon server
  - `SwarmState`: Consolidated workflow run state management with atomic updates
  - `HeartbeatService`: Background heartbeat management with proper status change detection
  - `Synchronizer`: State synchronization between client and server (task statuses, concurrency limits)
  - `Scheduler`: Task batching and queueing with capacity-aware scheduling
  - `SwarmBuilder`: State initialization from workflow objects or database
  - `WorkflowRunOrchestrator`: Thin coordinator managing the main execution loop
- **Test Structure Reorganization**: Further improved test organization for better maintainability:
  - `tests/unit/` - Unit tests
  - `tests/integration/` - Integration tests
  - `tests/e2e/` - End-to-end tests
  - Consolidated logging tests and improved pytest-xdist race condition handling
  - Organized fixtures into modules for better reusability
- Auto-build documentation for release branches in ReadTheDocs

### Changed
- **WorkflowRun Refactor - API Changes**: 
  - `WorkflowRun.run()` now returns `OrchestratorResult` instead of `None`, providing explicit execution results
  - Callers should use `result.task_final_statuses[task_id]` instead of `swarm.tasks[task_id].status` post-execution
  - `WorkflowRun` class is preserved for backward compatibility but marked as deprecated in favor of factory functions
- Simplified logging configuration with programmatic generation replacing complex YAML templates
- Removed DNS engine and singleton patterns from database layer for cleaner architecture

### Fixed
- Fixed WorkflowRun orchestrator main loop spinning without rate limiting, causing server spam with sync requests every few milliseconds instead of waiting for heartbeat intervals. The `HeartbeatService` now initializes `_last_heartbeat_time` to the current time instead of 0, ensuring proper sleep/sync pacing.
- Fixed DEBUG logs leaking to OTLP telemetry when host applications use direct-rendering mode with structlog. The `_forward_event_to_logging_handlers` processor now checks `isEnabledFor(level)` before forwarding logs, respecting the configured logger levels (e.g., `jobmon.core` at WARNING).
- Fixed `asyncio.run()` from within running event loop by adding proper nested event loop handling
- Fixed nox color flag conflict in pre-commit hooks
- Added defensive checks and improved exception handling in swarm operations

### Deprecated
- `WorkflowRun` class is deprecated in favor of `run_workflow()` and `resume_workflow_run()` factory functions
- Direct access to `WorkflowRun` internal state (`.tasks`, `.done_tasks`, `.failed_tasks`) is deprecated; use `OrchestratorResult` from `run()` instead

### Removed
- Removed legacy `WorkflowRun` internal methods replaced by service classes: `process_commands()`, `process_commands_async()`, `synchronize_state()`, `synchronize_state_async()`, `set_initial_fringe()`, `queue_task_batch_async()`, `_set_validated_task_resources()`, `_set_adjusted_task_resources()`, `get_swarm_commands()`, `SwarmCommand` class
- Removed `WorkflowRun` feature flags (`USE_NEW_GATEWAY`, `USE_NEW_STATE`, `USE_NEW_HEARTBEAT`, `USE_NEW_SYNCHRONIZER`, `USE_NEW_ORCHESTRATOR`) after migration completed


## [3.5.0] - 2025-12-02
### Added
- Added unified OTLP manager with HTTP exporter support, shared logger providers, and automatic CLI shutdown flushing so all components forward structured telemetry reliably.
- Added dedicated logging documentation set (architecture overview, operator guide, telemetry status) along with refreshed example configs covering OTLP transports.
- The `jobmon workflow_resume` command now automatically increases resources for tasks that failed due to resource errors by default. This feature identifies tasks in ERROR_RECOVERABLE or ERROR_FATAL status whose latest task instance is in RESOURCE_ERROR status, updates their resources using the defined resource scales, and sets their status to ERROR_RECOVERABLE for retry. Use `--use-original-resources` to skip this behavior and keep original resource values.
- Added `jobmon update_config` command to allow users to update configuration values in their defaults.yaml file using dot notation (e.g., `jobmon update_config http.retries_attempts 15`).
- **Structured Logging with Context Binding and OTLP Integration**: Complete implementation of structured logging across all Jobmon components with automatic context binding, OTLP attribute extraction, and APM integration:
  - **Context Binding Decorator** (`@bind_context`): Automatically bind function/method parameters to log context with support for nested attribute extraction and automatic cleanup
  - **Automatic Configuration**: All components (client, server, distributor, worker) auto-configure structlog via CLI inheritance with zero manual setup required
  - **OTLP Attribute Extraction**: Custom handler extracts structlog fields as separate OTLP attributes using thread-local storage, enabling searchable fields in APM/Elasticsearch
  - **Clean Console Output**: Traditional log format for console (timestamp, level, logger, message) while sending full structured data to OTLP
  - **Context Propagation**: Automatic context propagation from client through HTTP requests to server via `X-Server-Structlog-Context` header for distributed tracing
  - **Distributor Enhanced**: 15+ strategic structured logs added with `@bind_context` decorators on key methods (launch_task_instance, launch_task_instance_batch, triage_error, etc.)
  - **Readable Event Names**: Human-friendly event names ("Batch launched" not "batch_launched") for better log readability
  - **Code Consolidation**: Removed 162 lines of duplicate server logging configuration, consolidated to shared core utilities
  - **APM Query Support**: All context fields (workflow_run_id, task_instance_id, cluster_name, etc.) searchable as `numeric_labels.*` or `labels.*` in Kibana/APM
  - **Test Utilities**: Added `tests/workflows/emit_single_log.py` for verifying OTLP attribute extraction in development
- **Enhanced Logging System**: Redesigned Jobmon's logging architecture with automatic component configuration and production-ready features:
  - Automatic logging configuration for all CLI components (distributor, worker, server, client) with console logging by default
  - Template-based logging configurations with user override support (file-based and section-based customization)
  - Production-ready OpenTelemetry (OTLP) integration (opt-in via configuration)
  - Library-safe logging with proper propagation and no root logger configuration
  - Integration with `workflow.run()` via optional `configure_logging` parameter
  - Comprehensive test coverage with parallel execution support and database isolation fixes
- **Enhanced Docker Development Environment**: Complete containerized development setup for improved workflow testing:
  - New `jobmon_client` Docker container with local development support and editable installs
  - Live code editing with volume mounts for `jobmon_core` and `jobmon_client` source code
  - Interactive shell support (stdin/tty) for workflow development and testing
  - Comprehensive dependency management with automated installation scripts
  - Enhanced `docker-compose.yml` with proper OTLP configuration mounting
- **Test Structure Reorganization**: Improved test organization for better maintainability:
  - `tests/pytest/` - All automated pytest tests (moved from root tests/)
  - `tests/workflows/` - Development and manual testing workflows
  - `tests/_scripts/` - Utility scripts (unchanged)
  - Updated pytest configuration for new test paths with proper test discovery
  - Added sample workflow examples and comprehensive testing documentation
- Added an `all` option to the status filter on the home page of the Jobmon GUI.
- Added search parameters to the GUI URL.
- Allow advanced filtering on the Jobmon GUI.
  - Added support for negation e.g. `?username!=svc_scicomp`
  - Added support for multiple filter values e.g. `username=<user1>,<user2>`
- Added additional task information to the resource usage CSV download in the GUI.
- Show a red x next to a task template name on the workflow details page of the Jobmon GUI if any tasks are in fatal state in that task template.
- Async workflow-run scheduler with dedicated heartbeat loop for improved responsiveness and parallelized state synchronization.

### Changed
- Hardened structlog integration to detect `_nop`-filtered log levels, wrap `PrintLogger` factories with named proxies, and ensure telemetry processors execute even when hosts filter debug output.
- Reworked client logging bootstrap to respect direct-render hosts by pruning non-Jobmon handlers, reattaching OTLP handlers as needed, and deferring to host-provided structlog configuration when present.
- **BREAKING: Logging System Migration**: Completely replaced legacy logging system with new elegant architecture:
  - Client logging now uses `configure_client_logging()` instead of deprecated `JobmonLoggerConfig.attach_default_handler()`
  - Server logging automatically selects OTLP configuration based on `otlp.web_enabled` setting
  - All logging configurations now support user customization via `JobmonConfig` overrides
  - OTLP configurations moved from monolithic files to focused packages (`jobmon.core.otlp`, `jobmon.server.web.otlp`)
  - Default logging configurations moved to template-based system with shared patterns
- **BREAKING: Telemetry Configuration Structure**: Replaced `otlp` section with new `telemetry` configuration structure:
  - `otlp.http_enabled` → `telemetry.tracing.requester_enabled`
  - `otlp.web_enabled` → `telemetry.tracing.server_enabled`
  - `otlp.deployment_environment` → `telemetry.deployment_environment`
  - Nested tracing configuration under `telemetry.tracing` with configurable span exporters
  - Clear separation: logging via logconfig files, tracing via telemetry config
- Changed the default submitted date on the Jobmon GUI from 2 weeks to one day.
- Changed order of status in workflow pop up so that pending is now before schedule in the GUI.
- Changed the command column in the task details table the GUI. If you click the command a modal will pop up that shows the whole command.

### Fixed
- Fixed workflow test hook race condition where `_fail_after_n_executions` check could be bypassed when tasks complete within a single loop iteration, causing flaky test failures in CI.
- Fixed server-set terminal statuses handling and improved teardown robustness in async workflow run.
- Fixed multiprocess distributor issues with parallelized async state sync.

### Deprecated
- Legacy logging classes `JobmonLoggerConfig` and `ClientLogging` are deprecated in favor of new `configure_client_logging()` function
- Direct import of OTLP classes from `jobmon.core.otlp` and `jobmon.server.web.otlp` module roots (use specific submodules)

### Removed
- Removed legacy V2 server REST route modules and regenerated GUI API schema to reflect the consolidated V3 surface area.
- Removed legacy `JobmonLoggerConfig.attach_default_handler()` method and `ClientLogging().attach()` pattern
- Removed monolithic OTLP configuration files in favor of modular package structure
- Removed hardcoded logging configurations in favor of template-based system with user override support
- Removed duplicate logging setup code across client, server, and requester components


## [3.4.25] - 2025-10-23
### Added
- Added `jobmon update_config` command to allow users to update configuration values in their defaults.yaml file using dot notation (e.g., `jobmon update_config http.retries_attempts 15`).
- Added async retry support to Requester and modernized DistributorService for improved error handling and performance.
- Added UV for dependency and workflow management, replacing pip-tools for faster and more reliable dependency resolution.
- Added configurable database connection pool settings to prevent timeout errors in high-load scenarios.
- Added enhanced OpenTelemetry database instrumentation with error capture for better observability.
- Added JUnit XML report generation for test dashboard integration.
- Added consolidated JSON logging fixture for server tests to improve test maintainability.
- Added unified V3 API endpoint for task template resource usage statistics with comprehensive metrics including min, max, mean, and percentile calculations.
- Added missing `/array/{array_id}/transition_to_killed` endpoint to V3 API for proper KILL_SELF task status processing, completing V3 API feature parity with V2.
- Added more information about stopping workflows and updating task statuses to the technical panel in the Jobmon GUI.
- Added `Task Name` to the tooltip in the resource usage scatter plot.
- Enabled filtering by `Task Name` in the resource usage scatter plot.
- Added a `Download CSV button` to the resource usage page, allowing users to export all plot data regardless of filters.
- **Performance Optimization**: Improved query performance for task template error log visualization by replacing correlated subqueries with CTEs and combining queries using window functions.
- **Performance Optimization**: Enhanced database lock handling with NOWAIT locks and exponential backoff retry logic for potential race conditions.
- **Performance Optimization**: Optimized array batch processing with 1000-item batches and immediate commits to reduce lock contention.
- **Performance Optimization**: Optimized `/api/v3/workflow_tt_status_viz/{workflow_id}` endpoint by replacing two separate database queries with a single SQL aggregation query.
- **Performance Optimization**: Added retry logic with exponential backoff to `/api/v3/task_instance/{task_instance_id}/log_running` endpoint for better deadlock resilience and concurrent request handling.
- **Performance Optimization**: Enhanced `/api/v3/task_instance/instantiate_task_instances` endpoint with atomic transactions and retry logic to prevent deadlocks during high-concurrency task instantiation.
- **Performance Optimization**: Improved `/api/v3/dag/{dag_id}/edges` endpoint with explicit transaction commits and standardized error handling patterns.
- **Performance Optimization**: Optimized array transition endpoints (`/api/v3/array/{array_id}/transition_to_launched` and `/api/v3/array/{array_id}/transition_to_killed`) with atomic Task and TaskInstance updates in single transactions.
- **Performance Optimization**: Dramatically improved `/api/v3/get_task_template_details` endpoint performance by replacing complex 4-table JOIN with 4 targeted queries and O(n) set intersection.
- JSON Compatibility Layer: Added backward compatibility for `downstream_node_ids` field - clients ≤ 3.4.23 receive quoted JSON strings, newer clients receive unquoted arrays
- Added grace period handling for distributor startup with improved error recovery.

### Changed
- Overhauled frontend resource utilization page for better performance and user experience.
- Migrated project dependency management from pip-tools to UV workspace configuration.
- Consolidated database session management and configuration for improved consistency and performance.
- Enhanced Python client to handle string confidence interval parameters and new unified resource usage response format.
- Updated Workflow.add_tasks() parameter type from Sequence[Task] to Iterable[Task] to accept a broader range of iterable types including generators and iterators. (PR 279)
- Expanded error message when a TaskInstance doesn't report a heartbeat.
- Refactored task status update endpoint into smaller functions for better maintainability.
- Replaced SessionMaker with FastAPI DB injection.
- Locked corresponding rows in both Task and TaskInstance tables when transition states.
- Reduced conditional selection from twice to once in set_status_for_triaging to shorten locking time, but still aggressively locked all corresponding rows in the TaskInstance table.
- Added race condition protection in /log_error_worker_node to move task instance in T to R first.
- Switched to one session logic for /task_template/{task_template_id}/add_version with manual rollback to avoid dead lock.
- Optimized `/get_task_template_details` and `/task_template_resource_usage` routes.
- Optimized `/task_template_dag` route to use less memory.
- Improved isort configuration to correctly identify `jobmon` as first-party package, ensuring proper PEP 8 import order (stdlib → third-party → local).

### Fixed
- Fixed clustered errors bug where error clustering would fail in certain edge cases.
- Fixed deadlock in `queue_task_batch` endpoint with proper locking and added tests.
- Fixed datetime bug in workflow overview API (#320).
- Fixed `queue_task_batch` to restore v2 behavior - when no tasks need updating, the function now continues to query and return the actual current status of all requested tasks instead of returning an empty dict.
- Fixed critical database session leaks in workflow routes that could cause connection pool exhaustion in production.
- Fixed transaction anti-patterns with multiple commits, ensuring proper atomicity and error handling.
- Fixed DNS cache variable scope bug that was causing NXDOMAIN crashes in production environments.
- Fixed DNS test logging issues in CI by implementing proper structlog capture mechanisms.
- Fixed OpenTelemetry instrumentation order to initialize before database access, preventing instrumentation issues.
- Fixed test suite warnings and failures including AsyncIO deprecation warnings, Pydantic configuration warnings, Pandas FutureWarnings, and multiprocessing fork warnings on macOS.
- Fixed pre-existing test failures related to configuration type handling and swarm test infrastructure.
- Fixed configuration system to properly handle environment variable conflicts between primitive and nested assignments, with improved YAML parsing that preserves natural data types (integers remain integers) and proper merging logic that prevents "TypeError: 'str' object does not support item assignment" errors.
- Fixed test environment isolation by preventing .env file loading during pytest runs and implementing proper subprocess environment inheritance, eliminating SSL configuration conflicts and ensuring consistent database connections between test processes.
- Fixed V3 API task template resource usage statistics with unified response format, proper handling of memory values (distinguishing None/0B/invalid), correct task count semantics, and backward-compatible CLI support for both V3 dictionary and legacy array formats.
- Fixed datetime serialization in workflow overview API to handle both datetime objects and string formats for cross-database compatibility (PostgreSQL vs SQLite).
- Fixed ClientDisconnect exceptions appearing as errors in APM by adding global exception handler. (PR 282)
- Fixed get_max_concurrently_running endpoint to handle non-existent workflows gracefully by returning a 404 error with descriptive message instead of raising an exception. (PR 278)
- Fixed 'Set' object is not subscriptable in CLI error in `/update_statuses` route.
- Fixed client bug in loop_start timing.

### Deprecated
- Removed jobmon update_config CLI command

### Removed
- Removed jobmon_gui/CHANGELOG.md


## [3.4.23] - 2025-07-10
### Added
- Added optional authentication support for Jobmon server and GUI (PR TBD). Authentication can now be disabled via `JOBMON__AUTH__ENABLED=false` server-side and `VITE_APP_AUTH_ENABLED=false` client-side environment variables for development and testing environments.
- Added submitted_date and status_date to TaskInstance table on the Task Details page in the Jobmon GUI.
- Added the queue the TaskInstance ran on to the Requested Resources modal on the Task Details page in the Jobmon GUI.

### Fixed
- Fixed distributor startup communication to be resilient against stderr pollution from package warnings and other output. The startup detection now uses non-blocking I/O and pattern-based parsing instead of expecting exactly 5 bytes, preventing hangs when dependent packages emit warnings during process startup.

### Changed
- Optimize SQL in `update_status` route to lessen chance of wait lock timeout on the database.

## [3.4.14] - 2025-05-05
### Added
- Added the ability to specify task attributes in TaskGenerator create_task method (PR 264).

### Fixed
- Fixed a bug where name_func could not be passed through task_generator decorators (PR 264).

## [3.4.13] - 2025-04-21
### Added
- Added a link to the TaskDetails page from the clustered errors modal (PR 255).

### Changed
- Swapped the location of the DAG viz and TaskInstance table on the Task Details page (PR 255).
- Changed tasks in "G" state (registering) to white in Task DAG viz (PR 255).

### Fixed
- Fixed a bug where dates were appearing in UTC (not in user's specified timezone) in the Jobmon GUI (PR 251).
- Fixed broken home button on TaskDetails page (PR 255).

## [3.4.12] - 2025-04-03
### Added
- Added the ability to specify upstream tasks in TaskGenerator create_task(s) methods (PR 254).

## [3.4.11] - 2025-03-31
### Fixed
- Fixed bug in requester where ValueErrors weren't being handled (PR 250).

## [3.4.10] - 2025-03-25
### Fixed
- Fixed resource retry for new slurm API. Fixes bug where tasks were going into "U" state instead of "Z" state (jobmon_slurm PR 160).

## [3.4.9] - 2025-03-19
### Removed
- Removed pin in jobmon_client pyproject.toml (PR 248).

## [3.4.8] - 2025-03-19
### Added
- Added the ability for the user to change their task statuses in the Jobmon GUI (PR 243).

### Changed
- Upped the number of workers in the pods from 4 -> 6.
- Each subpackage has its own license (PR 246).

### Fixed
- Fixed a bug where task instance were stuck in `triaging` state (jobmon_slurm PR 159).

### Removed
- Removed `--args` separator from TaskTemplate (PR 244).

## [3.4.7] - 2025-03-04
### Changed
- Added missing client ingress route in TAD.

## [3.4.6] - 2025-03-04
### Fixed
- Fixed a bug in clustered error-ing where an exception would happen if there were no values in task_instance.error_log (PR 238).
- Properly handle deserialization of empty strings in the task generator (PR 240).

## [3.4.5] - 2025-02-26
### Fixed
- Fixed the Jobmon test suite (PR 235).
- Fixed a bug where the DAG viz wouldn't load if the DAG only had a single node (PR 236).

## [3.4.4] - 2025-02-25
### Fixed
- Fixed a decimal serialization error in v2 get_workflow_tt_status_viz (PR 233).
- Fixed a bug where resource usage data wasn't being shown do to incorrect indexing (PR 234).

## [3.4.3] - 2025-02-24
### Fixed
- Fixed a datetime error in v2 get_task_details_viz (PR 232).

## [3.4.2] - 2025-02-24
### Fixed
- Fixed a datetime error in v2 workflow_by_user_form (PR 231)
- Fixed pod scaling problems in TAD.

## [3.4.1] - 2025-02-24
### Fixed
- Removed an `await` in the requester context that was causing a bug (PR 229).
- Made `app_requested_context` backward compatible (PR 230).

## [3.4.0] - 2025-02-24 (jenkins 22)
### Added
- Warn users if they haven't specified `naming_args` when using the task generator (PR 202).

### Changed
- Bulk kill TaskInstances to speed up workflow stopping (PR 209).
- Switched from Flask to FastAPI (PR 169).
- Display the full stack trace when the distributor service encounters an exception, rather than only showing "Retry Error" (PR 227).

### Fixed
- Fixed a bug where users were getting hash collision errors for their Tasks that didn't have the same node_arg values (PR 214).
- Fixed a bug where the wrong usage values were being populated in the Jobmon database from the slurm accounting databse (jobmon_usage PR 4).
- Fixed a bug where timeouts set in the `workflow_resume` CLI were not being propagated properly (PR 215).

## [3.2.6] - TBD
### Added
- Reaper "poll_interval_minutes" config key - default 5 minutes (PR 69).
- Add workflow timeout to `resume_workflow` CLI (PR 73).
- Added alembic to Jobmon database (PR 84).
- Added API versions to Jobmon - one server deployment can now support multiple versions of the Jobmon client (PRs 88, 90).
- Enabled OpenTelemtry Protocol (OTLP) to Jobmon (PRs 89, 93).
- Added non-Bifrost navigation to the Jobmon GUI (PR 95).

### Changed
- Updated developer documentation (PR 72).
- When getting Tasks from Workflow, pre-query for initial Task ID. Performance optimization (PR 70).
- Changed hash function from sha1 to sha256, in attempt to fix task hash conflict bug (PR 78).
- Increased hashing columns from VARCHAR(50) to VARCHAR(150) (PR78).
- Changed upstream_node_id and downstream_node_id columns in edge table from TEXT to JSON (PR 84).
- Switch Jobmon deployment to be a one-click deployment. Created pipelines for dev, stage, and prod (jobmon_ihme_tad PR 98).
- Consolidate Jobmon GUI deployment pipeline in to main Jobmon pipeline (jobmon_ihme_tad PR 113)

### Fixed
- Linting, typechecking and the test suite are passing (PRs 74, 75, 80, 81, 82, 83).
- Auto-build of ReadTheDocs is working again (PR 77).
- Fixed the Jobmon GUI local frontend and backend deployment (PR 87).
- Fixed bug where the usage integrator was pulling wrong resource values from the Slurm Accounting DB (jobmon_slurm PR 154).

### Removed
- Removed cluster_type_id and cluster_id columns from the task_instance table in the database (PR 80).
- Removed Bifrost package from the Jobmon GUI (PR 94).
- Removed the `jobmon_ihme` CLI command.

## [3.2.5] - 2023-11-14
### Changed
- Added read timeout to the should_retry_exception() function (PR 68).

## [3.2.4] - 2023-09-19
### Fixed
- Allow users to not specify default resource scales.

## [3.2.3] - 2023-09-19
### Added
- Added an implementation for passing in iterators or callables for task resources, for more fine-grained scaling behavior.

## [3.2.2] - 2023-08-31
### Changed
- Users are now able to log to LIMITED_USE directory.

## [3.2.1] - 2023-03-08
Performance release.

No backwards incompatible changes; no need to change your code unless you want to use one of the new features.

### Changed
- Tested for task templates up to 200k.
- Last 10k characters of stdout now stored in db for each task_instance.
- Switch to asyncio to manage subprocess in worker_node
- move stdout_log and stderr_log to task_instance table  

## [3.2.0] - 2022-12-01
	
Internal Cleanliness, performance, and small feature-fixes

No backwards incompatible changes; no need to change your code unless you want to use one of the new features.

### Added
- Allow users to resume a workflow "from the database" using the workflow-id.
- Attributes can be added from customer code on the worker node
- Workflow now has a Tool property.
- Jobmon will warn if you are attempting to log to the Limited Use drive.
- Full CI/CD pipeline with integration tests on a deployed dev instance.
- Server URLs include Jobmon version number for cleaner routing
### Removed
- Removed all references to QSUB etc from error messages.
- Remove all sql-alchemy ORM calls for improved performance.
- No raw sql; allowing testing on any sql database without the flakey ephemera container.
### Changed
- workflow_status command can return fewer rows.
- Tool is now an optional argument when adding Tasks to a workflow.
- Immediately stringify all arguments passed by Customer to quickly detect non-string/numeric arguments.
- Usage integrator runs faster and is more resilient.
- Single Traefik instance with DNS routing rule that simplify client configuration and hot deploys.
## [3.1.5] - 2022-08-15
Urgent performance fixes.
### Fixed
- Fix inefficient update statement causing lock contention causing 502's and 504's in array.transition_to_launched
- Fix several bugs in usage_integrator
### Changed
- Limit number of rows returned on the server side for workflow_status
- Environment variable SLURM_CPUS_PER_TASK always set

[Unreleased]: https://github.com/ihmeuw-scicomp/jobmon/tree/release/3.2
[3.2.4]: https://github.com/ihmeuw-scicomp/jobmon/tree/client-3.2.4
[3.2.3]: https://github.com/ihmeuw-scicomp/jobmon/tree/client-3.2.3
[3.2.2]: https://github.com/ihmeuw-scicomp/jobmon/tree/client-3.2.2
[3.2.1]: https://github.com/ihmeuw-scicomp/jobmon/tree/client-3.2.1

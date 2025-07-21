# Changelog

All notable changes to Jobmon will be documented in this file.


## [Unreleased]
### Added
- Added async retry support to Requester and modernized DistributorService for improved error handling and performance.
- Added UV for dependency and workflow management, replacing pip-tools for faster and more reliable dependency resolution.
- Added configurable database connection pool settings to prevent timeout errors in high-load scenarios.
- Added enhanced OpenTelemetry database instrumentation with error capture for better observability.
- Added JUnit XML report generation for test dashboard integration.
- Added consolidated JSON logging fixture for server tests to improve test maintainability.
- Added unified V3 API endpoint for task template resource usage statistics with comprehensive metrics including min, max, mean, and percentile calculations.
- Added missing `/array/{array_id}/transition_to_killed` endpoint to V3 API for proper KILL_SELF task status processing, completing V3 API feature parity with V2.
- Added `Task Name` to the tooltip in the resource usage scatter plot.
- Enabled filtering by `Task Name` in the resource usage scatter plot.
- Added a `Download CSV button` to the resource usage page, allowing users to export all plot data regardless of filters.

### Changed
- Overhauled frontend resource utilization page for better performance and user experience.
- Migrated project dependency management from pip-tools to UV workspace configuration.
- Consolidated database session management and configuration for improved consistency and performance.
- Enhanced Python client to handle string confidence interval parameters and new unified resource usage response format.
- Updated Workflow.add_tasks() parameter type from Sequence[Task] to Iterable[Task] to accept a broader range of iterable types including generators and iterators. (PR 279)
- Expanded error message when a TaskInstance doesn't report a heartbeat.
- Refactored task status update endpoint into smaller functions for better maintainability.

### Fixed
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

### Deprecated
### Removed

## [3.4.24] - TBD
### Changed
- Updated Workflow.add_tasks() parameter type from Sequence[Task] to Iterable[Task] to accept a broader range of iterable types including generators and iterators. (PR 279)

### Fixed
- Fixed ClientDisconnect exceptions appearing as errors in APM by adding global exception handler. (PR 282)
- Fixed get_max_concurrently_running endpoint to handle non-existent workflows gracefully by returning a 404 error with descriptive message instead of raising an exception. (PR 278)
- Fixed 'Set' object is not subscriptable in CLI error in `/update_statuses` route.

### Fixed
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

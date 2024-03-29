# Changelog

All notable changes to Jobmon will be documented in this file.

## [Unreleased]
### Added
### Changed
### Fixed
### Deprecated
### Removed

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

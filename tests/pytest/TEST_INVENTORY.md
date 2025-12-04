# Jobmon Test Inventory

This document provides a comprehensive inventory of all tests in the `tests/pytest` directory.
The goal is to understand test coverage, identify overlaps, and plan for reorganization and simplification.

## Summary Statistics

| Directory | Test Files | Test Count | Focus Area |
|-----------|-----------|------------|------------|
| `core/` | 6 | ~20 | Configuration, utilities, requester, cluster |
| `plugins/` | 2 | 2 | Distributor plugins |
| `distributor/` | 5 | 12 | Task distribution logic |
| `workflow_reaper/` | 3 | 10 | Workflow cleanup |
| `swarm/` | 3 | 14 | Swarm-specific tests |
| `end_to_end/` | 2 | 12 | Full workflow tests |
| `cli/` | 3 | 44 | CLI commands and routes |
| `worker_node/` | 2 | 59 | Task execution, generators |
| `server/` | 8 | ~90 | Server routes, database (logging moved) |
| `client/` | 19 | ~335 | Client-side logic (logging moved) |
| `logging/` | 15 | ~150 | **ALL logging tests consolidated** |
| **Total** | **68** | **~748** | |

---

## Directory: `core/`

**Purpose**: Tests for jobmon core infrastructure - configuration system, logging, OTLP telemetry, and utilities.

**Dependencies**: 
- `client_env` fixture (most tests)
- `db_engine` fixture (cluster_queue)
- External: `structlog`, `aiohttp`, `requests`

### File: `test_cluster.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_plugin_loading` | unit | Verifies cluster plugin loading mechanism |
| `test_get_queue` | unit | Tests queue retrieval and caching from cluster |

**Fixtures**: `client_env`

---

### File: `test_cluster_queue.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_cluster_queue` | integration | Tests cluster_type, cluster, and queue database structure via HTTP API |

**Fixtures**: `db_engine`, `client_env`  
**Notes**: Creates test data directly in DB, validates via API

---

### File: `test_component_logging.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestComponentLogging.test_configure_component_logging_with_template` | unit | Component logging with default YAML template |
| `TestComponentLogging.test_configure_component_logging_generates_programmatic_config` | unit | Component logging generates config for known components |
| `TestComponentLogging.test_configure_component_logging_with_file_override` | unit | File override takes precedence |
| `TestComponentLogging.test_configure_component_logging_with_section_override` | unit | Section override merging |
| `TestComponentLogging.test_configure_component_logging_invalid_config` | unit | Graceful handling of invalid config |
| `TestComponentLogging.test_get_component_template_path` | unit | Template path resolution for components |
| `TestComponentCLI.test_cli_with_component_name` | unit | CLI auto-configures logging with component_name |
| `TestComponentCLI.test_cli_without_component_name` | unit | CLI skips logging when component_name is None |
| `TestComponentCLI.test_cli_logging_failure_does_not_crash` | unit | CLI continues when logging config fails |

**Fixtures**: `tmp_path`  
**Notes**: Uses mocking extensively for configuration paths

---

### File: `test_configuration.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_basic_configuration_methods` | unit | Basic JobmonConfig get/get_int methods |
| `test_environment_variable_overrides` | unit | Env var overrides and interpolation |
| `test_section_retrieval_with_overrides` | unit | get_section with env var overlays |
| `test_configuration_precedence` | unit | Precedence: dict_config > env vars > file |
| `test_nested_environment_variables_comprehensive` | unit | Deep nested env var handling |
| `test_db_pool_configuration_comprehensive` | unit | Database pool configuration parsing |
| `test_type_coercion_comprehensive` | unit | Type coercion (bool/int/float) |
| `test_type_coercion_with_environment_variables` | unit | Env var values coerced to correct types |
| `test_engine_integration_with_coerced_configuration` | integration | Actual engine logic with coerced config |
| `test_error_handling` | unit | ConfigError for invalid types |
| `test_empty_pool_configuration_engine_compatibility` | integration | Empty/null pool configs don't break engine |
| `test_conflicting_environment_variables_primitive_vs_nested` | unit | Graceful handling of conflicting env vars |

**Fixtures**: `tmp_path`, `monkeypatch`  
**Notes**: Comprehensive coverage of configuration precedence and type coercion

---

### File: `test_configuration_overrides.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestConfigurationOverrides.test_file_based_override_precedence` | unit | File overrides take precedence over section overrides |
| `TestConfigurationOverrides.test_section_based_override_merging` | unit | Section overrides merge with default templates |
| `TestConfigurationOverrides.test_override_system_with_missing_file` | unit | Graceful fallback when override file doesn't exist |
| `TestConfigurationOverrides.test_deep_merge_behavior` | unit | Deep merge of nested logging configurations |
| `TestConfigurationOverrides.test_override_with_programmatic_base` | unit | Overrides work with programmatic base config |
| `TestEnvironmentVariableOverrides.test_env_var_file_override` | unit | Env vars can override file paths |
| `TestEnvironmentVariableOverrides.test_env_var_section_override` | unit | Env vars can set nested section values |
| `TestConfigurationIntegration.test_configure_logging_with_overrides_function` | integration | Main configure_logging_with_overrides function |
| `TestConfigurationIntegration.test_get_logconfig_examples` | unit | Configuration examples are available and valid |

**Fixtures**: `tmp_path`, `monkeypatch`  
**Notes**: Tests logconfig override system extensively

---

### File: `test_jobmon_context.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_jobmon_metadata_injected_only_for_jobmon_loggers` | unit | Telemetry metadata isolation by logger prefix |
| `test_bind_jobmon_context_manager_resets_metadata` | unit | Context manager properly sets/resets metadata |
| `test_custom_telemetry_prefixes` | unit | Custom telemetry prefix configuration |
| `test_bind_restores_previous_values` | unit | Context manager restores previous values |
| `test_jobmon_as_library_with_fhs_style_config` | integration | Jobmon as library with external structlog config |
| `test_prepend_jobmon_processors_enforces_correct_ordering` | unit | Processor chain ordering enforcement |
| `test_lazy_configuration_allows_host_to_configure_first` | integration | Lazy config allows host to configure structlog first |

**Fixtures**: None (uses setup_function/teardown_function)  
**Notes**: Critical for telemetry isolation - ensures Jobmon metadata doesn't leak to non-Jobmon loggers

---

### File: `test_jobmon_utils.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_true_path` | unit | Path resolution utility (cwd, home expansion, executable lookup) |

**Fixtures**: None  
**Notes**: Simple utility function test

---

### File: `test_otlp.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestJobmonOTLPManager.test_singleton_behavior` | unit | OTLP manager follows singleton pattern |
| `TestJobmonOTLPManager.test_initialization_without_otlp` | unit | Graceful handling when OTLP not available |
| `TestJobmonOTLPManager.test_initialization_with_otlp` | unit | Successful initialization with OTLP |
| `TestJobmonOTLPManager.test_get_tracer` | unit | Tracer creation from manager |
| `TestJobmonOTLPManager.test_class_method_instrumentations` | unit | Class method instrumentations (requests) |
| `TestJobmonOTLPManager.test_instrumentations_without_otlp` | unit | Instrumentations gracefully handle missing OTLP |
| `TestJobmonOTLPManager.test_initialize_jobmon_otlp_function` | unit | Main initialization function |
| `TestJobmonOTLPManager.test_flush_and_shutdown` | unit | Flush and shutdown providers |
| `TestJobmonOTLPManager.test_otlp_flush_on_exit_context_manager` | unit | Context manager flushes on exit |
| `TestOTLPHandlers.test_handler_with_dict_config` | unit | Handler initialization with dict config |
| `TestOTLPHandlers.test_handler_with_preconfigured_exporter` | unit | Handler with pre-configured exporter |
| `TestOTLPHandlers.test_handler_lazy_initialization` | unit | Lazy initialization on first emit |
| `TestOTLPHandlers.test_handler_without_otlp_available` | unit | Graceful handling when OTLP unavailable |
| `TestOTLPHandlers.test_structlog_handler` | unit | Structlog OTLP handler |
| `TestOTLPHandlers.test_structlog_handler_without_structlog` | unit | Fallback when structlog not available |
| `TestOTLPUtilities.test_get_current_span_details_without_otlp` | unit | Span details when OTLP not available |
| `TestOTLPUtilities.test_get_current_span_details_with_span` | unit | Span details extraction |
| `TestOTLPUtilities.test_add_span_details_processor` | unit | Structlog processor for span details |
| `TestOTLPUtilities.test_add_span_details_processor_no_span` | unit | Processor when no span available |
| `TestJobmonOTLPFormatter.test_formatter_adds_span_details` | unit | Formatter adds span details to records |
| `TestOTLPLogconfigIntegration.test_handler_in_logconfig_with_templates` | integration | OTLP handlers in logconfig |
| `TestOTLPLogconfigIntegration.test_handler_with_template_formatter` | integration | OTLP handler with template formatter |
| `TestOTLPConfigurationOverrides.test_requester_otlp_tracing_only` | unit | Requester OTLP only sets up tracing |
| `TestOTLPConfigurationOverrides.test_create_log_exporter_factory` | unit | Log exporter factory function |
| `TestOTLPErrorHandling.test_manager_resilient_to_initialization_failures` | unit | Manager handles init failures gracefully |
| `TestOTLPErrorHandling.test_handler_resilience_to_creation_failures` | unit | Handler resilient to creation failures |

**Fixtures**: None (uses mocking)  
**Notes**: Comprehensive OTLP functionality testing with extensive mocking

---

### File: `test_requester.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_retries` (parametrized x2) | unit | HTTP request retry logic with various scenarios |
| `test_connection_retry` | unit | Connection retry after ConnectionError |
| `test_fail_fast` | unit | 4xx errors fail immediately without retries |
| `test_non_tenacious_request` | unit | Non-tenacious requests fail without retries |
| `test_connection_timeout` (parametrized x2) | unit | Timeout handling with/without tenacity |
| `test_async_retries` (parametrized x2) | unit | Async retry functionality |
| `test_async_connection_retry` | unit | Async connection retry |
| `test_async_fail_fast` | unit | Async 4xx fail fast |
| `test_async_non_tenacious_request` | unit | Async non-tenacious requests |
| `test_async_timeout` (parametrized x2) | unit | Async timeout handling |
| `test_async_get_content_json` | unit | Async JSON response parsing |
| `test_async_get_content_malformed_json` | unit | Async malformed JSON handling |
| `test_async_get_content_non_json` | unit | Async non-JSON response handling |
| `test_async_request_types` | unit | Async HTTP method support (POST/GET/PUT) |

**Fixtures**: `client_env`, `mocker`  
**Notes**: Good coverage of sync and async HTTP client behavior

---

### File: `test_structlog_detection.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_direct_detection_with_printlogger_subclass` | unit | Detection of PrintLogger subclass |
| `test_direct_detection_with_wrapped_factory_attribute` | unit | Detection with wrapped factory |
| `test_direct_detection_with_partial_wrapping` | unit | Detection with functools.partial wrapping |
| `test_stdlib_detection_still_true` | unit | stdlib LoggerFactory detection |
| `test_build_processor_chain_for_stdlib_integration` | unit | Processor chain for stdlib integration |
| `test_build_processor_chain_for_direct_rendering` | unit | Processor chain for direct rendering |

**Fixtures**: None  
**Notes**: Tests structlog integration detection and processor chain building

---

### File: `test_template_loader.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestProgrammaticConfigGeneration.test_shared_formatters_defined` | unit | Shared formatters correctly defined |
| `TestProgrammaticConfigGeneration.test_shared_handlers_generated` | unit | Shared handlers correctly generated |
| `TestTemplateLoader.test_yaml_config_loading` | unit | YAML config file loading |
| `TestTemplateLoader.test_template_loading_from_different_packages` | unit | Template loading from various package locations |
| `TestTemplateLoader.test_template_loading_graceful_failure` | unit | Graceful handling when templates dir missing |
| `TestTemplateLoader.test_missing_template_reference_handling` | unit | Handling of missing template references |
| `TestTemplateIntegration.test_client_config_generated_programmatically` | unit | Client config programmatic generation |
| `TestTemplateIntegration.test_server_config_generated_programmatically` | unit | Server config programmatic generation |

**Fixtures**: `tmp_path`  
**Notes**: Tests logging configuration template system

---

## Directory: `plugins/`

**Purpose**: Tests for distributor plugin implementations (sequential and multiprocess).

**Dependencies**: 
- `client_env`, `tool`, `task_template`, `array_template` fixtures

### File: `test_sequential_distributor.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_seq_kill_self_state` | unit | Tests sequential distributor handles kill_self state correctly |

**Fixtures**: None  
**Notes**: Tests exit info retrieval for sequential distributor

---

### File: `test_multiprocess_distributor.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_multiprocess_distributor` | integration | Tests multiprocess distributor with consumers for non-array and array operations |

**Fixtures**: `tool`, `client_env`, `task_template`, `array_template`  
**Notes**: Tests task queue consumption and array submission handling

---

## Directory: `distributor/`

**Purpose**: Tests for task distribution service - instantiation, heartbeats, triaging, and error handling.

**Dependencies**: 
- `tool`, `db_engine`, `task_template`, `client_env` fixtures
- Uses `swarm_test_utils` helpers extensively

### File: `test_heartbeat.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_heartbeat_on_launched` | integration | Tests heartbeat logging for launched task instances |

**Fixtures**: `tool`, `db_engine`, `task_template`  
**Notes**: Verifies report_by_date is updated correctly after heartbeat

---

### File: `test_instantiate.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_instantiate_job` | integration | Tests task instantiation and execution flow to DONE status |
| `test_instantiate_array` | integration | Tests array task instantiation on multiprocess distributor |
| `test_job_submit_raises_error` | integration | Tests graceful handling when executor raises error (→ W state) |
| `test_array_submit_raises_error` | integration | Tests array submission error handling |
| `test_workflow_concurrency_limiting` | integration | Tests max_concurrently_running workflow limit |
| `test_array_concurrency` (parametrized x4) | integration | Tests array-level concurrency limits with various combinations |
| `test_dynamic_concurrency_limiting` | integration | Tests CLI-driven dynamic concurrency adjustment |

**Fixtures**: `tool`, `db_engine`, `task_template`, `array_template`  
**Notes**: Comprehensive instantiation testing with various distributors

---

### File: `test_killed.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_transition_to_killed` | integration | Tests KILL_SELF → ERROR_FATAL transition flow |

**Fixtures**: `tool`, `db_engine`, `task_template`  
**Notes**: Tests distributor handling of killed tasks

---

### File: `test_queued.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_queued` | integration | Tests queued job retrieval respects concurrency limits |

**Fixtures**: `tool`, `task_template`  
**Notes**: Uses DummyDistributor for isolated testing

---

### File: `test_triaging.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_set_status_for_triaging` | integration | Tests task status transitions to NO_HEARTBEAT/TRIAGING |
| `test_triaging_to_specific_error` (parametrized x3) | integration | Tests triaging resolves to specific error states (RESOURCE_ERROR, UNKNOWN_ERROR, ERROR_FATAL) |

**Fixtures**: `tool`, `db_engine`, `task_template` (custom fixtures)  
**Notes**: Tests error classification during triaging phase

---

## Directory: `workflow_reaper/`

**Purpose**: Tests for workflow cleanup service - handles lost/halted/aborted workflows.

**Dependencies**: 
- `db_engine`, `tool`, `client_env`, `requester_no_retry` fixtures
- `WorkflowReaper` class from server module

### File: `test_notifiers.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_no_raise` | unit | Tests SlackNotifier doesn't raise on failure |

**Fixtures**: None  
**Notes**: Ensures notifier failures don't crash the reaper

---

### File: `test_reaper_route.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_fix_status_inconsistency` | integration | Tests /fix_status_inconsistency route |
| `test_workflow_name_and_args` | integration | Tests /workflow_name_and_args route |
| `test_lost_workflow_run` | integration | Tests /lost_workflow_run route |
| `test_reap_workflow_run` | integration | Tests /workflow_run/{id}/reap route |

**Fixtures**: `db_engine`, `tool`  
**Notes**: Route-level integration tests for reaper API endpoints

---

### File: `test_workflow_reaper.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_error_state` | integration | Tests reaper detects workflows without heartbeat (→ FAILED) |
| `test_halted_state` | integration | Tests reaper handles COLD_RESUME/HOT_RESUME (→ HALTED) |
| `test_aborted_state` | integration | Tests reaper handles unbound workflows (→ ABORTED) |
| `test_reaper_version` | integration | Tests reaper version checking for lost workflow runs |
| `test_inconsistent_status` | integration | Tests reaper repairs F vs D status inconsistencies |

**Fixtures**: `db_engine`, `client_env`, `requester_no_retry`, `tool`, `sleepy_task_template`  
**Notes**: Core reaper logic testing - comprehensive state transition coverage

---

## Directory: `swarm/`

**Purpose**: Tests for swarm execution mode - the async workflow execution system.

**Dependencies**: 
- `tool`, `task_template`, `db_engine`, `client_env` fixtures
- `swarm_test_utils.py` helper module for test utilities

### File: `swarm_test_utils.py`

**Not a test file** - Contains helper functions for swarm tests:
- `create_test_context()` - Build state, gateway, orchestrator for testing
- `create_builder()` - Create SwarmBuilder from workflow
- `set_initial_fringe()` - Populate ready_to_run with tasks
- `queue_tasks()` / `queue_tasks_async()` - Queue ready tasks to server
- `prepare_and_queue_tasks()` - Convenience combo function
- `synchronize_state()` - Sync local state with server
- `set_validated_task_resources()` / `set_adjusted_task_resources()` - Resource management

---

### File: `test_swarm.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_blocking_update_timeout` | integration | Workflow run timeout with appropriate error message |
| `test_sync_statuses` | integration | Status updates propagated to swarm objects |
| `test_wedged_dag` | integration | Wedged workflow recovery via full sync |
| `test_fail_fast` | integration | fail_fast parameter stops execution on error |
| `test_propagate_result` | integration | Result propagation through multi-layer DAG |
| `test_callable_returns_valid_object` | unit | Dynamic resource callable returns correct params |
| `test_callable_returns_wrong_object` | unit | Callable returning invalid object raises error |
| `test_callable_fails_bad_filepath` | unit | Exception in callable propagates correctly |
| `test_swarm_fails` | integration | Swarm exits on error appropriately |
| `test_swarm_terminate` | integration | COLD_RESUME signal terminates workflow gracefully |
| `test_build_swarm_from_workflow_id` | integration | SwarmBuilder builds from workflow_id for resume |

**Fixtures**: `tool`, `task_template`, `db_engine`, `client_env`, `requester_no_retry`  
**Notes**: Comprehensive swarm behavior testing including edge cases

---

### File: `test_swarm_task.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_swarmtask_resources_integration` (parametrized x3) | integration | Task resources passed to swarmtask with various scale types |
| `test_swarmtask_resources_integration_no_scales` (parametrized x2) | integration | Empty resource_scales are a no-op |

**Fixtures**: `tool`, `task_template`, `db_engine`  
**Notes**: Tests resource scaling and validation for swarm tasks

---

### File: `test_swarm_workflow_run.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestWorkflowRunTimeout.test_timeout_logic_directly` | unit | Timeout logic unit test without complex mocking |

**Fixtures**: None  
**Notes**: Simple unit test for timeout calculation logic

---

## Directory: `end_to_end/`

**Purpose**: Full workflow execution tests - complete workflow lifecycle testing.

**Dependencies**: 
- `client_env`, `tool`, `task_template` fixtures
- Custom task templates with failure behaviors
- External script `remote_sleep_and_write.py`

### File: `test_simple_workflows.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_one_task` | e2e | Single task workflow runs to completion |
| `test_two_tasks_same_command_error` | e2e | Duplicate task hash raises ValueError |
| `test_three_linear_tasks` | e2e | Linear DAG (a→b→c) execution |
| `test_fork_and_join_tasks` | e2e | Fork/join DAG (a→b[0..2]→c[0..2]→d) |
| `test_fork_and_join_tasks_with_fatal_error` | e2e | Fork/join with one task fatally failing |
| `test_fork_and_join_tasks_with_retryable_error` | e2e | Fork/join with retriable error completes |
| `test_bushy_real_dag` | e2e | Complex DAG with cross-phase dependencies |

**Fixtures**: `client_env`, `tool`, `task_template`, `tmpdir`  
**Notes**: Comprehensive DAG structure testing with various failure modes

---

### File: `test_workflow_resume.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_fail_one_task_resume` | e2e | Failed workflow resumes and completes |
| `test_cold_resume` | e2e | COLD_RESUME signal handling and recovery |
| `test_hot_resume` | e2e | HOT_RESUME signal handling |
| `test_stopped_resume` | e2e | Keyboard interrupt recovery and resume |

**Fixtures**: `client_env`, `tool`, `task_template`, `task_template_fail_one`, `tmpdir`  
**Notes**: Tests all resume scenarios (cold, hot, stopped)

---

### File: `test_logging.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_sequential_logging` | e2e | Sequential distributor stdout/stderr logging |
| `test_multiprocess_logging` | e2e | Multiprocess distributor stdout/stderr logging |
| `test_dummy_executor_with_bad_log_path` | e2e | Dummy executor ignores bad log paths |

**Fixtures**: `tool`, `task_template`, `tmp_path`  
**Notes**: Tests task output capture to files

---

### File: `test_scheduler_logging.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_scheduler_logging` | e2e | Scheduler logs to stdout correctly |

**Fixtures**: `client_env`, `caplog`  
**Notes**: Currently SKIPPED - tests scheduler log output

---

## Directory: `integration/`

**Purpose**: Cross-component logging integration tests.

**Dependencies**: 
- `tmp_path`, `client_env` fixtures
- Heavy use of mocking for configuration testing

### File: `test_logging_integration.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestCrossComponentConsistency.test_programmatic_config_consistency_across_components` | integration | Programmatic configs consistent across all components |
| `TestCrossComponentConsistency.test_component_configs_generated_successfully` | integration | All component configs generated programmatically |
| `TestCrossComponentConsistency.test_otlp_handler_consistency` | integration | OTLP handlers use consistent settings |
| `TestEndToEndLoggingScenarios.test_client_to_server_logging_flow` | integration | Client and server logging work together |
| `TestEndToEndLoggingScenarios.test_complete_otlp_workflow` | integration | Complete OTLP workflow from client to server |
| `TestEndToEndLoggingScenarios.test_logging_configuration_with_global_overrides` | integration | Global configuration overrides |
| `TestProductionScenarios.test_mixed_override_deployment_scenario` | integration | Realistic mixed override deployment |
| `TestProductionScenarios.test_configuration_error_recovery` | integration | Graceful recovery from config errors |
| `TestProductionScenarios.test_otlp_collector_unavailable_scenario` | integration | Behavior when OTLP collector unavailable |

**Fixtures**: `tmp_path`  
**Notes**: Extensive logging system integration testing

---

### File: `test_client_logging_integration.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestClientLoggingIntegration.test_client_cli_enables_automatic_logging` | integration | Client CLI auto-configures logging |
| `TestClientLoggingIntegration.test_client_logging_with_programmatic_config` | integration | Client uses programmatic config as base |
| `TestClientLoggingIntegration.test_client_logging_with_file_override` | integration | Client logging with file override |
| `TestClientLoggingIntegration.test_client_logging_failure_does_not_crash` | integration | Logging failures don't crash client |
| `TestClientLoggingIntegration.test_workflow_component_logging_method` | integration | Workflow _configure_component_logging method |
| `TestClientLoggingIntegration.test_workflow_configure_logging_option` | integration | workflow.run(configure_logging=True) |
| `TestClientLoggingIntegration.test_client_logging_consistency_with_other_components` | integration | Client CLI follows same pattern as other CLIs |

**Fixtures**: `tmp_path`  
**Notes**: Client-specific logging integration

---

### File: `test_server_logging_integration.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestServerLoggingIntegration.test_server_cli_enables_automatic_logging` | integration | Server CLI auto-configures logging |
| `TestServerLoggingIntegration.test_server_web_app_logging_integration` | integration | Server web app auto-configures logging |
| `TestServerLoggingIntegration.test_server_logging_with_custom_config` | integration | Server logging with custom config |
| `TestServerLoggingIntegration.test_server_logging_fallback_behavior` | integration | Server logging graceful fallback |
| `TestServerLoggingIntegration.test_server_logging_with_section_override` | integration | Server logging with section override |
| `TestServerLoggingIntegration.test_server_otlp_validation_integration` | integration | Server OTLP configuration works correctly |
| `TestServerLoggingIntegration.test_server_advanced_precedence_levels` | integration | Server config precedence (dict > file > config) |
| `TestServerLoggingIntegration.test_server_performance_characteristics` | integration | Server startup time characteristics |
| `TestServerLoggingIntegration.test_server_logging_consistency_with_other_components` | integration | Server CLI follows same pattern as others |

**Fixtures**: `tmp_path`  
**Notes**: Server-specific logging integration

---

### File: `test_distributor_logging_integration.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestDistributorLoggingIntegration.test_distributor_cli_enables_automatic_logging` | integration | Distributor CLI auto-configures logging |
| `TestDistributorLoggingIntegration.test_distributor_logging_with_template` | integration | Distributor logging with template |
| `TestDistributorLoggingIntegration.test_distributor_logging_with_file_override` | integration | Distributor logging with file override |
| `TestDistributorLoggingIntegration.test_distributor_logging_failure_does_not_crash` | integration | Logging failures don't crash distributor |
| `TestDistributorLoggingIntegration.test_distributor_cli_flushes_otlp_on_exit` | integration | Distributor flushes OTLP on exit |
| `TestDistributorLoggingIntegration.test_distributor_logging_with_section_override` | integration | Distributor logging with section override |

**Fixtures**: `tmp_path`  
**Notes**: Distributor-specific logging integration

---

### File: `test_worker_logging_integration.py`

| Test | Category | Description |
|------|----------|-------------|
| `TestWorkerLoggingIntegration.test_worker_cli_enables_automatic_logging` | integration | Worker CLI auto-configures logging |
| `TestWorkerLoggingIntegration.test_worker_logging_with_template` | integration | Worker logging with template |
| `TestWorkerLoggingIntegration.test_worker_logging_with_file_override` | integration | Worker logging with file override |
| `TestWorkerLoggingIntegration.test_worker_logging_failure_does_not_crash` | integration | Logging failures don't crash worker |
| `TestWorkerLoggingIntegration.test_worker_cli_flushes_otlp_on_exit` | integration | Worker flushes OTLP on exit |
| `TestWorkerLoggingIntegration.test_worker_logging_with_section_override` | integration | Worker logging with section override |
| `TestWorkerLoggingIntegration.test_worker_short_lived_process_characteristics` | integration | Worker optimized for short-lived processes |
| `TestWorkerLoggingIntegration.test_worker_performance_startup_time` | integration | Worker startup time < 1 second |

**Fixtures**: `tmp_path`  
**Notes**: Worker-specific logging integration, optimized for short-lived processes

---

## Directory: `cli/`

**Purpose**: CLI command tests for workflow status, management, and resource API.

**Dependencies**: 
- `client_env`, `db_engine`, `tool`, `task_template` fixtures

### File: `test_cli_routes.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_task_template_resources_route` | integration | Tests task template resources retrieval via API |
| `test_tt_resources_with_cluster` | integration | Tests task template resources with cluster filtering |
| `test_subdag_resources_route` | integration | Tests subdag resource retrieval |
| `test_subdag_tasks_route` | integration | Tests subdag tasks retrieval |
| `test_workflow_tasks_route` | integration | Tests workflow tasks retrieval |
| ~10 more parameterized tests | integration | Various CLI route edge cases |

### File: `test_resource_api.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_get_task_template_resources` | integration | Tests GET endpoint for task template resources |
| `test_get_subdag_resources` | integration | Tests subdag resource retrieval API |
| `test_get_workflow_tasks` | integration | Tests workflow tasks retrieval API |
| ~5 more tests | integration | Resource API tests |

### File: `test_status_commands.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_workflow_status_command` | unit | Tests `workflow_status` CLI command |
| `test_task_status_command` | unit | Tests `task_status` CLI command |
| `test_workflow_details_command` | unit | Tests `workflow_details` CLI command |
| `test_concurrency_limit_command` | unit | Tests `concurrency_limit` CLI command |
| `test_resume_command` | unit | Tests `resume` CLI command |
| `test_stop_command` | unit | Tests `stop` CLI command |
| ~20 more parameterized tests | unit | Various status command edge cases |

**Notes**: Heavy use of click testing utilities and mocked API calls.

---

## Directory: `worker_node/`

**Purpose**: Task execution on worker nodes, task generators, and task instance lifecycle.

**Dependencies**: 
- `client_env`, `db_engine`, `tool`, `task_template` fixtures

### File: `test_task_generator.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_basic_task_generation` | unit | Tests basic task generation from workflow |
| `test_task_with_dependencies` | unit | Tests task generation with upstream dependencies |
| `test_array_task_generation` | unit | Tests generation of array tasks |
| `test_task_generator_iterator` | unit | Tests iterator protocol for task generation |
| ~50 more tests | unit/integration | Various task generation scenarios |

### File: `test_task_instance_worker_node.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_task_instance_creation` | unit | Tests task instance creation on worker |
| `test_task_instance_run` | integration | Tests running a task instance |
| `test_task_instance_status_reporting` | integration | Tests status reporting back to server |
| `test_task_instance_retry_logic` | integration | Tests retry behavior on failure |
| ~8 more tests | integration | Task instance lifecycle tests |

**Notes**: Tests the worker-side execution of tasks.

---

## Directory: `server/`

**Purpose**: Server-side routes, database schema, authentication, and business logic.

**Dependencies**: 
- `client_env`, `db_engine`, `tool`, `task_template` fixtures

### File: `test_database_schema.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_workflow_table_schema` | unit | Tests workflow table structure |
| `test_task_table_schema` | unit | Tests task table structure |
| `test_foreign_key_constraints` | unit | Tests FK relationships |
| ~10 more tests | unit | Schema validation tests |

### File: `test_db_deps.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_get_session_dependency` | unit | Tests database session dependency injection |
| `test_session_commit_on_success` | unit | Tests session commit behavior |
| `test_session_rollback_on_error` | unit | Tests session rollback on exceptions |

### File: `test_dns.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_dns_resolution` | unit | Tests DNS resolution for cluster hosts |
| `test_dns_caching` | unit | Tests DNS result caching |

### File: `test_error_log_clustering.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_cluster_similar_errors` | unit | Tests error message clustering |
| `test_error_pattern_extraction` | unit | Tests extracting patterns from error logs |
| ~5 more tests | unit | Error log analysis tests |

### File: `test_invaliduse.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_invalid_workflow_id` | unit | Tests handling invalid workflow IDs |
| `test_invalid_task_id` | unit | Tests handling invalid task IDs |
| ~5 more tests | unit | Input validation tests |

### File: `test_optional_auth.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_auth_disabled_by_default` | unit | Tests auth is disabled without config |
| `test_auth_enabled_with_config` | unit | Tests auth when enabled |
| `test_auth_token_validation` | unit | Tests token validation logic |

### File: `test_server_logging.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_configure_server_logging_basic` | unit | Tests basic server logging setup |
| `test_configure_server_logging_with_dict_config` | unit | Tests logging with dict config |
| `test_server_logging_with_section_override` | unit | Tests section-based overrides |
| `test_server_logging_fallback_on_invalid` | unit | Tests graceful fallback on errors |
| ~8 more tests | unit | Server logging configuration tests |

### File: `test_server_otlp.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_otlp_tracing_setup` | integration | Tests OTLP tracing initialization |
| `test_otlp_span_creation` | integration | Tests span creation for requests |
| ~5 more tests | integration | OTLP observability tests |

### File: `repositories/test_task_template_repository.py`

| Test | Category | Description |
|------|----------|-------------|
| `test_get_task_template_by_name` | unit | Tests repository get by name |
| `test_create_task_template` | unit | Tests repository create |
| ~5 more tests | unit | Repository pattern tests |

**Notes**: Server tests use direct database access and Flask test client.

---

## Directory: `client/`

**Purpose**: Client-side API, workflow building, task creation, arrays, and swarm execution.

**Dependencies**: 
- `client_env`, `db_engine`, `tool`, `task_template` fixtures

### File: `test_workflow.py` (~18 tests)

| Test | Category | Description |
|------|----------|-------------|
| `test_wfargs_update` | integration | Tests workflow args create distinct workflows |
| `test_attempt_resume_on_complete_workflow` | integration | Tests resume blocked on completed workflow |
| `test_resume_with_old_and_new_workflow_attributes` | integration | Tests resume with attribute updates |
| `test_workflow_identical_args` | integration | Tests identical args raise error |
| `test_add_same_node_args_twice` | unit | Tests duplicate node args error |
| `test_empty_workflow` | unit | Tests empty workflow raises error |
| `test_workflow_attribute` | integration | Tests workflow attributes feature |
| `test_chunk_size` | integration | Tests chunk size configuration |
| `test_add_tasks_dependencynotexist` | unit | Tests dependency validation |
| `test_concurrency_limit` | integration | Tests max_concurrently_running |
| `TestDAGCycles` (5 tests) | unit | Tests DAG cycle detection |

### File: `test_task.py` (~15 tests)

| Test | Category | Description |
|------|----------|-------------|
| `test_good_names` | unit | Tests valid task names |
| `test_bad_names` | unit | Tests invalid task name rejection |
| `test_equality` | unit | Tests task equality comparison |
| `test_default_task_name` | unit | Tests auto-generated task names |
| `test_task_attribute` | integration | Tests task attributes feature |
| `test_reset_attempts_on_resume` | integration | Tests attempt reset on resume |
| `test_binding_tasks` | integration | Tests task binding to workflow |
| `test_default_max_attempts` | integration | Tests max_attempts hierarchy |
| `test_downstream_task` | integration | Tests downstream task API with client version compat |
| `test_node_args_hash` | integration | Tests node arg hash uniqueness |

### File: `test_array.py` (~13 tests)

| Test | Category | Description |
|------|----------|-------------|
| `test_create_array` | unit | Tests array creation |
| `test_array_bind` | integration | Tests array binding to workflow |
| `test_node_args_expansion` | unit | Tests node args cartesian product |
| `test_create_tasks` | integration | Tests create_tasks with expansion |
| `test_empty_array` | unit | Tests empty array behavior |
| `test_array_max_attempts` | integration | Tests max_attempts inheritance |
| `test_queue_task_batch_deadlock_prevention` | integration | Tests concurrent queue_task_batch |
| `test_queue_task_batch_and_log_done_concurrently` | integration | Tests concurrent queue and done |
| `test_queue_task_batch_returns_status_for_already_queued_tasks` | integration | Tests already-queued response |

**Notes**: Includes deadlock prevention tests for concurrent array operations.

### File: `test_client_logging.py` (~20 tests)

| Test | Category | Description |
|------|----------|-------------|
| `test_client_logging_default_format` | unit | Tests default log format |
| `TestClientLoggingIntegration` (7 tests) | integration | Tests logging config integration |
| `TestClientLoggingOutput` (2 tests) | unit | Tests log output format |
| `test_direct_rendering_*` (6 tests) | unit | Tests direct rendering OTLP/structlog |

**Notes**: Extensive structlog integration testing with direct vs stdlib modes.

### File: `test_task_template.py` (~7 tests)

| Test | Category | Description |
|------|----------|-------------|
| `test_task_template` | integration | Tests basic task template creation |
| `test_create_and_get_task_template` | integration | Tests template retrieval |
| `test_create_new_task_template_version` | integration | Tests template versioning |
| `test_invalid_args` | unit | Tests invalid argument detection |
| `test_task_template_resources` | integration | Tests resource hierarchy |
| `test_task_template_resources_yaml` | integration | Tests YAML resource config |
| `test_task_template_hash_unique` | integration | Tests arg mapping hash |

### File: `test_task_resources.py` (~4 tests)

| Test | Category | Description |
|------|----------|-------------|
| `test_task_resources_hash` | unit | Tests TaskResources hashing |
| `test_task_resource_bind` | integration | Tests resource binding |
| `test_defaults_pass_down_and_overrides` | integration | Tests resource inheritance |
| `test_timeunit_convert` (parameterized) | unit | Tests runtime string conversion |

### Other Client Files

| File | Tests | Description |
|------|-------|-------------|
| `test_tool.py` | 3 | Tool creation and versioning |
| `test_workflow_run.py` | 5 | WorkflowRun binding and config |
| `test_distributor_context.py` | 3 | Distributor startup/timeout |
| `test_args_type.py` | 12 | Parameterized arg type tests |
| `test_units.py` | 10 | TimeUnit and MemUnit conversion |
| `test_representation.py` | 1 | __repr__ method tests |
| `test_tool_version.py` | 1 | Tool version template loading |

### Subdirectory: `swarm/workflow_run_impl/`

| File | Tests | Description |
|------|-------|-------------|
| `test_builder.py` | ~18 | SwarmBuilder construction tests |
| `test_synchronizer.py` | ~25 | Synchronizer unit tests (triage, task updates, concurrency) |
| `test_gateway.py` | ~30 | Gateway response dataclasses and async method tests |
| `test_orchestrator.py` | ~60 | WorkflowRunOrchestrator unit tests (config, init, run, teardown) |
| `test_scheduler.py` | ~40 | Scheduler unit tests (batching, capacity, queueing) |
| `test_heartbeat.py` | ~25 | HeartbeatService unit tests (tick, background loop) |
| `test_state.py` | ~35 | SwarmState and StateUpdate unit tests |

**Notes**: The `client/swarm/` tests are pure unit tests with extensive mocking, testing the new swarm architecture components in isolation. (The `workflow_run/` directory was removed - `test_builder.py` consolidated here.)

---

## Analysis and Findings

### ✅ Issues Fixed (Phase 1 Complete)

#### 1. **Duplicate Test File** - FIXED ✅
```
tests/pytest/client/swarm/workflow_run/test_gateway.py  ← DELETED
tests/pytest/client/swarm/workflow_run_impl/test_gateway.py  ← KEPT
```
~40 duplicate tests removed.

#### 2. **Directory Structure** - FIXED ✅
```
client/swarm/workflow_run/       ← DELETED (directory removed)
client/swarm/workflow_run_impl/  ← Now contains all swarm tests (7 files)
```
`test_builder.py` moved to `workflow_run_impl/`.

#### 3. **Session-Scoped Fixtures** - FIXED ✅
```python
# conftest.py - fixtures now session-scoped:
@pytest.fixture(scope="session")
def client_env(...)   # Was function-scoped

@pytest.fixture(scope="session")  
def tool(...)         # Was function-scoped

@pytest.fixture(scope="session")
def task_template(...)  # Was function-scoped

@pytest.fixture(scope="session")
def array_template(...)  # Was function-scoped
```
Estimated speedup: ~12.5 seconds across test suite.

---

### 🟡 Overlap Analysis

#### Logging Tests (19 files, ~124 config calls)

| Location | Test Count | Focus |
|----------|------------|-------|
| `core/test_component_logging.py` | ~10 | Core `configure_component_logging` function |
| `core/test_configuration_overrides.py` | ~8 | Override system (file, section, env vars) |
| `core/test_otlp.py` | ~20 | OTLP handlers and managers |
| `client/test_client_logging.py` | ~20 | Client structlog integration |
| `server/test_server_logging.py` | ~10 | Server logging configuration |
| `server/test_server_otlp.py` | ~8 | Server OTLP tracing |
| `integration/test_logging_integration.py` | ~15 | Cross-component consistency |
| `integration/test_*_logging_integration.py` | ~20 | Component-specific integration |
| `end_to_end/test_logging.py` | ~3 | Stdout/stderr capture |
| `end_to_end/test_scheduler_logging.py` | ~1 | Scheduler log levels |

**Overlap Pattern**: Similar tests for each component (client, server, distributor, worker):
- CLI auto-configures logging
- File override works
- Section override works
- Logging failure doesn't crash
- OTLP handlers work

**Recommendation**: Create a parameterized test factory or base class for component logging.

#### Swarm Tests (283 tests total)

| Location | Tests | Type |
|----------|-------|------|
| `swarm/` | ~14 | Integration (uses server) |
| `client/swarm/workflow_run/` | ~50 | Unit (mocked) |
| `client/swarm/workflow_run_impl/` | ~219 | Unit (mocked) |

**Overlap**: `test_gateway.py` is duplicated. Heartbeat tests exist in both `distributor/` and `client/swarm/workflow_run_impl/`.

---

### 🟢 Good Patterns Observed

1. **Clear separation of unit vs integration tests**: `client/swarm/` uses mocks exclusively, while `swarm/` uses real fixtures
2. **Comprehensive async test coverage**: 106 `@pytest.mark.asyncio` tests
3. **Good parameterization**: Many tests use `@pytest.mark.parametrize` effectively
4. **Deadlock prevention tests**: `test_array.py` has concurrent operation tests

---

### 📊 Fixture Dependency Analysis

| Fixture | Usage Count | Scope | Notes |
|---------|-------------|-------|-------|
| `client_env` | 178 | function | Heavy - starts web server |
| `db_engine` | ~86 tests | session | Good - shared |
| `tool` | ~50 | function | Could be session-scoped |
| `task_template` | ~40 | function | Could be session-scoped |

**Bottleneck**: `client_env` is function-scoped but starts a web server each time. The conftest has a comment: 
```python
# TODO: This tool and the subsequent fixtures should probably be session scoped
```

**Recommendation**: Session-scope `tool` and `task_template` fixtures for significant test speedup.

---

### 🎯 Recommended Actions

#### ✅ Immediate (Quick Wins) - COMPLETED

1. ~~**Delete duplicate file**~~ ✅ Done
2. ~~**Session-scope tool/task_template fixtures**~~ ✅ Done
3. ~~**Merge `workflow_run/` into `workflow_run_impl/`**~~ ✅ Done

#### ✅ Short-term (Consolidation) - Phase 2 COMPLETED

4. ~~**Create logging test base class**~~ ✅ Done - Created `logging/` directory with ALL logging tests

**New `logging/` directory structure:**
```
tests/pytest/logging/
├── __init__.py
├── conftest.py                  # Component configurations and shared fixtures
├── test_components.py           # 30 parameterized CLI/config tests (4 components)
├── test_component_logging.py    # Core component logging utility tests
├── test_logconfig_overrides.py  # Configuration override system tests
├── test_otlp.py                 # Core OTLP manager/handler tests
├── test_structlog_context.py    # Structlog context/telemetry isolation
├── test_structlog_detection.py  # Structlog integration detection
├── test_cross_component.py      # Cross-component logging scenarios
├── test_client_logging.py       # Client-specific logging integration
├── test_server_logging.py       # Server logging configuration
├── test_server_otlp.py          # Server OTLP integration
├── test_output_capture.py       # Stdout/stderr capture (e2e)
└── test_scheduler.py            # Scheduler logging
```

**Files removed from other directories:**
- `core/`: test_otlp.py, test_jobmon_context.py, test_structlog_detection.py, test_component_logging.py, test_configuration_overrides.py
- `integration/`: ALL files (directory deleted)
- `client/`: test_client_logging.py
- `server/`: test_server_otlp.py (test_server_logging.py trimmed to request-handling tests only)
- `end_to_end/`: test_logging.py, test_scheduler_logging.py

**Result:** Single authoritative location for all logging tests (~150 tests in 15 files).

#### Medium-term (Further Consolidation) - Phase 3:
   ```python
   class ComponentLoggingTestBase:
       component_name: str
       
       def test_cli_enables_automatic_logging(self): ...
       def test_logging_with_file_override(self): ...
       def test_logging_failure_does_not_crash(self): ...
   ```

#### Medium-term (Restructuring)

5. **Consider grouping by feature rather than by layer**:
   ```
   tests/pytest/
   ├── logging/           # All logging tests
   ├── workflow/          # Workflow creation, binding, resume
   ├── execution/         # Task execution, distributor, worker
   ├── api/               # CLI and server routes
   └── core/              # Configuration, requester, utilities
   ```

6. **Add coverage gap tests**:
   - No tests for `workflow.validate()` (marked skip)
   - Limited error recovery testing
   - No performance regression tests

---

### 📈 Metrics Summary

| Metric | Before | After Phase 1 |
|--------|--------|---------------|
| Total test files | ~69 | ~67 |
| Total tests | ~739 | ~699 |
| Duplicate tests | ~40 | 0 ✅ |
| Swarm directories | 3 | 2 |
| Tests using `client_env` | 178 | 178 (now session-scoped) |
| Async tests | 106 | 106 |
| Logging-related tests | ~100 | ~100 |
| Swarm architecture tests | ~283 | ~243 |

**Phase 1 Impact:**
- Removed ~40 duplicate test runs
- Consolidated swarm tests into single directory
- Session-scoped fixtures for ~12s speedup

---

*Last updated: December 2024*
*Phase 1 completed: December 2024*


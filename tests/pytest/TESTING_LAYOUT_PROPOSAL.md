# Jobmon Test Suite Reorganization Proposal

Based on the comprehensive inventory analysis, this document proposes an optimal testing layout
designed to reduce duplication, improve maintainability, and speed up test execution.

---

## Executive Summary

| Metric | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| Test directories | 12 | 8 | -33% complexity |
| Duplicate tests | ~40 | 0 | -40 test runs |
| Logging test files | 19 | 4 | -79% fragmentation |
| Fixture inefficiency | High | Low | ~2x faster tests |
| Swarm test directories | 3 | 1 | Clear ownership |

---

## Current Problems

### 1. Fragmented Organization

```
Current Structure (12 directories, overlapping concerns):

tests/pytest/
├── cli/                        # CLI commands (but also has route tests)
├── client/                     # Client code (huge, 355 tests)
│   └── swarm/
│       ├── workflow_run/       # Duplicate of workflow_run_impl!
│       └── workflow_run_impl/  # Main swarm tests
├── core/                       # Core utilities (also has logging tests)
├── distributor/                # Distributor service
├── end_to_end/                 # E2E tests (also has logging tests)
├── integration/                # Integration (mostly logging tests)
├── plugins/                    # 2 tests only
├── server/                     # Server (also has logging tests)
├── swarm/                      # More swarm tests (overlap with client/swarm)
├── worker_node/                # Worker tests
└── workflow_reaper/            # Reaper tests
```

**Issues:**
- Logging tests scattered across 19 files in 7 directories
- Swarm tests in 3 different places (`swarm/`, `client/swarm/workflow_run/`, `client/swarm/workflow_run_impl/`)
- Duplicate `test_gateway.py` files
- `plugins/` has only 2 tests
- Unclear distinction between `integration/` and `end_to_end/`

### 2. Fixture Inefficiency

```python
# Current: function-scoped fixtures recreated for every test
@pytest.fixture  # scope="function" by default
def tool(...):
    # Creates Tool, registers task_templates with database
    # ~200ms per test

@pytest.fixture
def task_template(tool):
    # Depends on tool, also function-scoped
    # Another ~50ms per test
```

**Impact:** With 178 tests using `client_env` and ~50 using `tool`/`task_template`:
- Estimated wasted time: 50 tests × 250ms = **12.5 seconds** of unnecessary setup

### 3. Test Type Confusion

| Directory | Claims to be | Actually is |
|-----------|--------------|-------------|
| `integration/` | Integration tests | Mostly unit tests with mocks |
| `end_to_end/` | E2E tests | True E2E tests |
| `client/swarm/workflow_run_impl/` | Implementation tests | Unit tests |
| `swarm/` | Swarm tests | Integration tests |

---

## Proposed Structure

```
tests/pytest/
│
├── conftest.py                     # Global fixtures (improved scoping)
├── fixtures/                       # NEW: Shared fixture utilities
│   ├── __init__.py
│   ├── database.py                 # Database fixtures
│   ├── server.py                   # Server/client_env fixtures  
│   ├── workflows.py                # Workflow/tool/task fixtures
│   └── mocks.py                    # Shared mock utilities
│
├── unit/                           # Pure unit tests (no server needed)
│   ├── conftest.py                 # Unit test specific fixtures
│   │
│   ├── core/                       # Core library unit tests
│   │   ├── test_configuration.py   # JobmonConfig unit tests
│   │   ├── test_requester.py       # HTTP client unit tests
│   │   ├── test_utilities.py       # Path resolution, etc.
│   │   └── test_serialization.py   # Data serialization
│   │
│   ├── client/                     # Client library unit tests
│   │   ├── test_workflow.py        # Workflow building (no binding)
│   │   ├── test_task.py            # Task creation (no binding)
│   │   ├── test_array.py           # Array expansion (no binding)
│   │   ├── test_task_template.py   # Template logic
│   │   ├── test_resources.py       # Resource calculations
│   │   └── test_dag.py             # DAG cycle detection
│   │
│   ├── swarm/                      # Swarm execution unit tests
│   │   ├── test_state.py           # SwarmState unit tests
│   │   ├── test_scheduler.py       # Scheduler unit tests
│   │   ├── test_orchestrator.py    # Orchestrator unit tests
│   │   ├── test_synchronizer.py    # Synchronizer unit tests
│   │   ├── test_heartbeat.py       # HeartbeatService unit tests
│   │   └── test_gateway.py         # Gateway response parsing
│   │
│   ├── server/                     # Server logic unit tests
│   │   ├── test_repositories.py    # Repository pattern tests
│   │   ├── test_error_clustering.py # Error log clustering
│   │   ├── test_schema.py          # Database schema validation
│   │   └── test_auth.py            # Authentication logic
│   │
│   └── worker/                     # Worker logic unit tests
│       ├── test_task_generator.py  # Task generator logic
│       └── test_execution.py       # Execution logic
│
├── logging/                        # ALL logging tests consolidated
│   ├── conftest.py                 # Logging test fixtures
│   ├── test_configuration.py       # Core logging config
│   ├── test_structlog.py           # Structlog detection/integration
│   ├── test_otlp.py                # OTLP handlers/managers
│   ├── test_context.py             # Jobmon context/telemetry isolation
│   └── test_components.py          # Component-specific (parameterized!)
│       # Parameterized tests for: client, server, distributor, worker
│       # - CLI auto-configuration
│       # - File/section overrides
│       # - Graceful failure handling
│       # - OTLP flush on exit
│
├── integration/                    # Tests requiring database + server
│   ├── conftest.py                 # Integration fixtures (session-scoped)
│   │
│   ├── client/                     # Client integration tests
│   │   ├── test_workflow_binding.py    # Workflow creation + binding
│   │   ├── test_task_binding.py        # Task binding
│   │   ├── test_array_operations.py    # Array creation + concurrency
│   │   └── test_resume.py              # Workflow resume scenarios
│   │
│   ├── server/                     # Server integration tests
│   │   ├── test_routes.py          # API route tests
│   │   ├── test_database.py        # Database operations
│   │   └── test_dns.py             # DNS resolution
│   │
│   ├── distributor/                # Distributor integration tests
│   │   ├── test_instantiation.py   # Task instantiation flow
│   │   ├── test_triaging.py        # Error triaging
│   │   ├── test_heartbeat.py       # Heartbeat handling
│   │   ├── test_concurrency.py     # Concurrency limits
│   │   └── test_plugins.py         # Sequential/multiprocess distributors
│   │
│   ├── swarm/                      # Swarm integration tests
│   │   ├── test_execution.py       # Full swarm execution
│   │   ├── test_builder.py         # SwarmBuilder integration
│   │   ├── test_resources.py       # Resource scaling
│   │   └── test_termination.py     # Graceful termination
│   │
│   ├── reaper/                     # Workflow reaper tests
│   │   ├── test_reaper_logic.py    # Core reaper logic
│   │   └── test_reaper_routes.py   # Reaper API routes
│   │
│   └── cli/                        # CLI integration tests
│       ├── test_status_commands.py # Status/management commands
│       └── test_resource_api.py    # Resource management API
│
└── e2e/                            # End-to-end workflow tests
    ├── conftest.py                 # E2E specific fixtures
    ├── test_simple_workflows.py    # Basic DAG patterns
    ├── test_resume_workflows.py    # Cold/hot resume
    ├── test_error_workflows.py     # Error handling patterns
    └── test_logging_capture.py     # Stdout/stderr capture

```

---

## Key Design Decisions

### 1. Consolidate All Logging Tests

**Current:** 19 files across 7 directories with repetitive patterns

**Proposed:** Single `logging/` directory with parameterized tests

```python
# tests/pytest/logging/test_components.py

import pytest
from typing import NamedTuple

class ComponentTestConfig(NamedTuple):
    name: str
    cli_class: str
    configure_func: str
    has_otlp_flush: bool = False

COMPONENTS = [
    ComponentTestConfig("client", "ClientCLI", "configure_client_logging"),
    ComponentTestConfig("server", "ServerCLI", "configure_server_logging"),
    ComponentTestConfig("distributor", "DistributorCLI", "configure_distributor_logging", True),
    ComponentTestConfig("worker", "WorkerNodeCLI", "configure_worker_logging", True),
]

@pytest.fixture(params=COMPONENTS, ids=lambda c: c.name)
def component(request):
    return request.param


class TestComponentLogging:
    """Parameterized tests for all component logging configurations."""
    
    def test_cli_enables_automatic_logging(self, component, mocker):
        """All CLIs should call configure_component_logging."""
        mock_configure = mocker.patch("jobmon.core.config.logconfig_utils.configure_component_logging")
        # Import and instantiate CLI
        ...
        mock_configure.assert_called_once_with(component.name)
    
    def test_logging_with_file_override(self, component, tmp_path, mocker):
        """File override takes precedence for all components."""
        ...
    
    def test_logging_with_section_override(self, component, tmp_path, mocker):
        """Section override merges correctly for all components."""
        ...
    
    def test_logging_failure_does_not_crash(self, component, mocker):
        """Logging config failure doesn't crash any component."""
        ...
    
    @pytest.mark.parametrize("component", [c for c in COMPONENTS if c.has_otlp_flush], 
                             ids=lambda c: c.name, indirect=True)
    def test_otlp_flush_on_exit(self, component, mocker):
        """Components with OTLP flush do so on exit."""
        ...
```

**Result:** 
- ~80 repetitive tests → ~20 parameterized tests
- Same coverage, 75% less code
- Single place to update when logging behavior changes

### 2. Merge Swarm Test Directories

**Current:**
```
swarm/                              # Integration tests (14 tests)
client/swarm/workflow_run/          # Unit tests (50 tests) - HAS DUPLICATE
client/swarm/workflow_run_impl/     # Unit tests (219 tests)
```

**Proposed:**
```
unit/swarm/                         # Pure unit tests (mocked, ~240 tests)
integration/swarm/                  # Integration tests (~20 tests)
```

**Migration:**
1. Delete duplicate `client/swarm/workflow_run/test_gateway.py`
2. Move `client/swarm/workflow_run/test_builder.py` → `integration/swarm/test_builder.py`
3. Move `client/swarm/workflow_run_impl/*` → `unit/swarm/*`
4. Move `swarm/test_swarm.py` → `integration/swarm/test_execution.py`
5. Move `swarm/test_swarm_task.py` → `integration/swarm/test_resources.py`
6. Move `swarm/test_swarm_workflow_run.py` → `unit/swarm/test_orchestrator.py` (timeout tests)

### 3. Session-Scoped Shared Fixtures

**Current approach (inefficient):**
```python
@pytest.fixture
def tool(web_server_process, client_env):
    """Created fresh for EVERY test - expensive!"""
    tool = Tool(name="test_tool")
    tool.set_default_compute_resources(...)
    return tool
```

**Proposed approach:**
```python
# tests/pytest/fixtures/workflows.py

@pytest.fixture(scope="session")
def base_tool(web_server_process, client_env):
    """Session-scoped tool - created once."""
    tool = Tool(name="test_tool_base")
    tool.set_default_compute_resources(...)
    return tool

@pytest.fixture(scope="session")
def base_task_template(base_tool):
    """Session-scoped task template - created once."""
    return base_tool.get_task_template(
        template_name="base_template",
        command_template="echo {arg}",
        node_args=["arg"],
    )

# For tests that need isolated tools (rare):
@pytest.fixture
def isolated_tool(web_server_process, client_env):
    """Function-scoped for tests that modify tool state."""
    return Tool(name=f"isolated_tool_{uuid4()}")
```

**Estimated speedup:** 
- 50 tests × 250ms saved = **12.5 seconds**
- Plus reduced database churn

### 4. Clear Test Categories

| Directory | Requires Server? | Requires Database? | Test Type |
|-----------|------------------|-------------------|-----------|
| `unit/` | ❌ No | ❌ No | Pure logic |
| `logging/` | ❌ No | ❌ No | Configuration |
| `integration/` | ✅ Yes | ✅ Yes | Component interaction |
| `e2e/` | ✅ Yes | ✅ Yes | Full workflows |

**Running subsets:**
```bash
# Fast feedback during development
pytest tests/pytest/unit/ -x

# Logging changes only
pytest tests/pytest/logging/

# Full integration suite
pytest tests/pytest/integration/

# Release validation
pytest tests/pytest/e2e/
```

---

## Migration Plan

### Phase 1: Quick Wins (1-2 hours)

| Task | Effort | Impact |
|------|--------|--------|
| Delete duplicate `test_gateway.py` | 5 min | -40 test runs |
| Session-scope `tool` fixture | 15 min | ~10s faster |
| Session-scope `task_template` fixture | 15 min | ~5s faster |
| Move `test_builder.py` to `workflow_run_impl/` | 10 min | Cleaner structure |
| Delete empty `workflow_run/` directory | 5 min | Less confusion |

### Phase 2: Logging Consolidation (4-6 hours)

| Task | Effort | Impact |
|------|--------|--------|
| Create `logging/conftest.py` | 30 min | Shared fixtures |
| Create `logging/test_components.py` with parameterization | 2 hrs | Reduces 80 → 20 tests |
| Move and refactor existing logging tests | 2 hrs | Single source of truth |
| Update CI to run `logging/` as a group | 30 min | Better isolation |

### Phase 3: Directory Restructure (1-2 days)

| Task | Effort | Impact |
|------|--------|--------|
| Create `unit/`, `integration/`, `e2e/` structure | 1 hr | Clear boundaries |
| Move pure unit tests to `unit/` | 4 hrs | Faster isolation runs |
| Consolidate swarm tests | 2 hrs | Single ownership |
| Update imports and fixtures | 2 hrs | Working tests |
| Update CI pipeline | 1 hr | Parallel test stages |

### Phase 4: Fixture Optimization (4-6 hours)

| Task | Effort | Impact |
|------|--------|--------|
| Create `fixtures/` module structure | 1 hr | Reusable fixtures |
| Extract database fixtures | 1 hr | Better organization |
| Extract workflow fixtures | 1 hr | Session-scoped |
| Add fixture documentation | 1 hr | Maintenance |
| Profile and tune | 2 hrs | Performance validation |

---

## File-by-File Migration Map

### Files to DELETE

| File | Reason |
|------|--------|
| `client/swarm/workflow_run/test_gateway.py` | 100% duplicate |
| `client/swarm/workflow_run/__init__.py` | Directory being removed |

### Files to MOVE (no changes needed)

| From | To |
|------|-----|
| `client/swarm/workflow_run/test_builder.py` | `integration/swarm/test_builder.py` |
| `swarm/test_swarm.py` | `integration/swarm/test_execution.py` |
| `swarm/test_swarm_task.py` | `integration/swarm/test_resources.py` |
| `swarm/swarm_test_utils.py` | `fixtures/swarm_utils.py` |

### Files to MERGE

| Source Files | Target | Notes |
|--------------|--------|-------|
| `core/test_configuration.py` + `core/test_configuration_overrides.py` | `unit/core/test_configuration.py` | Same concern |
| `core/test_jobmon_context.py` + `core/test_structlog_detection.py` | `logging/test_structlog.py` | Both structlog |
| `integration/test_*_logging_integration.py` (4 files) | `logging/test_components.py` | Parameterized |
| `server/test_server_logging.py` + `server/test_server_otlp.py` | `logging/test_otlp.py` | OTLP focused |

### Files to RENAME for clarity

| From | To | Reason |
|------|-----|--------|
| `client/test_task_resources.py` | `unit/client/test_resources.py` | Shorter |
| `client/test_args_type.py` | `unit/client/test_serialization.py` | Clearer purpose |
| `worker_node/test_task_instance_worker_node.py` | `integration/worker/test_execution.py` | Cleaner name |

---

## CI Pipeline Integration

### Current (Sequential)

```yaml
test:
  script: pytest tests/pytest/
  # Runs everything, ~15-20 minutes
```

### Proposed (Parallel Stages)

```yaml
stages:
  - lint
  - unit
  - integration
  - e2e

unit:
  stage: unit
  parallel:
    matrix:
      - TEST_DIR: [unit/core, unit/client, unit/swarm, unit/server, unit/worker, logging]
  script: pytest tests/pytest/$TEST_DIR -x --tb=short
  # Each job: ~1-2 minutes

integration:
  stage: integration
  needs: [unit]
  parallel:
    matrix:
      - TEST_DIR: [integration/client, integration/server, integration/distributor, integration/swarm, integration/cli]
  script: pytest tests/pytest/$TEST_DIR --tb=short
  # Each job: ~2-3 minutes

e2e:
  stage: e2e
  needs: [integration]
  script: pytest tests/pytest/e2e/ -v
  # ~5 minutes
```

**Benefits:**
- Unit tests run in parallel (~2 min total vs 5+ min sequential)
- Fast failure feedback (unit tests first)
- Integration tests only run if unit tests pass
- E2E tests only run if integration tests pass
- Clear ownership for debugging failures

---

## Naming Conventions

### Test Files

```
test_{feature}.py           # General tests for a feature
test_{feature}_{aspect}.py  # Specific aspect of a feature

Examples:
test_workflow.py            # Workflow creation tests
test_workflow_resume.py     # Workflow resume-specific tests
test_resources.py           # Resource handling tests
test_resources_scaling.py   # Resource scaling tests
```

### Test Functions

```python
def test_{what}_{when}_{expected}():
    """Describe the behavior being tested."""

# Good examples:
def test_workflow_resume_succeeds_after_task_failure():
def test_task_instantiation_fails_on_invalid_resources():
def test_heartbeat_updates_report_by_date():

# Avoid:
def test_1():
def test_workflow():  # Too vague
def test_workflow_resume_test():  # Redundant 'test'
```

### Test Classes

```python
class TestFeatureBehavior:
    """Group related tests together."""

# Good examples:
class TestDAGCycleDetection:
class TestWorkflowResumeScenarios:
class TestOTLPHandlerInitialization:

# Avoid:
class Tests:  # Too vague
class TestWorkflow:  # Too broad
```

### Fixtures

```python
@pytest.fixture
def {scope}_{what}():
    """Describe what this provides."""

# Good examples:
def base_tool():              # Session-scoped, shared tool
def isolated_workflow():      # Function-scoped, isolated
def failing_task_template():  # Task template that fails

# Avoid:
def fixture():  # Meaningless name
def t():        # Too short
```

---

## Expected Outcomes

### Quantitative

| Metric | Current | Target | Method |
|--------|---------|--------|--------|
| Total test count | ~739 | ~660 | Remove duplicates, consolidate |
| Test files | 68 | ~45 | Merge related tests |
| Directories | 12 | 8 | Cleaner hierarchy |
| Logging files | 19 | 4 | Parameterization |
| CI time (full suite) | ~20 min | ~12 min | Parallel + fixtures |
| Unit test time | ~5 min | ~2 min | No server startup |

### Qualitative

- **Discoverability:** Clear hierarchy makes finding tests easy
- **Ownership:** Each directory has clear purpose
- **Speed:** Fast feedback loop with unit tests
- **Maintenance:** Single place to update common patterns
- **Onboarding:** New developers understand structure quickly

---

## Appendix: Complete File Inventory

### Files Staying in Place (no changes)

```
conftest.py                           # Update fixtures only
core/test_cluster.py                  # → unit/core/
core/test_cluster_queue.py            # → integration/server/
core/test_jobmon_utils.py             # → unit/core/
core/test_requester.py                # → unit/core/
end_to_end/test_simple_workflows.py   # → e2e/
end_to_end/test_workflow_resume.py    # → e2e/
```

### Files Requiring Content Changes

```
logging/test_components.py            # NEW: Parameterized component tests
fixtures/workflows.py                 # NEW: Session-scoped fixtures
fixtures/mocks.py                     # NEW: Shared mock utilities
```

---

*Proposal created: December 2024*
*Based on TEST_INVENTORY.md analysis*


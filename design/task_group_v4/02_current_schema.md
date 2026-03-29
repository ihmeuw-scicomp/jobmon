# Current Schema (v3)

## Entity Relationship

```
Tool → ToolVersion → TaskTemplate → TaskTemplateVersion
                                         ↓
Workflow → DAG → Edge ← Node ← TaskTemplateVersion
  ↓                              ↓
  Array ←────────────────────── Task → TaskInstance
  (1 per template per workflow)
```

## Key Tables

### workflow
```
id                       INT PK
tool_version_id          INT FK → tool_version
dag_id                   INT FK → dag
workflow_args_hash       VARCHAR(150) UNIQUE
task_hash                VARCHAR(150)
name                     VARCHAR(255)
description              TEXT
status                   VARCHAR(1)
max_concurrently_running INT
created_date             DATETIME
```

### dag
```
id           INT PK
hash         VARCHAR(250) UNIQUE
created_date DATETIME
```
Purpose: deduplication. If two workflows have identical dependency
structure, they share a DAG. In practice this rarely triggers.

### node
```
id                          INT PK
task_template_version_id    INT FK
node_args_hash              VARCHAR(150)
UNIQUE (task_template_version_id, node_args_hash)
```
Purpose: unique task identity within a template. One node can be
referenced by tasks in multiple workflows.

### edge
```
dag_id               INT  (PK part 1)
node_id              INT  (PK part 2)
upstream_node_ids    JSON
downstream_node_ids  JSON
```
Purpose: dependency storage. The JSON columns are the bottleneck —
16KB average, 37KB max per row. A workflow with 5000 nodes produces
~80MB of JSON data in this table.

### array
```
id                          INT PK
task_template_version_id    INT FK
workflow_id                 INT FK
name                        VARCHAR(255)
max_concurrently_running    INT
created_date                DATETIME
UNIQUE (task_template_version_id, workflow_id)
```
Purpose: groups tasks from one template within a workflow. The
unique constraint means ONE array per template per workflow — this
prevents splitting a template across pipeline stages.

### task
```
id                 INT PK
workflow_id        INT FK
node_id            INT FK
array_id           INT FK
task_args_hash     VARCHAR(150)
name               VARCHAR(255)
command            TEXT
status             VARCHAR(1)
max_attempts       INT
num_attempts       INT
resource_scales    VARCHAR(1000)
fallback_queues    VARCHAR(1000)
```

### task_instance
```
id                 INT PK
task_id            INT FK
workflow_run_id    INT FK
array_id           INT FK
task_resources_id  INT FK
array_batch_num    INT
array_step_id      INT
status             VARCHAR(1)
distributor_id     VARCHAR(300)
```

### task_template
```
id               INT PK
tool_version_id  INT FK
name             VARCHAR(255)
UNIQUE (tool_version_id, name)
```

### task_template_version
```
id                  INT PK
task_template_id    INT FK
command_template    VARCHAR(5000)
arg_mapping_hash    VARCHAR(150)
```

## File Locations

### Server Models
```
jobmon_server/src/jobmon/server/web/models/
  workflow.py
  dag.py
  node.py
  edge.py
  array.py
  task.py
  task_instance.py
  task_template.py
  task_template_version.py
  task_resources.py
```

### Server Routes
```
jobmon_server/src/jobmon/server/web/routes/
  v3/
    __init__.py          # api_v3_router, mounts fsm + cli + reaper
    fsm/
      __init__.py        # imports all fsm modules
      workflow.py        # workflow CRUD, resume, task_template_dag
      array.py           # array CRUD, queue_task_batch, concurrency
      dag.py             # DAG binding
      node.py            # node binding
      task.py            # task binding, status transitions
      task_instance.py   # TI lifecycle, heartbeats
      ...
    cli/
      __init__.py        # imports all cli modules
      workflow.py        # GUI: overview, details, status, filters
      task_template.py   # GUI: tt status, usage, errors, timeline
      task.py            # GUI: task table
      ...
```

### Client
```
jobmon_client/src/jobmon/client/
  tool.py                # Tool class, template factory
  task_template.py       # TaskTemplate, create_task/create_tasks
  task_template_version.py
  array.py               # Array class, task grouping
  workflow.py             # Workflow class, add_task, bind, run
  task.py                 # Task class, add_upstream
  node.py                 # Node class, node_args identity
  dag.py                  # Dag class, cycle detection, edge building
  task_resources.py
  workflow_run.py
  swarm/
    state.py             # SwarmState, task scheduling state
    array.py             # SwarmArray, runtime array tracking
    services/
      scheduler.py       # batch generation, concurrency enforcement
      synchronizer.py    # status sync, concurrency limit sync
```

### Migrations
```
jobmon_server/src/jobmon/server/web/migrations/
  env.py                 # migration environment, swap_foreign_keys_for_indices
  versions/              # alembic migration files
    2025_01_28_..._add_task_status_audit.py  # recent example
```

### App Factory
```
jobmon_server/src/jobmon/server/web/api.py
  get_app(versions=["v3"])  # creates FastAPI app, mounts versioned routers
```

### Tests
```
tests/
  integration/
    cli/                 # route-level integration tests
    client/              # client-level tests
  client/
    test_task.py
    test_task_group.py   # PR #9's tests (not merged)
  _scripts/              # test helper scripts
```

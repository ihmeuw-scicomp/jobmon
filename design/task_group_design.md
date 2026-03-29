# TaskGroup Design Document

## The Core Model

Jobmon manages graphs of computational tasks. From first principles, there
are only two structural concepts: **groups** (collections of tasks that share
something) and **dependencies** (ordering constraints between groups or tasks).

The TaskGroup model makes this explicit with five tables.

## Five Tables

```
TaskGroup           -- the universal container and DAG node
TaskGroupEdge       -- structural dependencies between groups
Task                -- a unit of computation within a group
TaskDependency      -- execution-level dependencies between tasks
TaskInstance        -- one execution attempt of a task
```

### TaskGroup

```sql
CREATE TABLE task_group (
    id                       INT PRIMARY KEY AUTO_INCREMENT,
    name                     VARCHAR(255) NOT NULL,
    workflow_id              INT NOT NULL,
    parent_id                INT DEFAULT NULL,
    task_args_hash           VARCHAR(150) DEFAULT NULL,
    task_template_version_id INT DEFAULT NULL,
    max_concurrently_running INT DEFAULT NULL,
    ordering                 INT DEFAULT 0,
    created_date             DATETIME DEFAULT CURRENT_TIMESTAMP,

    INDEX ix_tg_workflow (workflow_id),
    INDEX ix_tg_parent (parent_id)
);
```

A TaskGroup is both a container for tasks and a node in the structural DAG.

- **Leaf groups** have `task_template_version_id` set and contain tasks
  created from that template. This is what an Array is today.
- **Organizational groups** have `task_template_version_id = NULL` and
  serve as pipeline stages that contain other groups.
- **The root group** has `parent_id = NULL` and represents the workflow.

**task_args_hash** stores the data context shared across all tasks in the
group. These are the "data args" that parameterize the computation but
don't affect the graph shape. All tasks in a group share the same task_args.

**Concurrency** is hierarchical. A group inherits its parent's limit if
its own `max_concurrently_running` is null.

Multiple groups can reference the same `task_template_version_id` within
one workflow. This is how the same template runs at different pipeline
stages without creating false DAG cycles.

### TaskGroupEdge

```sql
CREATE TABLE task_group_edge (
    id                INT PRIMARY KEY AUTO_INCREMENT,
    source_group_id   INT NOT NULL,
    target_group_id   INT NOT NULL,
    dependency_spec   JSON DEFAULT NULL,

    UNIQUE INDEX ux_tge_pair (source_group_id, target_group_id),
    INDEX ix_tge_target (target_group_id)
);
```

A TaskGroupEdge says "tasks in the target group depend on tasks in the
source group." The `dependency_spec` defines HOW tasks match:

- `{"location_id": "location_id"}` — match by label value
- `NULL` — every task in the target depends on every task in the source

This table is tiny (dozens of rows per workflow). It IS the structural
DAG that the GUI visualizes.

### Task

```sql
CREATE TABLE task (
    id              INT PRIMARY KEY AUTO_INCREMENT,
    task_group_id   INT NOT NULL,
    workflow_id     INT NOT NULL,
    node_args_hash  VARCHAR(150) NOT NULL,
    command         TEXT,
    name            VARCHAR(255),
    status          VARCHAR(1),
    max_attempts    INT DEFAULT 3,

    UNIQUE INDEX ux_task_identity (task_group_id, node_args_hash),
    INDEX ix_task_workflow (workflow_id),
    INDEX ix_task_status (status)
);
```

**node_args_hash** is the graph-forming identity — the args that make this
task unique within its group (e.g., location_id=1, sex=male). These
determine WHICH tasks exist and HOW they connect.

The distinction between node_args_hash (on Task) and task_args_hash
(on TaskGroup) is fundamental:

- **node_args** vary per task, determine graph shape
- **task_args** are constant across the group, determine data context

### TaskDependency

```sql
CREATE TABLE task_dependency (
    task_id          INT NOT NULL,
    upstream_task_id INT NOT NULL,

    PRIMARY KEY (task_id, upstream_task_id),
    INDEX ix_td_upstream (upstream_task_id)
);
```

The execution DAG. Populated during bind from three sources:

1. Group edges + label matching (~90% of deps)
2. Intra-group task deps (e.g., cascade tree)
3. Explicit task.add_upstream() overrides

### TaskInstance (unchanged)

One execution attempt of a task. Multiple instances per task on retry.

## What This Replaces

| Current                  | New                 | How                                    |
|--------------------------|---------------------|----------------------------------------|
| Workflow (as container)  | Root TaskGroup      | parent_id = NULL                       |
| Array                    | Leaf TaskGroup      | task_template_version_id set           |
| DAG                      | TaskGroup tree      | group hierarchy IS the DAG             |
| Edge (JSON blobs)        | TaskGroupEdge       | structural deps with dependency_spec   |
| Edge (execution)         | TaskDependency      | normalized int pairs, indexed          |
| Node                     | Task.node_args_hash | identity within group                  |

## v4 API Coexistence

Built alongside v3 using `/api/v4/` route prefix. Both run simultaneously.

The server app factory already supports multiple versions:
```python
versions = versions or ["v3"]  # add "v4" here
```

v3 stays frozen. v4 uses the new tables. A workflow created via v3
gets backfilled TaskGroup data so the v4 GUI endpoints work.

## Demo Use Cases

See `design/task_group_demo_use_cases.py` for two production-derived
examples showing the target client API:

1. **dismod_at** — linear pipeline with intra-group cascade tree
2. **fhs-pipeline-mortality** — complex multi-stage pipeline where
   finalize_provenance_stage runs at 3 different stages

## Implementation Phases

### Phase 1: Schema + v4 scaffold (~2 weeks)
- Alembic migration: new tables + task.task_group_id
- Backfill from existing arrays
- v4 router scaffold
- Models

### Phase 2: v4 server routes (~3 weeks)
- FSM: task_group CRUD, task binding, workflow creation
- CLI: group tree, group DAG, status aggregation
- TaskDependency materialization

### Phase 3: v4 client (~3 weeks)
- TaskGroup class with create_tasks, interleave_upstream
- Workflow.add_group()
- Bind sequence

### Phase 4: GUI on v4 (~2 weeks)
- DAG from task_group_edge
- Status by group

### Phase 5: v3 deprecation
- Freeze v3, migration guide
- Remove old tables (major version)

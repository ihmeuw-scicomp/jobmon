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

## Client API

### No Breaking Changes

The user-facing API is fully backward compatible. Existing code using
`Tool`, `TaskTemplate`, `create_task()`, `create_tasks()`, `add_task()`,
`add_upstream()`, and `Workflow.run()` works unchanged.

What's **added** (not breaking):
- `TaskGroup` class
- `workflow.add_group()`
- `group.interleave_upstream()`
- `group.get_task()` / `group.get_subset()`

What's **removed from client internals** (not user-facing):
- `Array` → replaced by auto-created TaskGroup (same behavior)
- `Dag` → no more client-side DAG building
- `Node` → folded into Task identity

Users never import Array, Dag, or Node directly. `task.array` becomes a
deprecated alias for `task.task_group`.

### Backward Compatible Path

`TaskTemplate.create_tasks()` and `TaskTemplate.create_task()` continue
to work exactly as before. Under the hood, they create leaf TaskGroups
automatically.

```python
# Existing code — unchanged in v4
tt = tool.get_task_template("compute", ...)
tasks = tt.create_tasks(location_id=[1, 2, 3])  # auto-creates leaf group
wf.add_tasks(tasks)

task = tt.create_task(location_id=4)  # group assigned in add_task()
wf.add_task(task)
```

`TaskTemplate.create_tasks()` internally creates a leaf TaskGroup named
after the template:

```python
def create_tasks(self, **node_kwargs):
    group = TaskGroup(name=self.template_name, template=self)
    tasks = group.create_tasks(**node_kwargs)
    self._task_group = group
    return tasks
```

`TaskTemplate.create_task()` returns a bare task. `workflow.add_task()`
auto-creates or finds a leaf group by template name — same inference
logic as v3's Array assignment:

```python
def add_task(self, task):
    if not task.task_group:
        name = task.template.template_name
        if name not in self._auto_groups:
            self._auto_groups[name] = TaskGroup(
                name=name, template=task.template,
            )
        self._auto_groups[name].add_task(task)
    ...
```

### Explicit TaskGroup Path (New Capability)

Users opt into explicit groups when they need structure beyond one-group-
per-template:

```python
from jobmon.client.task_group import TaskGroup

# Leaf group with template — like Array but with a custom name
compute = TaskGroup("compute", template=tt_compute)
compute.create_tasks(location_id=[1, 2, 3])

# Same template at a different stage — impossible with Array
finalize_gk = TaskGroup("finalize_after_gk", template=tt_finalize)
finalize_gk.create_tasks(stage="gk")
finalize_squeeze = TaskGroup("finalize_after_squeeze", template=tt_finalize)
finalize_squeeze.create_tasks(stage="squeeze")

# Organizational group — no template, holds child groups
preprocessing = TaskGroup("preprocessing")
preprocessing.add_child(compute)
preprocessing.add_child(finalize_gk)

# Mixed group — tasks from multiple templates
setup = TaskGroup("setup")
setup.add_task(tt_config.create_task(version="v1"))
setup.add_task(tt_validate.create_task(version="v1"))

# Group-level dependencies
finalize_gk.interleave_upstream(compute, {"location_id": "location_id"})
finalize_squeeze.add_upstream_group(squeeze)

wf.add_groups([preprocessing, squeeze, finalize_squeeze])
```

### Incremental Migration

Users can adopt TaskGroup gradually without rewriting existing code:

```python
# Step 1: Existing code works as-is (auto-created leaf groups)
tasks_a = tt_compute.create_tasks(location_id=[1, 2, 3])
tasks_b = tt_finalize.create_tasks(location_id=[1, 2, 3])
for t in tasks_b:
    loc = t.node_args["location_id"]
    upstream = [a for a in tasks_a
                if a.node_args["location_id"] == loc][0]
    t.add_upstream(upstream)
wf.add_tasks(tasks_a + tasks_b)

# Step 2: Use interleave on the auto-created groups
tasks_a = tt_compute.create_tasks(location_id=[1, 2, 3])
tasks_b = tt_finalize.create_tasks(location_id=[1, 2, 3])
tt_finalize.task_group.interleave_upstream(
    tt_compute.task_group,
    {"location_id": "location_id"},
)
wf.add_tasks(tasks_a + tasks_b)

# Step 3: Explicit groups (full v4 API)
compute = TaskGroup("compute", template=tt_compute)
compute.create_tasks(location_id=[1, 2, 3])
finalize = TaskGroup("finalize", template=tt_finalize)
finalize.create_tasks(location_id=[1, 2, 3])
finalize.interleave_upstream(compute, {"location_id": "location_id"})
wf.add_groups([compute, finalize])
```

### Bind Sequence

**v3 bind (current):**
1. `dag.bind()` — serialize nodes, upload edge JSON blobs (slow)
2. `workflow.bind()` — create workflow with dag_id
3. `array.bind()` — create each array
4. `_bind_tasks()` — create tasks with array_id, node_id

**v4 bind:**
1. `workflow.bind()` — create workflow
2. `group.bind()` — create each group with parent_id
3. `edge.bind()` — create group edges with dependency_spec
4. `_bind_tasks()` — create tasks with task_group_id
5. Server materializes `TaskDependency` from group edges + label
   matching + explicit add_upstream() calls

The client never builds or uploads JSON edge blobs. The server writes
normalized (task_id, upstream_task_id) pairs directly.

## v4 API Coexistence

Built alongside v3 using `/api/v4/` route prefix. Both run simultaneously.

The server app factory already supports multiple versions:
```python
versions = versions or ["v3"]  # add "v4" here
```

v3 stays frozen. v4 uses the new tables. A workflow created via v3
gets backfilled TaskGroup data so the v4 GUI endpoints work.

The only configuration change for users: set `route_prefix = "/api/v4"`
in jobmonconfig.yaml.

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

# v4 Server Routes

## Route Registration

Following the v3 pattern in `api.py`:

```python
# api.py — add "v4" to versions
versions = versions or (["auth", "v3", "v4"] if auth_enabled else ["v3", "v4"])
```

```
routes/v4/
  __init__.py           # api_v4_router with /v4 prefix
  fsm/
    __init__.py         # fsm_router, imports modules
    task_group.py       # group CRUD + edges
    workflow.py         # workflow creation
    task.py             # task binding
    task_instance.py    # scheduling endpoints
  cli/
    __init__.py         # cli_router, imports modules
    task_group.py       # GUI: tree, DAG, status
    task.py             # GUI: task table by group
```

## FSM Routes (Execution)

### Task Group CRUD

**POST /task_group**
Create a task group. Idempotent (returns existing if name+workflow match).

Request:
```json
{
    "workflow_id": 123,
    "name": "compute_population",
    "parent_id": 456,
    "task_template_version_id": 789,
    "task_args_hash": "abc123",
    "max_concurrently_running": 200
}
```

Response:
```json
{
    "task_group_id": 1,
    "newly_created": true
}
```

**POST /task_group_edge**
Create a dependency between groups.

Request:
```json
{
    "source_group_id": 1,
    "target_group_id": 2,
    "dependency_spec": {"location_id": "location_id"}
}
```

**GET /task_group/{id}/effective_concurrency**
Walk parent chain, return effective limit.

Response:
```json
{
    "effective_max_concurrently_running": 200,
    "own_limit": null,
    "inherited_from_group_id": 456
}
```

**PUT /task_group/{id}/max_concurrently_running**
Update limit at runtime. Same pattern as current array concurrency update.

Request:
```json
{
    "max_concurrently_running": 300
}
```

### Workflow

**POST /workflow** (v4 version)
Same as v3 but without dag_id (no monolithic DAG).

Request:
```json
{
    "tool_version_id": 1,
    "workflow_args_hash": "hash",
    "task_hash": "hash",
    "name": "my_workflow",
    "max_concurrently_running": 1000
}
```

### Task Binding

**POST /task/bind** (v4 version)
Bind tasks with task_group_id instead of array_id + node_id.

Request (chunk of tasks):
```json
{
    "tasks": {
        "<hash>": {
            "task_group_id": 1,
            "node_args_hash": "abc",
            "command": "python run.py --loc 1",
            "name": "compute_loc_1",
            "max_attempts": 3,
            "labels": {"location_id": 1, "sex": "male"}
        }
    },
    "workflow_id": 123
}
```

**POST /task_group/{id}/materialize_dependencies**
Server-side materialization of task_dependency rows. Called after
all tasks are bound. Evaluates group edges + label matching.

Response:
```json
{
    "dependencies_created": 32451
}
```

### Task Instance (Scheduling)

**POST /task_group/{id}/queue_task_batch**
Same as current `/array/{id}/queue_task_batch` but keyed by group.

**POST /task_group/{id}/transition_to_launched**
Same pattern, keyed by group.

## CLI Routes (GUI)

### Group Tree

**GET /workflow/{id}/task_group_tree**
Full group tree with status counts per group.

Response:
```json
{
    "groups": [
        {
            "id": 1,
            "name": "preprocessing",
            "parent_id": null,
            "task_template_version_id": null,
            "max_concurrently_running": 200,
            "status_counts": {
                "PENDING": 0, "RUNNING": 50, "DONE": 432, "FATAL": 0
            },
            "total_tasks": 482
        },
        {
            "id": 2,
            "name": "compute_population",
            "parent_id": 1,
            "task_template_version_id": 789,
            "status_counts": {"DONE": 482},
            "total_tasks": 482
        }
    ]
}
```

### Group-Level DAG

**GET /workflow/{id}/task_group_dag**
Returns group edges for visualization. Replaces the 27-second
`task_template_dag` endpoint.

Response:
```json
{
    "groups": [
        {"id": 1, "name": "compute_pop", "parent_id": null},
        {"id": 2, "name": "finalize", "parent_id": null}
    ],
    "edges": [
        {
            "source_group_id": 1,
            "target_group_id": 2,
            "dependency_spec": {"location_id": "location_id"}
        }
    ]
}
```

### Task Table by Group

**GET /task_group/{id}/tasks**
Paginated task list for a group. Replaces the template-filtered
task table endpoint.

Query params: `page`, `page_size`, `status` (filter)

### Status Summary

**GET /task_group/{id}/status_summary**
Aggregate status for a group and all its descendants.

Response:
```json
{
    "task_group_id": 1,
    "name": "preprocessing",
    "own_tasks": {"PENDING": 0, "DONE": 482},
    "descendant_tasks": {"PENDING": 0, "DONE": 483},
    "total_tasks": 483
}
```

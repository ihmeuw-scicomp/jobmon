# Resume and Concurrency

## Resume Semantics

### Current v3 Resume

1. User calls `workflow.run()` with same `workflow_args`
2. Client computes `workflow_args_hash` → finds existing workflow
3. Client computes `task_hash` (hash of all task hashes)
4. If `task_hash` matches → all tasks exist, resume from last state
5. If `task_hash` differs → match individual tasks by
   `(node_id, task_args_hash)`, create new tasks for unmatched
6. Tasks in DONE state are skipped, others re-run

Task identity: `hash(task) = hash(node_args_hash + task_args_hash)`
Node identity: `(task_template_version_id, node_args_hash)`

### v4 Resume

1. Same: find workflow by `workflow_args_hash`
2. Match groups by `(name, task_args_hash)` within the workflow
3. Within each matched group, match tasks by `node_args_hash`
4. Matched DONE tasks → skip. Others → re-run. Unmatched → create.

Group identity: `(name, task_args_hash)` within a workflow.
Task identity: `node_args_hash` within a group.

### Edge Cases

**Group renamed**: New name = new group = all fresh tasks. Old group's
tasks remain as orphans (DONE from previous run, not re-run, not
cleaned up until workflow finalizes).

**Tasks added to existing group**: New tasks get created, existing
DONE tasks are skipped. The task_dependency table is re-materialized
to include the new tasks.

**task_args changed**: task_args_hash changes → new group identity →
all fresh tasks. Same graph shape, different data.

**Template version changed**: If the task_template_version_id on the
group changes, tasks get new commands but may keep the same
node_args_hash. Resume matches by node_args_hash and overwrites
the command. (Same as v3 behavior with node_id matching.)

## Concurrency

### Current v3 Concurrency

Two flat levels:
- `workflow.max_concurrently_running` — global limit
- `array.max_concurrently_running` — per-template limit

Scheduler enforces both independently:
```python
workflow_capacity = max_running - active_count
array_capacity = array.max_running - active_in_array
available = min(workflow_capacity, array_capacity)
```

### v4 Hierarchical Concurrency

Groups form a tree. Each group can have `max_concurrently_running`.
Null means inherit from parent. Effective limit = min of all ancestors.

```python
def effective_concurrency(group):
    limits = []
    current = group
    while current is not None:
        if current.max_concurrently_running is not None:
            limits.append(current.max_concurrently_running)
        current = current.parent
    return min(limits) if limits else float('inf')
```

Example:
```
Workflow (root group, max=1000)
  preprocessing (max=200)
    compute_pop (max=null → inherits 200)
    collect_pop (max=50)
  modeling (max=500)
    gk_model (max=null → inherits 500)
```

`compute_pop` effective limit: min(200, 1000) = 200
`collect_pop` effective limit: min(50, 200, 1000) = 50
`gk_model` effective limit: min(500, 1000) = 500

### Scheduler Changes

The v3 scheduler tracks per-array capacity in `SwarmArray`. In v4,
`SwarmArray` becomes `SwarmGroup` with a parent reference. The
`_generate_batches()` method checks capacity at each level:

```python
def get_group_capacity(group_id):
    active = count_active_in_group(group_id)
    limit = effective_concurrency(groups[group_id])
    own_capacity = limit - active

    # Also check parent capacity (active tasks in parent = sum of children)
    if group.parent_id:
        parent_capacity = get_group_capacity(group.parent_id)
        return min(own_capacity, parent_capacity)
    return own_capacity
```

### Runtime Concurrency Updates

Users can update concurrency at runtime (existing v3 feature).
In v4, `PUT /task_group/{id}/max_concurrently_running` updates
a group's limit. The synchronizer polls these limits periodically
(same as v3's array limit sync).

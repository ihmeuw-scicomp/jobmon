# PR #9 Context (TaskGroup Prototype)

## What It Was

PR #9 (ihmeuw-scicomp/jobmon#9, branch `GBDSCI-5460-task-groups`) was a
client-side-only TaskGroup implementation by agbaum. It was never merged
(still open against release/3.2).

## What To Take From It

### Label System

Tasks carry key-value labels (node_args + task_template_name). Groups
can be queried/subset by label values:

```python
group.get_subset(location_id=1)              # → Set[Task]
group.get_subgroup(location_id=1)            # → TaskGroup
group.get_task(location_id=1, sex="male")    # → Task (unique match)
```

This is the foundation for dependency matching and should be preserved.

### Interleave Pattern

The key innovation — label-based dependency wiring:

```python
group_b.interleave_upstream(
    group_a,
    dependency_specification={"downstream_label": "upstream_label"},
    subsetter={"sex": "male"},           # only these tasks in group_b
    upstream_subsetter={"year": 2020},   # only these tasks in group_a
)
```

This matches tasks by label value and calls `task.add_upstream()` for
each match. The dependency_specification maps downstream label names to
upstream label names.

### Subsetter

Both `interleave_upstream()` and `add_labels()` support `subsetter` —
a dict that filters which tasks in the group are affected. This enables
patterns like "only template2 tasks depend on template1_agg tasks":

```python
group.interleave_upstream(
    upstream_group,
    subsetter={"task_template_name": "template2"},
    upstream_subsetter={"task_template_name": "template1_agg"},
    dependency_specification={"location": "location"},
)
```

### Union/Composition

Groups can be combined:

```python
combined = group_a | group_b     # union
combined = group_a.union(group_b, "new_name")
```

## What To Change

### Client-Side → Server-Side

PR #9's TaskGroup was purely client-side — it manipulated Task objects
in memory. In v4, TaskGroup is a server-side database entity. The
`interleave_upstream` call creates a `task_group_edge` row with
`dependency_spec`, and the server materializes task_dependency rows.

### Label Storage

PR #9 used `task.labels` (a dict on the Task object). In v4, labels
need to be persisted server-side for materialization. Options:
- `task_label` table (task_id, key, value)
- JSON column on task
- Reuse existing `task_attribute` table

### Group Identity

PR #9 groups were ephemeral Python objects. In v4, groups have database
identity (id, name, workflow_id). This enables:
- Server-side status aggregation by group
- DAG visualization from group structure
- Resume matching by group name
- Runtime concurrency updates per group

### The dependency_specification Format

PR #9 used a `Dict[str, str]` mapping downstream labels to upstream
labels. v4 stores this as JSON in `task_group_edge.dependency_spec`.
Same format, persisted.

PR #9's suggestion to use `List[Tuple[str, str]]` instead of a dict
(to support mapping one downstream label to multiple upstream labels)
is worth considering but not required for v4 launch.

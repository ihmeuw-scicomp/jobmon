# TaskDependency Materialization

## Overview

When a workflow is bound via v4, the server materializes the
`task_dependency` table from three sources. This replaces the
80MB JSON edge blobs with indexed integer pairs.

## Source 1: Group Edges + Label Matching (~90% of deps)

For each `task_group_edge` row:

```python
def materialize_group_edge(source_group_id, target_group_id, dependency_spec, db):
    source_tasks = db.query(Task).filter(Task.task_group_id == source_group_id).all()
    target_tasks = db.query(Task).filter(Task.task_group_id == target_group_id).all()

    if dependency_spec is None:
        # All-to-all (barrier pattern)
        for target in target_tasks:
            for source in source_tasks:
                insert_dependency(target.id, source.id)
        return

    # Label matching
    # Build index on source tasks by their matching label values
    source_index = defaultdict(list)
    for task in source_tasks:
        key = tuple(task.labels.get(v) for v in dependency_spec.values())
        source_index[key].append(task.id)

    for target in target_tasks:
        key = tuple(target.labels.get(k) for k in dependency_spec.keys())
        for source_id in source_index.get(key, []):
            insert_dependency(target.id, source_id)
```

Example: `dependency_spec = {"location_id": "location_id"}`
- Target task has `location_id=5`
- Find all source tasks with `location_id=5`
- Insert one `task_dependency` row per match

### Where Labels Come From

Task labels are the node_args — the same values used to create tasks
via `create_tasks(location_id=[1,2,3])`. They're stored as task
attributes (key-value pairs) already used for `get_tasks_by_node_args`.

In the v4 schema, labels can be stored:
- **Option A**: In a `task_label` table (task_id, key, value) — normalized
- **Option B**: As JSON on the task row — simpler, used for matching
- **Option C**: Via the existing `task_attribute` infrastructure

Option A is cleanest for indexed queries during materialization.

### Bulk Materialization SQL

For simple single-key matching, this can be done in pure SQL:

```sql
-- Materialize dependency: target group depends on source group
-- matching by location_id
INSERT INTO task_dependency (task_id, upstream_task_id)
SELECT t.id, s.id
FROM task t
JOIN task_label tl ON t.id = tl.task_id AND tl.key = 'location_id'
JOIN task_label sl ON sl.key = 'location_id' AND sl.value = tl.value
JOIN task s ON sl.task_id = s.id
WHERE t.task_group_id = :target_group_id
  AND s.task_group_id = :source_group_id;
```

For multi-key matching (e.g., location_id AND sex):

```sql
INSERT INTO task_dependency (task_id, upstream_task_id)
SELECT t.id, s.id
FROM task t
JOIN task_label tl1 ON t.id = tl1.task_id AND tl1.key = 'location_id'
JOIN task_label tl2 ON t.id = tl2.task_id AND tl2.key = 'sex'
JOIN task_label sl1 ON sl1.key = 'location_id' AND sl1.value = tl1.value
JOIN task_label sl2 ON sl1.task_id = sl2.task_id AND sl2.key = 'sex' AND sl2.value = tl2.value
JOIN task s ON sl1.task_id = s.id
WHERE t.task_group_id = :target_group_id
  AND s.task_group_id = :source_group_id;
```

For all-to-all (null spec):

```sql
INSERT INTO task_dependency (task_id, upstream_task_id)
SELECT t.id, s.id
FROM task t, task s
WHERE t.task_group_id = :target_group_id
  AND s.task_group_id = :source_group_id;
```

## Source 2: Explicit Task-Level Dependencies

When the user calls `task.add_upstream(other_task)`, the client
records this and sends it during bind. The server inserts directly:

```sql
INSERT INTO task_dependency (task_id, upstream_task_id)
VALUES (:task_id, :upstream_task_id);
```

This handles:
- Fine-grained cross-group deps that don't follow a label pattern
- Intra-group deps (cascade tree, sequential chains)

## Source 3: Intra-Group Dependencies from Client

When tasks within the same group have dependencies (e.g., the
`continue_cascade` location tree), these are sent by the client
as explicit task-level dependencies. They go into `task_dependency`
the same way as source 2.

There's no separate "intra-group DAG" — it's all task_dependency rows.

## Materialization Timing

Options:
- **Eager (on bind)**: Materialize all task_dependency rows during
  workflow bind. Simpler, all deps ready before execution starts.
- **Lazy (on demand)**: Materialize per-group when tasks are about
  to be scheduled. Better for very large workflows.

Start with eager. Optimize to lazy if bind time becomes a problem.

## Row Counts

For workflow 557592 (5140 tasks):
- ~1M individual task-level edges exist in the current JSON blobs
- task_dependency would have ~1M rows × 8 bytes = ~8MB
- vs current ~80MB of JSON
- Fully indexed on both (task_id) and (upstream_task_id)

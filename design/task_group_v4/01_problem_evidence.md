# Problem Evidence

## Why TaskGroup Exists

The current data model has two fundamental problems that TaskGroup solves:
expensive DAG queries caused by JSON blob storage, and false cycles caused
by templates used at multiple pipeline stages.

## Problem 1: The Edge Table JSON Blobs

The `edge` table stores task dependencies as JSON arrays in
`upstream_node_ids` and `downstream_node_ids` columns.

Measurements from workflow 557683 (fhs-pipeline-mortality, 5,060 nodes):

```
SELECT node_id FROM edge WHERE dag_id = 587323
  → 0.15s, 5060 rows

SELECT COUNT(*) FROM edge WHERE dag_id = 587323
  → 0.04s (index-only scan)

SELECT node_id, downstream_node_ids FROM edge WHERE dag_id = 587323
  → 27.25s, 5060 rows

AVG(LENGTH(downstream_node_ids)) = 16,165 bytes
MAX(LENGTH(downstream_node_ids)) = 37,103 bytes
Total data transferred: ~80MB
```

The primary key index on (dag_id, node_id) finds the rows instantly.
The 27 seconds is pure I/O reading the JSON blob column from disk.

The 4-way join used by the `task_template_dag` endpoint
(Edge → Node → TaskTemplateVersion → TaskTemplate) takes the same
time because the bottleneck is reading `downstream_node_ids`, not
the joins. EXPLAIN shows all joins use eq_ref/PRIMARY.

## Problem 2: False DAG Cycles

Workflow 557592 (fhs-pipeline-mortality):
- 18 templates, 5140 nodes, 74 template-level edges
- 7 bidirectional template pairs (false cycles)
- 2 self-loops (sum_to_all_cause, squeeze_stage)

Root cause: `finalize_provenance_stage` has 9 nodes at pipeline
stages spanning ranks 2-32. When collapsed to template level:
- finalize ↔ run_merge_slices (600 edges each way, 1:1 ratio)
- finalize ↔ arima (290 each way)
- finalize ↔ sum_to_all_cause (290 each way)
- finalize ↔ squeeze (66 each way)
- plus 3 more bidirectional pairs

These are not real cycles. At the task level it IS a DAG. The cycles
appear only because different tasks within the same template sit at
different pipeline stages, and collapsing to template level loses
that information.

## Workflow Survey

Analysis of 6 production workflows across different tools:

| Workflow | Tool | Tasks | Templates | Pattern | Cycles |
|----------|------|-------|-----------|---------|--------|
| 558413 | ST-GPR | 137 | 16 | tree-like | none |
| 438514 | dismod_at_dev | 2,011 | 6 | cascade tree | 1 self-loop |
| 442449 | CODEm | 14 | 14 | linear chain | none |
| 558363 | dismod_at | 16,486 | 5 | cascade tree | 1 self-loop |
| 557360 | lsae_demographics | 454 | 2 | linear chain | none |
| 557592 | fhs-pipeline-mortality | 5,140 | 18 | complex fan-out | 7 bidir + 2 self |

5 of 6 map directly to the TaskGroup model (each template = one group).
The self-loops in dismod_at are intra-group task deps (location tree
cascade within `continue_cascade`). The fhs-pipeline-mortality cycles
require explicit pipeline stages — exactly what TaskGroup provides.

## Three Dependency Patterns Observed

1. **Cross-group label matching** (~90% of edges): "connect by location_id"
   → TaskGroupEdge with dependency_spec

2. **Intra-group task deps**: cascade tree within continue_cascade
   → TaskDependency (same as task.add_upstream)

3. **Fan-out / barrier**: one task depends on all tasks in a group
   → TaskGroupEdge with null dependency_spec (all-to-all)

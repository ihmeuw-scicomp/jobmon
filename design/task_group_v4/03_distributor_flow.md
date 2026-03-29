# Distributor and Scheduler Flow

## How Tasks Get Executed (Current v3)

### 1. Workflow Bind

Client calls in sequence:
1. `dag.bind()` — uploads nodes in chunks of 500, then edges with JSON blobs
2. `POST /workflow` — creates workflow with dag_id, workflow_args_hash
3. `array.bind()` — `POST /array` for each (template, workflow) pair
4. `_bind_tasks()` — `POST /task/bind_tasks_no_args` in chunks of 500,
   each task references node_id and array_id

### 2. Task Selection (Scheduler)

The scheduler (`swarm/services/scheduler.py`) maintains:
- `SwarmState.ready_to_run: deque[SwarmTask]` — tasks with all deps done
- `SwarmState._task_status_map` — tracks task statuses
- Per-array capacity tracking

`_generate_batches()` dequeues tasks from `ready_to_run` and groups them:
- Respects workflow `max_concurrently_running` (global limit)
- Respects array `max_concurrently_running` (per-template limit)
- Groups consecutive tasks by (array_id, task_resources) into batches
- Max batch size: 500

### 3. Dependency Resolution

When a task completes, the server checks what tasks are now unblocked:

The distributor polls `GET /task/{task_id}/most_recent_status` and the
synchronizer fetches status updates. When a task transitions to DONE,
the scheduler checks its downstream tasks' upstream dependencies.

**Currently**: downstream tasks are found via Node edges. The swarm
client-side state tracks `upstream_tasks` sets per task, loaded during
workflow binding.

**How deps are loaded**: During `_bind_tasks()`, each task's
`upstream_tasks` are serialized. The server stores them via the edge
table (node-level). The swarm loads them back during initialization.

### 4. Batch Submission

When the scheduler produces a batch:
1. `POST /array/{array_id}/queue_task_batch` with task IDs
   - Server creates TaskInstance records with array_batch_num, array_step_id
   - Transitions tasks to QUEUED
2. `POST /task_instance/instantiate_task_instances` — creates TIs
3. Distributor calls `launch_task_instance_batch()` on the cluster
   - For array-capable clusters: `submit_array_to_batch_distributor()`
   - Returns mapping of array_step_id → distributor_id (HPC job ID)
4. `POST /array/{array_id}/transition_to_launched` — marks as LAUNCHED

### 5. How This Changes in v4

**Bind**: No DAG/edge upload. Instead:
1. `POST /task_group` for each group (with parent_id)
2. `POST /task_group_edge` for each group dependency (with spec)
3. `POST /task/bind` for tasks (with task_group_id, no node_id)
4. Server materializes `task_dependency` rows

**Dependency resolution**: Scheduler queries `task_dependency` table
directly — `SELECT upstream_task_id FROM task_dependency WHERE task_id = ?`
instead of traversing Node edges.

**Batch submission**: Uses task_group_id where it currently uses array_id.
The `queue_task_batch` and `transition_to_launched` routes reference
task_group_id. TaskInstance gets task_group_id instead of array_id.
The `array_batch_num` / `array_step_id` pattern stays — just keyed
by group instead of array.

**Concurrency**: The scheduler walks the group tree to find the
effective limit (min of all ancestors) instead of checking flat
workflow + array limits.

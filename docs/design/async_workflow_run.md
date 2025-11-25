## Async WorkflowRun Heartbeat & Scheduler Redesign

### Problem Statement
- `WorkflowRun.run` is fully synchronous. Long blocking calls (most notably `queue_task_batch`) prevent `_log_heartbeat`, causing the workflow run to miss its heartbeat deadline and get reaped (`RUNNING → ERROR`), even when thousands of tasks remain ready.
- Once the server flips the run to `ERROR`, the client loop keeps spinning (it never exits early on server-driven transitions), so we burn CPU while making no progress.

### Goals
1. Decouple heartbeat emission from scheduling work so the client can always refresh its lease before the reaper window elapses.
2. Allow scheduling/batching logic to overlap with heartbeat/network work without depending on threads or polling hacks.
3. Simplify the shutdown path: as soon as the server reports a terminal status (`ERROR`, `TERMINATED`, etc.), stop scheduling and exit cleanly.
4. Avoid transitional wrappers—migrate the loop to native asyncio instead of maintaining both sync and async codepaths.

### High-Level Design
We refactor `WorkflowRun` into an asyncio-driven orchestration service:

```
WorkflowRun.run()
 └── asyncio.run(self._run_async(...))
       ├─ heartbeat_task = create_task(self._heartbeat_loop())
       ├─ scheduler_task = create_task(self._scheduler_loop(...))
       └─ await gather(...) with coordinated cancellation on error/exit
```

Key loop responsibilities:
- `_heartbeat_loop` wakes at a configurable cadence, checks the elapsed time since the last successful heartbeat, and eagerly sends one via the async requester if needed.
- `_scheduler_loop` contains the existing DAG traversal, but uses `await` for every network call. It yields control regularly so heartbeats remain responsive.
- When either loop observes a terminal status (server response or local exception), both tasks stop and the method exits with the correct workflow-run status.

### Request/Response Layer
- `Requester` already implements `send_request_async`. `WorkflowRun` will cache a single `aiohttp.ClientSession` (`self._session`) and route all HTTP calls through `await self.requester.send_request_async(...)`.
- Helper utility: `_async_request(app_route, payload, request_type)` centralizes session management and telemetry context binding (`structlog` context is already supported in `Requester`).
- Shutdown: `_close_session()` runs in `finally` blocks to close the session after both loops stop.

### Scheduler Loop Changes
1. **Async command execution**  
   - `SwarmCommand` becomes awaitable: either wraps an async callable or runs blocking code via `asyncio.to_thread` (for the rare CPU-only steps).  
   - `get_swarm_commands` itself remains synchronous (pure data prep), but the consumer `process_commands_async` awaits each command execution.

2. **Timeout accounting**  
   - `process_commands_async(timeout)` mirrors the current logic: measure elapsed time after each `await`, and stop issuing new commands when the budget is exhausted.
   - `_get_time_till_next_heartbeat` becomes async-friendly (returns floats; no change needed except call sites).

3. **State synchronization**  
   - `_synchronize_state_async(full_sync=False)` performs `_set_status_for_triaging`, `_log_heartbeat_async`, `_task_status_updates_async`, and concurrency-limit refreshes—all via async HTTP calls.
   - `_decide_run_loop_continue` stays synchronous since it only inspects in-memory state.

4. **Terminal-state awareness**  
   - Immediately after `_log_heartbeat_async` (server response contains authoritative status) and after `_update_status_async`, check `self._status`. If it is terminal, raise a sentinel exception to unwind `_scheduler_loop`.

### Heartbeat Loop
```
async def _heartbeat_loop(self):
    while not self._stop_event.is_set():
        await asyncio.sleep(self._heartbeat_tick)
        if time.time() - self._last_heartbeat_time >= self._workflow_run_heartbeat_interval:
            await self._log_heartbeat_async()
```
- `_heartbeat_tick` defaults to half the workflow-run heartbeat interval to provide headroom.
- If `_log_heartbeat_async` raises (network failure, invalid transition), allow the exception to propagate so the scheduler loop can stop and bubble the error to the user.

### Status Handling & Shutdown
- `_log_heartbeat_async` and `_task_status_updates_async` both return the server-provided workflow-run status. If it differs from the locally cached `_status`, update and check for terminal states.
- `_scheduler_loop` listens for:
  - `WorkflowRunStatus.ERROR`, `TERMINATED`, `STOPPED`, `DONE` → break out after finishing any in-flight bookkeeping.
  - `WorkflowRunStatus.COLD_RESUME/HOT_RESUME` → run the existing termination logic, then exit.
- `_run_async` ensures that all pending commands are flushed/cancelled before closing the session to avoid `Unclosed client session` warnings.

### API & Surface Changes
- `WorkflowRun.run` keeps the same signature, but internally calls `asyncio.run`. This is acceptable because the distributor CLI already ensures no event loop is running in the process.
- Methods now named `*_async` replace their synchronous counterparts (e.g., `queue_task_batch_async`, `_update_status_async`). We remove the old sync versions to avoid dual maintenance.
- `SwarmCommand` signature changes (`Callable[..., Awaitable[None]]`).
- The rest of the client surface (`Workflow.run`, CLI commands) are unaffected except for slightly different log timing.

### Error Handling & Retries
- All async HTTP calls inherit requester-level tenacity retries. If retries exceed the budget, the exception surfaces to `_scheduler_loop`, which logs and sets the workflow-run status to `ERROR` (matching current behavior).
- Heartbeat task uses the same requester; if it fails repeatedly, the workflow run exits rather than spinning indefinitely.

### Testing Strategy
1. **Unit tests** with `pytest.mark.asyncio` to cover:
   - Heartbeat loop firing during an artificially delayed `queue_task_batch_async`.
   - Proper shutdown when server returns `status="E"` mid-loop.
2. **Integration tests**:
   - `tests/pytest/swarm/test_swarm.py` exercises `swarm.process_commands()` / `swarm.synchronize_state()` directly; these tests must move under `asyncio` so they can `await` the new APIs.
   - Distributor suites (`tests/pytest/distributor/test_{triaging,instantiate,killed,heartbeat,queued}.py`) and worker-node tests (`tests/pytest/worker_node/test_task_instance_worker_node.py`) likewise need async-aware fixtures because they call `swarm.process_commands()` in setup.
   - End-to-end resumes/logging (`tests/pytest/end_to_end/test_workflow_resume.py`, `tests/pytest/end_to_end/test_logging.py`) patch heartbeat/status helpers; update those monkeypatches to async coroutines and run the tests with `pytest.mark.asyncio`.
3. **Regression tests** for initialization/resume paths to ensure asynchronous refactor doesn’t break DAG construction or CLI flows that still invoke `WorkflowRun.run()` synchronously.

### Rollout Notes
- Because there is no need for backward compatibility, all call sites move to the async versions in one commit.
- Document the new requirement (Python ≥ 3.8 already available) in release notes, highlighting the improved heartbeat resilience.
- Monitor staging logs after deployment: we expect heartbeat logs to be evenly spaced even during heavy queueing, and we should no longer see reaper-triggered `RUNNING → ERROR` transitions for active runs.


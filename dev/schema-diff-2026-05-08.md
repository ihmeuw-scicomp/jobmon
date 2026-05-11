# Schema diff: `jobmon_dev` (infra) vs `docker` (scicomp prod)

Snapshot taken 2026-05-08 while preparing the `feat/move-to-infra` migration.

## Databases compared

| label | host | schema |
|---|---|---|
| dev  | `et-mysql-jobmon-d01.mysql.database.azure.com` | `jobmon_dev` |
| prod | `scicomp-mysql-mysql-p01.privatelink.mysql.database.azure.com` | `docker` |

Both reachable from a Mac on the IHME network using `mysql+pymysql://` with `connect_args={"ssl": {"ssl_mode": "REQUIRED"}}`.

`alembic_version` differs (`b2c3d4e5f6a7` dev, `c3d4e5f6a7b8` prod) but is intentionally ignored in this audit — see "Out of scope" below.

## Per-table verdict

### Match (24)

`arg`, `arg_type`, `cluster`, `cluster_type`, `dag`, `edge`, `node`, `node_arg`, `queue`, `task_arg`, `task_attribute`, `task_attribute_type`, `task_instance_status`, `task_resources_type`, `task_status`, `task_status_audit`, `task_template`, `template_arg_map`, `tool`, `tool_version`, `workflow_attribute`, `workflow_attribute_type`, `workflow_run_status`, `workflow_status`

### Only in prod (3)

`convert_log`, `convert_state`, `convert_state_dag`. Vestigial tables from a past batched data-conversion job. No application code in `jobmon_server/` references them. Safe to omit from dev.

### Differ (9)

All deltas collapse to two patterns.

#### Pattern A — dev tightened columns to `NOT NULL` that prod leaves nullable

| table | columns made `NOT NULL` in dev |
|---|---|
| `array` | `max_concurrently_running` |
| `task` | `command`, `max_attempts`, `num_attempts` |
| `task_instance` | `array_batch_num`, `array_id`, `array_step_id`, `task_resources_id`, `workflow_run_id` |
| `task_instance_error_log` | `description` |
| `task_resources` | `requested_resources` |
| `task_template_version` | `command_template` |
| `workflow` | `dag_id`, `max_concurrently_running` |
| `workflow_run` | `workflow_id` |

#### Pattern B — column present in prod, missing in dev

| table | column |
|---|---|
| `task` | `submitted_date datetime NULL` |

Types, defaults, `EXTRA`, and FK shape (by `(column, ref_table, ref_column)`) match everywhere else.

## Risks for the next migration

1. **Bulk-loading prod rows into dev will fail** wherever dev has `NOT NULL` and prod allows `NULL` (Pattern A). Any prod row carrying `NULL` in those columns is rejected on insert.
2. **`task.submitted_date` is prod-only.** Reading prod data into a dev-shaped schema means the value is dropped. Note: this column is **not declared in the ORM** ([`jobmon_server/src/jobmon/server/web/models/task.py`](jobmon_server/src/jobmon/server/web/models/task.py)) — it looks like a prod-side denormalization added outside alembic for query perf, not an application feature. The ORM's `submitted_date` lives on `task_instance` ([`task_instance.py:88`](jobmon_server/src/jobmon/server/web/models/task_instance.py#L88)), which matches in both DBs.
3. The `convert_*` tables on prod hold no application-relevant rows. Excluding them from any export to dev is safe.

## Migrating SciComp prod → Infra dev: detailed risk catalog

This is what to expect when dumping `docker` and loading into `jobmon_dev` (or replicating live).

### Hard blockers (insert will fail without intervention)

| prod row condition | dev rejects because |
|---|---|
| `array.max_concurrently_running IS NULL` | dev requires non-null |
| `task.command IS NULL` | dev `TEXT NOT NULL` |
| `task.max_attempts IS NULL` or `task.num_attempts IS NULL` | dev `INT NOT NULL` |
| `task_instance.array_id` / `array_batch_num` / `array_step_id` `IS NULL` | dev requires every TI to be part of an array (even singletons get a 1-element array) |
| `task_instance.task_resources_id IS NULL` | dev requires resources bound at creation |
| `task_instance.workflow_run_id IS NULL` | dev forbids orphan task_instances |
| `task_instance_error_log.description IS NULL` | dev requires non-empty error text |
| `task_resources.requested_resources IS NULL` | dev requires the resources JSON blob |
| `task_template_version.command_template IS NULL` | dev requires the template string |
| `workflow.dag_id IS NULL` | dev requires every workflow be bound to a DAG |
| `workflow.max_concurrently_running IS NULL` | dev requires non-null |
| `workflow_run.workflow_id IS NULL` | dev forbids orphan workflow_runs |

Mitigations, in order of preference:

1. **Pre-flight audit on prod**: for each column above, `SELECT COUNT(*) WHERE col IS NULL`. If the count is zero everywhere, this whole block is a non-issue. Run this before any cutover.
2. **Backfill on the prod copy** before loading: pick sensible defaults (e.g. `max_concurrently_running = 10000`, `num_attempts = 0`) and `UPDATE ... SET col = default WHERE col IS NULL`.
3. **Quarantine on the dev side**: load the rejected rows into a staging table for manual triage instead of dropping them.

### Soft regressions (load succeeds, but something is off afterwards)

1. **`task.submitted_date` drops.** The column exists only in prod's `task` table — it's not in the ORM and not used by current app code, but any ad-hoc analytics / GUI export that reads it will return nothing on dev. Confirm no Grafana / Airflow / Tableau pulls reference `task.submitted_date` before the cutover. ([`workflow_repository.py:673,686`](jobmon_server/src/jobmon/server/web/repositories/workflow_repository.py#L673) reads `wf_submitted_date` but from a `task_instance.submitted_date` source — that path is safe.)
2. **`convert_log` / `convert_state` / `convert_state_dag` left behind.** Application code doesn't read them, but if any operational runbook resumes a half-finished prior conversion based on `convert_state`, that resumability is lost on dev.
3. **AUTO_INCREMENT counters** need to be set above the imported MAX(id) on every imported table or new inserts will collide. MySQL doesn't preserve AUTO_INCREMENT across `mysqldump`/restore unless you ask explicitly.
4. **Missing prod indexes hurt hot-path query latency on dev** (out of scope here in detail but worth restating): without `task_instance.ix_ti_task_id_covering`, `ix_task_instance_report_by_date`, `ix_task_instance_status_date`, the distributor heartbeat scan and the GUI's task table both regress. Without `idx__workflow__name` / `idx__workflow__workflow_args`, the GUI workflow search regresses. Plan to backport these indexes in the same maintenance window as the data load.
5. **FK referential integrity.** This audit only verified FK *shape*, not `ON DELETE` / `ON UPDATE` actions. If prod has rows pointing at tombstoned parents (which a `CASCADE`-on-prod / `RESTRICT`-on-dev combo would have hidden), they'll fail on load. Worth running an `IS NULL` check on each `referenced_column` post-join during pre-flight.

## Potential code issues when running on the Infra schema

The Infra (dev) schema is stricter than the prod schema. That tightening is not consistently mirrored in the ORM or in the application code, so a number of paths that worked silently on prod can raise `IntegrityError` on Infra.

### Where the ORM disagrees with the DB

| location | ORM says | dev DB says | risk |
|---|---|---|---|
| [`task.py:52`](jobmon_server/src/jobmon/server/web/models/task.py#L52) | `array_id = Column(Integer, ForeignKey(...), default=None)` | (no diff vs prod — Pattern A is on `task_instance.array_id`, not `task.array_id`) | None — but easy to confuse with the TI column below. |
| [`task_instance.array_id`](jobmon_server/src/jobmon/server/web/models/task_instance.py) | declared `Column(Integer, ForeignKey(...))` (nullable by default) | `NOT NULL` | ORM happily yields `array_id=None`; insert raises `IntegrityError` only at flush. |
| `task_instance.task_resources_id` | same — ORM nullable, dev `NOT NULL` | `NOT NULL` | Same shape: ORM lets you build a half-constructed `TaskInstance` then crashes on commit. |
| `task_instance.workflow_run_id` | ORM nullable | `NOT NULL` | Same shape. |
| `task.command` / `task.max_attempts` / `task.num_attempts` | ORM nullable | `NOT NULL` | Any factory path that doesn't fully populate a `Task` (e.g. partial test fixtures, admin endpoints, retry logic that creates a stub `Task` first then fills in) will succeed on prod and fail on dev. |
| `task_resources.requested_resources` | ORM nullable | `NOT NULL` | Any code that creates a `TaskResources` placeholder before computing the resources blob will fail on dev. |

The general failure mode is: an ORM `add(...)` + flush raising `IntegrityError("(1048, \"Column 'X' cannot be null\")")` deep inside a route or transition handler. Because the ORM declarations match prod's looseness, type checks and unit tests using SQLite (also loose by default) won't catch this — it surfaces only against the Infra MySQL.

### Code paths most likely to be hit

1. **`TransitionService` retry path.** When a `TaskInstance` is reset for retry, code that constructs a new TI by copy may not set `array_batch_num` / `array_step_id` until the array re-broadcasts. On dev that's an integrity error; on prod it persists as `NULL` and is reconciled later.
2. **Test fixtures in `tests/conftest.py`.** Per [`.claude/rules/testing.md`](.claude/rules/testing.md), the existing fixtures have deep FK chains. Any fixture that built a `TaskInstance` without an array binding worked against an older prod-shaped test DB but will fail under Infra-shaped MySQL. Re-running the suite against Infra (vs the bundled SQLite) is the cheapest way to find these.
3. **GUI admin actions.** Bulk task status manipulation in the React UI ends up issuing PUTs that touch task / task_instance rows. Any code path that updates only the columns it cares about and leaves others as `NULL` is exposed.
4. **Heartbeat / claim loops in `distributor_service.py`.** When a TI is claimed for execution and resources are bound late, the brief window where `task_resources_id IS NULL` is now forbidden. Check the order of `INSERT`s in [`distributor_service.py`](jobmon_core/src/jobmon/distributor/distributor_service.py).
5. **`array.create_array_batch` and `task_instance.bind_task_instance` server routes.** These are the most concentrated entry points for the columns in Pattern A; they need to be reviewed end-to-end against the dev schema.

### Recommended pre-cutover validation

1. **Run the full test suite against Infra MySQL**, not SQLite. Any latent NULL-tolerance assumption surfaces as an `IntegrityError` in the test logs.
2. **Replay a representative workflow** (the `six_job_test.py` from `.claude/rules/testing.md`) end-to-end on Infra and watch the backend logs for `IntegrityError`.
3. **Grep server routes for `nullable=True` columns being updated in isolation**: any `update().values(col=None)` against a Pattern-A column is an immediate crash on Infra.
4. **Decide ORM strategy**: either tighten the ORM models to `nullable=False` to match the Infra DB (and accept that prod would then fail validation client-side), or relax the Infra DB columns back to nullable. The former is the cleaner long-term fix; the latter is the faster cutover.

## Recommended reconciliation (dev → match prod)

If the goal is "make dev structurally identical to prod for a clean cutover":

1. Relax dev's `NOT NULL` back to `NULL` on the columns in Pattern A — one alembic `op.alter_column(table, col, nullable=True, existing_type=...)` per column.
2. Add `task.submitted_date datetime NULL` to dev.
3. Leave the `convert_*` trio out of dev.

The reverse direction (tighten prod up to dev) is the larger lift: every column in Pattern A would need a backfill of existing `NULL`s before flipping to `NOT NULL`.

## Out of scope for this audit

- Alembic revision graph (ignored by request).
- Column collations / charsets / table engines / `AUTO_INCREMENT` counters.
- Foreign-key `ON DELETE` / `ON UPDATE` actions — only `(column, ref_table, ref_column)` shape was checked.
- Index parity — covered separately in the prior schema diff (notably prod has a covering index `task_instance.ix_ti_task_id_covering` that dev lacks).
- Stored procedures / triggers / views.

## Reproduction

The diff was generated with `information_schema` queries from a Python script using `sqlalchemy + pymysql`. To regenerate:

```python
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = :schema
ORDER BY TABLE_NAME, ORDINAL_POSITION;
```

against each schema, then diff the resulting `{table: {column: (type, nullable, default, extra)}}` dicts.

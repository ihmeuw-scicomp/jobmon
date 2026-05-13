"""Regression tests for ``TransitionService.create_audit_records_bulk``.

The bulk close-then-open pattern on ``task_status_audit`` previously took the
form ``UPDATE … WHERE task_id IN (…) AND exited_at IS NULL`` followed by an
``INSERT`` of new open rows. Under MySQL default ``REPEATABLE READ`` against
the ``(task_id, exited_at)`` secondary index added by migration
``b2c3d4e5f6a7``, two transactions with interleaved ``task_id`` lists would
deadlock: their UPDATE-stage next-key locks blocked each other's follow-on
INSERT insert-intention locks. Production logged ~240 of these per 24 h on
the ``INSERT INTO task_status_audit`` statement.

The fix splits the close into a non-locking SELECT for primary keys followed
by an UPDATE keyed by those PKs, with a residual ``IS NULL`` predicate to
prevent clobbering a concurrent closer. The tests below check:

  * behaviour parity — one open row closed, one new open inserted
  * TOCTOU safety — a concurrent closer between Phase 1 and Phase 2 cannot
    have its ``exited_at`` overwritten
  * deadlock-free under MySQL concurrency — two threads with interleaved
    task_id lists both commit without raising 1213. Gated on
    ``JOBMON_MYSQL_URI`` since SQLite cannot reproduce the lock geometry.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime
from typing import List
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskStatus
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_resources_type import TaskResourcesType
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.services.transition_service import TransitionService

# ---------------------------------------------------------------------------
# Local fixture — keeps this file independent of test_transition_service.py
# (whose fixture is module-local, not in conftest, so not shareable).
# ---------------------------------------------------------------------------


@pytest.fixture
def workflow_with_tasks(dbsession: Session):
    """Minimal workflow + 5 tasks for exercising the audit bulk path."""
    cluster_type = ClusterType(name="ct_audit_bulk")
    dbsession.add(cluster_type)
    dbsession.flush()

    cluster = Cluster(name="c_audit_bulk", cluster_type_id=cluster_type.id)
    dbsession.add(cluster)
    dbsession.flush()

    queue = Queue(name="q_audit_bulk", cluster_id=cluster.id, parameters="{}")
    dbsession.add(queue)
    dbsession.flush()

    task_resources_type = TaskResourcesType(id="O", label="ORIGINAL")
    dbsession.merge(task_resources_type)
    dbsession.flush()

    tool = Tool(name="tool_audit_bulk")
    dbsession.add(tool)
    dbsession.flush()

    tool_version = ToolVersion(tool_id=tool.id)
    dbsession.add(tool_version)
    dbsession.flush()

    task_template = TaskTemplate(tool_version_id=tool_version.id, name="tt_audit_bulk")
    dbsession.add(task_template)
    dbsession.flush()

    task_template_version = TaskTemplateVersion(
        task_template_id=task_template.id,
        command_template="echo {arg}",
        arg_mapping_hash="arg_hash_audit_bulk",
    )
    dbsession.add(task_template_version)
    dbsession.flush()

    node = Node(
        task_template_version_id=task_template_version.id,
        node_args_hash="node_hash_audit_bulk",
    )
    dbsession.add(node)
    dbsession.flush()

    dag = Dag(hash="dag_hash_audit_bulk")
    dbsession.add(dag)
    dbsession.flush()

    workflow = Workflow(
        tool_version_id=tool_version.id,
        dag_id=dag.id,
        name="wf_audit_bulk",
        workflow_args_hash="wf_hash_audit_bulk",
        task_hash="task_hash_audit_bulk",
        max_concurrently_running=10,
        status="G",
    )
    dbsession.add(workflow)
    dbsession.flush()

    array = Array(
        workflow_id=workflow.id,
        task_template_version_id=task_template_version.id,
        name="array_audit_bulk",
        max_concurrently_running=10,
    )
    dbsession.add(array)
    dbsession.flush()

    workflow_run = WorkflowRun(workflow_id=workflow.id, status="R", user="test_user")
    dbsession.add(workflow_run)
    dbsession.flush()

    task_resources = TaskResources(
        queue_id=queue.id,
        task_resources_type_id="O",
        requested_resources="{}",
    )
    dbsession.add(task_resources)
    dbsession.flush()

    tasks = []
    for i in range(5):
        task = Task(
            workflow_id=workflow.id,
            node_id=node.id,
            array_id=array.id,
            task_args_hash=f"hash_audit_bulk_{i}",
            name=f"task_audit_bulk_{i}",
            command=f"echo task {i}",
            status=TaskStatus.REGISTERING,
            max_attempts=3,
            task_resources_id=task_resources.id,
        )
        dbsession.add(task)
        tasks.append(task)
    dbsession.flush()

    return {"workflow": workflow, "tasks": tasks}


# ---------------------------------------------------------------------------
# Behaviour parity
# ---------------------------------------------------------------------------


class TestCreateAuditRecordsBulkParity:
    """The rearranged implementation must preserve the close+insert contract."""

    def test_closes_existing_open_and_inserts_new_open(
        self, dbsession: Session, workflow_with_tasks
    ):
        """One open audit row → close it; new record → insert it open."""
        tasks = workflow_with_tasks["tasks"]
        workflow = workflow_with_tasks["workflow"]
        task = tasks[0]

        # Seed an existing open row (entered_at < now, exited_at NULL).
        existing = TaskStatusAudit(
            task_id=task.id,
            workflow_id=workflow.id,
            previous_status=None,
            new_status=TaskStatus.REGISTERING,
            exited_at=None,
        )
        dbsession.add(existing)
        dbsession.flush()
        existing_id = existing.id

        # Trigger the bulk path with a single record for the same task.
        TransitionService.create_audit_records_bulk(
            session=dbsession,
            records=[
                {
                    "task_id": task.id,
                    "workflow_id": workflow.id,
                    "previous_status": TaskStatus.REGISTERING,
                    "new_status": TaskStatus.QUEUED,
                }
            ],
        )
        dbsession.flush()
        # The Phase 2 UPDATE runs with synchronize_session=False (Core
        # bulk update); the ORM identity map still has the original
        # ``existing`` object with exited_at=None. Expire so the next
        # read pulls fresh column values from the DB.
        dbsession.expire_all()

        rows = (
            dbsession.execute(
                select(TaskStatusAudit)
                .where(TaskStatusAudit.task_id == task.id)
                .order_by(TaskStatusAudit.id)
            )
            .scalars()
            .all()
        )
        assert len(rows) == 2, "expected the original row plus one new open row"

        closed, opened = rows
        assert closed.id == existing_id
        assert closed.exited_at is not None, "Phase 2 must close the existing row"
        assert opened.exited_at is None, "Phase 3 inserts the new row open"
        assert opened.new_status == TaskStatus.QUEUED
        assert opened.previous_status == TaskStatus.REGISTERING

    def test_no_open_row_just_inserts(self, dbsession: Session, workflow_with_tasks):
        """When no prior open row exists, Phase 2 is skipped, Phase 3 still runs."""
        tasks = workflow_with_tasks["tasks"]
        workflow = workflow_with_tasks["workflow"]
        task = tasks[0]

        TransitionService.create_audit_records_bulk(
            session=dbsession,
            records=[
                {
                    "task_id": task.id,
                    "workflow_id": workflow.id,
                    "previous_status": None,
                    "new_status": TaskStatus.REGISTERING,
                }
            ],
        )
        dbsession.flush()

        rows = (
            dbsession.execute(
                select(TaskStatusAudit).where(TaskStatusAudit.task_id == task.id)
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].exited_at is None
        assert rows[0].new_status == TaskStatus.REGISTERING

    def test_empty_records_is_noop(self, dbsession: Session):
        """Empty input must early-return without issuing SQL."""
        # Just verify no exception. If a degenerate UPDATE or INSERT slipped
        # through with an empty IN-list, SQLAlchemy would raise.
        TransitionService.create_audit_records_bulk(session=dbsession, records=[])


# ---------------------------------------------------------------------------
# TOCTOU
# ---------------------------------------------------------------------------


class TestCreateAuditRecordsBulkTOCTOU:
    """A concurrent closer between Phase 1 and Phase 2 must not be clobbered."""

    def test_residual_is_null_predicate_prevents_clobber(
        self, dbsession: Session, workflow_with_tasks
    ):
        """Simulates the race directly: Phase 1's SELECT returns the PK of a
        row that is already closed (as it would under MVCC if another writer
        committed between snapshot time and our UPDATE). Phase 2's residual
        ``IS NULL`` predicate must make the UPDATE a no-op so the existing
        ``exited_at`` is not clobbered with ``NOW()``.
        """
        tasks = workflow_with_tasks["tasks"]
        workflow = workflow_with_tasks["workflow"]
        task = tasks[0]

        # Seed a row that is ALREADY closed (the "other writer" already won).
        pre_existing_exit = datetime(2026, 1, 1, 0, 0, 0)
        closed = TaskStatusAudit(
            task_id=task.id,
            workflow_id=workflow.id,
            previous_status=None,
            new_status=TaskStatus.REGISTERING,
            exited_at=pre_existing_exit,
        )
        dbsession.add(closed)
        dbsession.flush()
        closed_id = closed.id

        # Force Phase 1's SELECT to return [closed_id] as if the snapshot
        # had still shown the row as open. The Phase 2 UPDATE then targets
        # that PK with an ``exited_at IS NULL`` residual; the actual row
        # state is "closed", so the predicate should filter out the row.
        real_execute = dbsession.execute

        def execute_with_stale_select(stmt, *args, **kwargs):
            sql = str(stmt).lower()
            is_phase_1_select = (
                "select" in sql
                and "task_status_audit.id" in sql
                and "exited_at is null" in sql
            )
            if is_phase_1_select:

                class _FakeScalars:
                    def all(self_inner):
                        return [closed_id]

                class _FakeResult:
                    def scalars(self_inner):
                        return _FakeScalars()

                return _FakeResult()
            return real_execute(stmt, *args, **kwargs)

        with patch.object(dbsession, "execute", side_effect=execute_with_stale_select):
            TransitionService.create_audit_records_bulk(
                session=dbsession,
                records=[
                    {
                        "task_id": task.id,
                        "workflow_id": workflow.id,
                        "previous_status": TaskStatus.REGISTERING,
                        "new_status": TaskStatus.QUEUED,
                    }
                ],
            )
        dbsession.flush()

        # The closed row's exited_at must STILL be the pre-existing
        # timestamp. If the residual IS NULL filter were absent, Phase 2
        # would have set it to NOW().
        dbsession.expire_all()
        refreshed = dbsession.execute(
            select(TaskStatusAudit).where(TaskStatusAudit.id == closed_id)
        ).scalar_one()
        assert refreshed.exited_at == pre_existing_exit, (
            "Phase 2's residual IS NULL predicate failed to prevent "
            "clobbering an already-closed row"
        )

        # Phase 3 should still have inserted a new open row.
        new_open = dbsession.execute(
            select(TaskStatusAudit).where(
                TaskStatusAudit.task_id == task.id,
                TaskStatusAudit.id != closed_id,
            )
        ).scalar_one()
        assert new_open.exited_at is None
        assert new_open.new_status == TaskStatus.QUEUED


# ---------------------------------------------------------------------------
# MySQL deadlock-free integration
# ---------------------------------------------------------------------------


_MYSQL_URI = os.environ.get("JOBMON_MYSQL_TEST_URI")


@pytest.mark.skipif(
    not _MYSQL_URI,
    reason=(
        "Requires JOBMON_MYSQL_TEST_URI pointing at a MySQL 8.x instance. "
        "SQLite cannot reproduce the (task_id, exited_at) gap-lock geometry "
        "the fix targets, so this test is mysql-only."
    ),
)
class TestCreateAuditRecordsBulkMySQLNoDeadlock:
    """Two concurrent bulk close-then-open calls with interleaved task_ids
    must commit cleanly across many iterations. Pre-fix code reliably
    raises ``1213 Deadlock found`` within tens of iterations because the
    next-key gap locks the close-UPDATE takes on
    ``ix_task_status_audit_task_exited`` block the follow-on INSERT's
    insert-intention locks (and vice versa for the peer transaction).
    """

    # Each thread runs N close-then-open iterations. The geometry doesn't
    # fire on every iteration — production sees ~240/24h across the full
    # transition fleet — so we have to make the test run many times to
    # converge on a stable signal. With 100 iterations and interleaved
    # task_ids, the pre-fix code reproducibly raises 1213 within a few
    # seconds on MySQL 8.0.
    ITERATIONS = 100
    TASKS_PER_THREAD = 25

    def _seed_audit_rows(self, engine, workflow_id: int, task_ids: List[int]) -> None:
        """Ensure one open audit row per task_id."""
        with Session(bind=engine) as s:
            for tid in task_ids:
                s.add(
                    TaskStatusAudit(
                        task_id=tid,
                        workflow_id=workflow_id,
                        previous_status=None,
                        new_status=TaskStatus.REGISTERING,
                        exited_at=None,
                    )
                )
            s.commit()

    def _reset_audit_rows(self, engine, workflow_id: int, task_ids: List[int]) -> None:
        """Drop all audit rows for these task_ids, then reseed one open row
        each — so the next iteration starts in a known state.
        """
        from sqlalchemy import delete

        with Session(bind=engine) as s:
            s.execute(
                delete(TaskStatusAudit).where(TaskStatusAudit.task_id.in_(task_ids))
            )
            s.commit()
        self._seed_audit_rows(engine, workflow_id, task_ids)

    def test_interleaved_bulk_closes_do_not_deadlock(self):
        engine = create_engine(_MYSQL_URI, future=True)
        workflow_id = int(os.environ.get("JOBMON_TEST_WORKFLOW_ID", "999000000"))

        # Interleave the two threads' task_id lists so each pair of
        # adjacent index entries belongs to different transactions.
        # This is the geometry that produces the cycle under the
        # pre-fix ``UPDATE … exited_at IS NULL`` scan.
        a_ids = [workflow_id + (2 * i + 1) for i in range(self.TASKS_PER_THREAD)]
        b_ids = [workflow_id + (2 * i + 2) for i in range(self.TASKS_PER_THREAD)]
        all_ids = a_ids + b_ids
        try:
            self._seed_audit_rows(engine, workflow_id, all_ids)

            errors: list[BaseException] = []
            barrier = threading.Barrier(2)

            def run(task_ids: List[int]):
                try:
                    for _ in range(self.ITERATIONS):
                        with Session(bind=engine) as s:
                            barrier.wait(timeout=30)
                            TransitionService.create_audit_records_bulk(
                                session=s,
                                records=[
                                    {
                                        "task_id": tid,
                                        "workflow_id": workflow_id,
                                        "previous_status": (TaskStatus.REGISTERING),
                                        "new_status": TaskStatus.QUEUED,
                                    }
                                    for tid in task_ids
                                ],
                            )
                            s.commit()
                except BaseException as e:  # noqa: BLE001
                    # On first error, abort the barrier so the peer
                    # doesn't block forever.
                    errors.append(e)
                    barrier.abort()

            t1 = threading.Thread(target=run, args=(a_ids,))
            t2 = threading.Thread(target=run, args=(b_ids,))
            t1.start()
            t2.start()
            t1.join(timeout=120)
            t2.join(timeout=120)

            assert not errors, (
                f"Concurrent bulk-close raised across {self.ITERATIONS} "
                "iterations — the rearrangement is not eliminating the "
                f"deadlock on this MySQL version. First error: {errors[0]!r}"
            )
        finally:
            # Always cleanup so the test is re-runnable.
            from sqlalchemy import delete

            with Session(bind=engine) as s:
                s.execute(
                    delete(TaskStatusAudit).where(TaskStatusAudit.task_id.in_(all_ids))
                )
                s.commit()

"""Unified transition service with TI-centric architecture and audit logging."""

from time import sleep
from typing import Any, Dict, List, Optional, Set, Type

import structlog
from sqlalchemy import insert, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from jobmon.core.constants import TaskStatus
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.services.task_fsm import TaskFSM

logger = structlog.get_logger(__name__)


class TransitionService:
    """Unified transition service with TI-centric model.

    - TaskInstance transitions drive Task transitions
    - Task FSM acts as the gate
    - All transitions audited to task_status_audit

    Handles both single-task and bulk transitions with:
    - Centralized FSM validation via TaskFSM
    - Built-in retry logic with exponential backoff
    - Row locking with NOWAIT (single) or SKIP LOCKED (bulk)
    - Automatic audit logging to task_status_audit table

    IMPORTANT - Transaction Management:
    - This service has internal retries that call session.rollback() on lock errors
    - Callers should NOT have uncommitted work they want to keep before calling
    - Call this service at the START of a transaction, or in its own transaction
    - The service does NOT call commit() - caller must commit on success
    """

    DEFAULT_MAX_RETRIES = 5
    DEFAULT_BASE_DELAY_MS = 2  # Exponential backoff: 2ms, 4ms, 8ms, 16ms, 32ms

    @classmethod
    def transition_task_instance(
        cls: Type["TransitionService"],
        session: Session,
        task_instance_id: int,
        task_id: int,
        new_ti_status: str,
        task_num_attempts: int,
        task_max_attempts: int,
        report_by_date: Optional[Any] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> Dict[str, Any]:
        """Worker-triggered: TI transition cascades to Task.

        1. Lock TI (NOWAIT) -> Lock Task (NOWAIT)
        2. Update TI status
        3. Compute implied Task status (via TaskFSM.get_task_status_for_ti)
        4. Validate Task FSM gate
        5. Update Task -> Audit Task

        Preserves TI-first locking order for atomicity.
        Has internal retries with rollback.

        Args:
            session: Database session (caller must commit on success)
            task_instance_id: TaskInstance ID to transition
            task_id: Task ID associated with the TaskInstance
            new_ti_status: New TaskInstance status
            task_num_attempts: Current task num_attempts
            task_max_attempts: Task max_attempts
            report_by_date: Optional report_by_date value (already computed)
            max_retries: Number of retry attempts on lock contention

        Returns:
            {
                "ti_updated": bool,
                "task_transitioned": bool,
                "task_status": str or None,
                "error": str or None
            }
        """
        # Get final task status (skipping intermediate ERROR_RECOVERABLE)
        final_t_status = TaskFSM.get_task_status_for_ti(
            ti_status=new_ti_status,
            task_num_attempts=task_num_attempts,
            task_max_attempts=task_max_attempts,
        )

        last_error: Any = None
        for attempt in range(max_retries):
            try:
                return cls._execute_ti_transition(
                    session=session,
                    task_instance_id=task_instance_id,
                    task_id=task_id,
                    new_ti_status=new_ti_status,
                    final_t_status=final_t_status,
                    report_by_date=report_by_date,
                )
            except OperationalError as e:
                last_error = e
                session.rollback()
                error_str = str(e).lower()
                is_lock_error = any(
                    msg in error_str
                    for msg in [
                        "could not obtain lock",
                        "nowait",
                        "lock wait timeout",
                        "deadlock",
                        "database is locked",
                    ]
                )
                if is_lock_error and attempt < max_retries - 1:
                    delay = cls.DEFAULT_BASE_DELAY_MS * (2 ** (attempt + 1)) / 1000
                    logger.warning(
                        f"Lock contention on TI transition, "
                        f"retry {attempt + 1}/{max_retries}",
                        task_instance_id=task_instance_id,
                        delay_seconds=delay,
                    )
                    sleep(delay)
                    continue
                raise

        # Max retries exceeded
        logger.error(
            "Max retries exceeded for TI transition",
            task_instance_id=task_instance_id,
            last_error=str(last_error),
        )
        return {
            "ti_updated": False,
            "task_transitioned": False,
            "task_status": None,
            "error": "Max retries exceeded",
        }

    @classmethod
    def _execute_ti_transition(
        cls: Type["TransitionService"],
        session: Session,
        task_instance_id: int,
        task_id: int,
        new_ti_status: str,
        final_t_status: Optional[str],
        report_by_date: Optional[Any],
    ) -> Dict[str, Any]:
        """Execute the TI transition with proper locking order."""
        # 1. Lock TI first (NOWAIT)
        ti_lock_stmt = (
            select(TaskInstance.id, TaskInstance.task_id)
            .where(TaskInstance.id == task_instance_id)
            .with_for_update(nowait=True)
        )
        session.execute(ti_lock_stmt).one()

        # 2. Lock Task (NOWAIT)
        task_lock_stmt = (
            select(Task.id, Task.status, Task.workflow_id)
            .where(Task.id == task_id)
            .with_for_update(nowait=True)
        )
        task_row = session.execute(task_lock_stmt).one()

        # 3. Update TI status
        ti_values: Dict[str, Any] = {
            "status": new_ti_status,
            "status_date": func.now(),
        }
        if report_by_date is not None:
            ti_values["report_by_date"] = report_by_date

        session.execute(
            update(TaskInstance)
            .where(TaskInstance.id == task_instance_id)
            .values(**ti_values)
            .execution_options(synchronize_session=False)
        )

        # 4. Update Task if transition needed
        task_transitioned = False
        if final_t_status and task_row.status != final_t_status:
            # Validate FSM gate
            if TaskFSM.is_valid_transition(task_row.status, final_t_status):
                session.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(status=final_t_status, status_date=func.now())
                    .execution_options(synchronize_session=False)
                )

                # 5. Audit Task transition
                session.execute(
                    insert(TaskStatusAudit).values(
                        task_id=task_id,
                        workflow_id=task_row.workflow_id,
                        previous_status=task_row.status,
                        new_status=final_t_status,
                    )
                )
                task_transitioned = True

                logger.info(
                    "Task transitioned via TI",
                    task_instance_id=task_instance_id,
                    task_id=task_id,
                    from_status=task_row.status,
                    to_status=final_t_status,
                )
            else:
                logger.warning(
                    "Task transition blocked by FSM",
                    task_id=task_id,
                    from_status=task_row.status,
                    to_status=final_t_status,
                )

        return {
            "ti_updated": True,
            "task_transitioned": task_transitioned,
            "task_status": final_t_status if task_transitioned else task_row.status,
            "error": None,
        }

    @classmethod
    def gate_tasks_for_queueing(
        cls: Type["TransitionService"],
        session: Session,
        task_ids: List[int],
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> Dict[str, List[int]]:
        """Distributor-triggered: Check Task gate for TI creation.

        1. Lock Tasks with SKIP LOCKED
        2. Validate FSM gate (REGISTERING/ADJUSTING_RESOURCES -> QUEUED)
        3. Update Tasks, increment num_attempts
        4. Audit Task status changes

        Returns: {"gated": [...], "invalid": [...], "locked": [...]}
        Caller creates TaskInstances for "gated" tasks.

        Args:
            session: Database session (caller must commit on success)
            task_ids: Task IDs to gate for queueing
            max_retries: Number of retry attempts on lock contention

        Returns:
            {
                "gated": [ids that passed gate and transitioned to QUEUED],
                "invalid": [ids that failed FSM validation],
                "locked": [ids that were locked by another transaction],
                "not_found": [ids that don't exist]
            }
        """
        result = cls.transition_tasks(
            session=session,
            task_ids=task_ids,
            to_status=TaskStatus.QUEUED,
            increment_attempts=True,
            max_retries=max_retries,
            use_skip_locked=True,
        )
        # Rename for clarity
        return {
            "gated": result["transitioned"],
            "invalid": result["invalid"],
            "locked": result["locked"],
            "not_found": result["not_found"],
        }

    @classmethod
    def transition_tasks(
        cls: Type["TransitionService"],
        session: Session,
        task_ids: List[int],
        to_status: str,
        increment_attempts: bool = False,
        max_retries: int = DEFAULT_MAX_RETRIES,
        use_skip_locked: bool = False,
    ) -> Dict[str, List[int]]:
        """Unified transition function for single or bulk operations.

        Args:
            session: Database session (caller must commit on success)
            task_ids: Task IDs to transition (1 or many)
            to_status: Target status (from TaskStatus constants)
            increment_attempts: If True, increment num_attempts (for QUEUED)
            max_retries: Number of retry attempts on lock contention
            use_skip_locked: If True, skip locked rows instead of failing
                            Use for bulk ops; requires MySQL 8.0+

        Returns:
            {
                "transitioned": [ids that were updated],
                "invalid": [ids that failed FSM validation],
                "locked": [ids that were locked by another transaction],
                "not_found": [ids that don't exist]
            }

        Note:
            - Has internal retries with rollback on lock errors
            - Does NOT commit - caller must commit on success
            - Call at START of transaction (rollback will clear any prior work)
        """
        if not task_ids:
            return {"transitioned": [], "invalid": [], "locked": [], "not_found": []}

        # Get valid source statuses from centralized FSM
        valid_from_statuses = TaskFSM.get_valid_sources(to_status)
        if not valid_from_statuses:
            logger.error(f"No valid source statuses for transition to {to_status}")
            return {
                "transitioned": [],
                "invalid": task_ids,
                "locked": [],
                "not_found": [],
            }

        last_error: Any = None
        for attempt in range(max_retries):
            try:
                return cls._execute_transition(
                    session=session,
                    task_ids=task_ids,
                    to_status=to_status,
                    valid_from_statuses=valid_from_statuses,
                    increment_attempts=increment_attempts,
                    use_skip_locked=use_skip_locked,
                )
            except OperationalError as e:
                last_error = e
                session.rollback()
                error_str = str(e).lower()
                is_lock_error = any(
                    msg in error_str
                    for msg in [
                        "could not obtain lock",
                        "nowait",
                        "lock wait timeout",
                        "deadlock",
                        "database is locked",
                    ]
                )
                if is_lock_error and attempt < max_retries - 1:
                    # Exponential backoff: 2ms, 4ms, 8ms, 16ms, 32ms
                    # Matches pattern in task_instance.py:186
                    delay = cls.DEFAULT_BASE_DELAY_MS * (2 ** (attempt + 1)) / 1000
                    logger.warning(
                        f"Lock contention on transition, retry {attempt + 1}/{max_retries}",
                        to_status=to_status,
                        task_count=len(task_ids),
                        delay_seconds=delay,
                    )
                    sleep(delay)
                    continue
                raise

        # Max retries exceeded
        logger.error(
            f"Max retries exceeded for transition to {to_status}",
            task_count=len(task_ids),
            last_error=str(last_error),
        )
        return {"transitioned": [], "invalid": [], "locked": task_ids, "not_found": []}

    @classmethod
    def _execute_transition(
        cls: Type["TransitionService"],
        session: Session,
        task_ids: List[int],
        to_status: str,
        valid_from_statuses: Set[str],
        increment_attempts: bool,
        use_skip_locked: bool,
    ) -> Dict[str, List[int]]:
        """Execute the actual transition (called within retry loop)."""
        # 1. Build lock query
        lock_query = (
            select(Task.id, Task.status, Task.workflow_id)
            .where(Task.id.in_(task_ids))
            .execution_options(synchronize_session=False)
        )

        if use_skip_locked:
            # SKIP LOCKED: Only lock available rows, skip already-locked ones
            # Requires MySQL 8.0+
            lock_query = lock_query.with_for_update(skip_locked=True)
        else:
            # NOWAIT: Fail immediately if any row is locked
            # Better for single-task operations where we want fast feedback
            lock_query = lock_query.with_for_update(nowait=True)

        rows = session.execute(lock_query).all()

        # 2. Categorize results
        found_ids = {row.id for row in rows}
        not_found = [tid for tid in task_ids if tid not in found_ids]

        # With SKIP LOCKED, missing IDs might be locked (not just non-existent)
        locked: List[int] = []
        if use_skip_locked and not_found:
            # Check which missing IDs actually exist but are locked
            existing = (
                session.execute(select(Task.id).where(Task.id.in_(not_found)))
                .scalars()
                .all()
            )
            existing_set = set(existing)
            locked = [tid for tid in not_found if tid in existing_set]
            not_found = [tid for tid in not_found if tid not in existing_set]

        # 3. Partition by FSM validity
        eligible = []
        invalid = []
        for row in rows:
            if row.status in valid_from_statuses:
                eligible.append(row)
            else:
                invalid.append(row.id)

        if not eligible:
            return {
                "transitioned": [],
                "invalid": invalid,
                "locked": locked,
                "not_found": not_found,
            }

        eligible_ids = [row.id for row in eligible]

        # 4. Bulk update
        update_values: Dict[str, Any] = {
            "status": to_status,
            "status_date": func.now(),
        }
        if increment_attempts:
            update_values["num_attempts"] = Task.num_attempts + 1

        session.execute(
            update(Task)
            .where(Task.id.in_(eligible_ids))
            .values(**update_values)
            .execution_options(synchronize_session=False)
        )

        # 5. Bulk audit insert
        audit_values = [
            {
                "task_id": row.id,
                "workflow_id": row.workflow_id,
                "previous_status": row.status,
                "new_status": to_status,
            }
            for row in eligible
        ]
        session.execute(insert(TaskStatusAudit).values(audit_values))

        logger.info(
            "Tasks transitioned",
            to_status=to_status,
            count=len(eligible_ids),
            invalid_count=len(invalid),
            locked_count=len(locked),
        )

        return {
            "transitioned": eligible_ids,
            "invalid": invalid,
            "locked": locked,
            "not_found": not_found,
        }

"""Unified transition service with TI-centric architecture and audit logging."""

from time import sleep
from typing import Any, Dict, List, Optional, Set, Tuple, Type

import structlog
from sqlalchemy import insert, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from jobmon.core.constants import TaskInstanceStatus, TaskStatus
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

    # TaskInstance valid transitions (mirrors TaskInstance.valid_transitions)
    TI_VALID_TRANSITIONS: Set[Tuple[str, str]] = {
        (TaskInstanceStatus.QUEUED, TaskInstanceStatus.INSTANTIATED),
        (TaskInstanceStatus.QUEUED, TaskInstanceStatus.KILL_SELF),
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.LAUNCHED),
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.NO_DISTRIBUTOR_ID),
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.KILL_SELF),
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.RUNNING),
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.RUNNING),
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.UNKNOWN_ERROR),
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.NO_HEARTBEAT),
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.RESOURCE_ERROR),
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.KILL_SELF),
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.ERROR_FATAL),
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.TRIAGING),
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.ERROR),
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.UNKNOWN_ERROR),
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.RESOURCE_ERROR),
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.KILL_SELF),
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.DONE),
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.RUNNING),
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.RESOURCE_ERROR),
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.UNKNOWN_ERROR),
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.ERROR_FATAL),
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.ERROR_FATAL),
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.DONE),
        (TaskInstanceStatus.NO_HEARTBEAT, TaskInstanceStatus.ERROR),
    }

    # TaskInstance untimely transitions (race conditions to ignore gracefully)
    TI_UNTIMELY_TRANSITIONS: Set[Tuple[str, str]] = {
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.LAUNCHED),
        (TaskInstanceStatus.ERROR, TaskInstanceStatus.LAUNCHED),
        (TaskInstanceStatus.ERROR, TaskInstanceStatus.UNKNOWN_ERROR),
        (TaskInstanceStatus.ERROR, TaskInstanceStatus.ERROR),
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.DONE),
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.ERROR),
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.RESOURCE_ERROR),
        (TaskInstanceStatus.RESOURCE_ERROR, TaskInstanceStatus.UNKNOWN_ERROR),
        (TaskInstanceStatus.DONE, TaskInstanceStatus.UNKNOWN_ERROR),
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.ERROR),
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.RESOURCE_ERROR),
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.UNKNOWN_ERROR),
    }

    # TaskInstance error states (including NO_HEARTBEAT for orphaned TI handling)
    TI_ERROR_STATES: Set[str] = {
        TaskInstanceStatus.NO_DISTRIBUTOR_ID,
        TaskInstanceStatus.ERROR,
        TaskInstanceStatus.UNKNOWN_ERROR,
        TaskInstanceStatus.RESOURCE_ERROR,
        TaskInstanceStatus.NO_HEARTBEAT,
    }

    # Task terminal states
    TASK_TERMINAL_STATES: Set[str] = {TaskStatus.DONE, TaskStatus.ERROR_FATAL}

    # Lock error messages to detect
    LOCK_ERROR_MESSAGES = [
        "could not obtain lock",
        "nowait",
        "lock wait timeout",
        "deadlock",
        "database is locked",
    ]

    @classmethod
    def _is_lock_error(cls: Type["TransitionService"], error: Exception) -> bool:
        """Check if an OperationalError is a lock-related error."""
        error_str = str(error).lower()
        return any(msg in error_str for msg in cls.LOCK_ERROR_MESSAGES)

    @classmethod
    def _calculate_backoff_delay(cls: Type["TransitionService"], attempt: int) -> float:
        """Calculate exponential backoff delay in seconds."""
        return cls.DEFAULT_BASE_DELAY_MS * (2 ** (attempt + 1)) / 1000

    @classmethod
    def _is_error_retry_transition(
        cls: Type["TransitionService"],
        ti_status: str,
        final_task_status: str,
    ) -> bool:
        """Check if this is an error-with-retry transition.

        Returns True when a TaskInstance enters an error state but the Task
        is transitioning to REGISTERING or ADJUSTING_RESOURCES (i.e., retrying).
        This indicates we should record an ERROR_RECOVERABLE audit entry.
        """
        return ti_status in cls.TI_ERROR_STATES and final_task_status in {
            TaskStatus.REGISTERING,
            TaskStatus.ADJUSTING_RESOURCES,
        }

    @classmethod
    def create_audit_record_with_immediate_exit(
        cls: Type["TransitionService"],
        session: Session,
        task_id: int,
        workflow_id: int,
        previous_status: str,
        new_status: str,
    ) -> None:
        """Create audit record with both entered_at and exited_at set (for transient states).

        Used for ERROR_RECOVERABLE status which is a transient state - the task
        enters and exits it in the same transition.

        Args:
            session: Database session
            task_id: The task ID being transitioned
            workflow_id: The workflow ID the task belongs to
            previous_status: The status the task is transitioning from
            new_status: The transient status (e.g., ERROR_RECOVERABLE)
        """
        # Close previous open record
        session.execute(
            update(TaskStatusAudit)
            .where(
                TaskStatusAudit.task_id == task_id,
                TaskStatusAudit.exited_at.is_(None),
            )
            .values(exited_at=func.now())
            .execution_options(synchronize_session=False)
        )

        # Create new record with exited_at = entered_at = func.now()
        session.execute(
            insert(TaskStatusAudit).values(
                task_id=task_id,
                workflow_id=workflow_id,
                previous_status=previous_status,
                new_status=new_status,
                entered_at=func.now(),
                exited_at=func.now(),
            )
        )

    @classmethod
    def create_audit_record(
        cls: Type["TransitionService"],
        session: Session,
        task_id: int,
        workflow_id: int,
        previous_status: str,
        new_status: str,
    ) -> None:
        """Create audit record with proper exited_at handling.

        1. Closes any open audit record for this task (sets exited_at = func.now())
        2. Creates new audit record (entered_at uses model default = func.now())

        Uses database func.now() for timestamps to ensure server-time consistency.

        Args:
            session: Database session
            task_id: The task ID being transitioned
            workflow_id: The workflow ID the task belongs to
            previous_status: The status the task is transitioning from
            new_status: The status the task is transitioning to
        """
        # Close previous open record
        session.execute(
            update(TaskStatusAudit)
            .where(
                TaskStatusAudit.task_id == task_id,
                TaskStatusAudit.exited_at.is_(None),
            )
            .values(exited_at=func.now())
            .execution_options(synchronize_session=False)
        )

        # Create new record (entered_at uses model default)
        session.execute(
            insert(TaskStatusAudit).values(
                task_id=task_id,
                workflow_id=workflow_id,
                previous_status=previous_status,
                new_status=new_status,
            )
        )

    @classmethod
    def create_audit_records_bulk(
        cls: Type["TransitionService"],
        session: Session,
        records: List[Dict],
    ) -> None:
        """Bulk create audit records with proper exited_at handling.

        Uses database func.now() for timestamps.

        Args:
            session: Database session
            records: List of dicts with task_id, workflow_id, previous_status, new_status
        """
        if not records:
            return

        task_ids = [r["task_id"] for r in records]

        # Bulk close previous open records
        session.execute(
            update(TaskStatusAudit)
            .where(
                TaskStatusAudit.task_id.in_(task_ids),
                TaskStatusAudit.exited_at.is_(None),
            )
            .values(exited_at=func.now())
            .execution_options(synchronize_session=False)
        )

        # Bulk insert new records (entered_at uses model default)
        session.execute(insert(TaskStatusAudit).values(records))

    @classmethod
    def transition_task_instance(
        cls: Type["TransitionService"],
        session: Session,
        task_instance_id: int,
        task_id: int,
        current_ti_status: str,
        new_ti_status: str,
        task_num_attempts: int,
        task_max_attempts: int,
        report_by_date: Optional[Any] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> Dict[str, Any]:
        """Worker-triggered: TI transition cascades to Task.

        1. Validate TI transition (untimely, valid_transitions)
        2. Lock TI (NOWAIT) -> Lock Task (NOWAIT)
        3. Update TI status
        4. Compute implied Task status (via TaskFSM.get_task_status_for_ti)
        5. Handle orphaned TI (Task terminal → TI goes to ERROR_FATAL)
        6. Validate Task FSM gate
        7. Update Task -> Audit Task

        Preserves TI-first locking order for atomicity.
        Has internal retries with rollback.

        Args:
            session: Database session (caller must commit on success)
            task_instance_id: TaskInstance ID to transition
            task_id: Task ID associated with the TaskInstance
            current_ti_status: Current TaskInstance status (for validation)
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
                "error": str or None,  # "untimely_transition", "invalid_ti_transition", None
                "orphaned": bool,      # True if Task was terminal
            }
        """
        # Validate TI transition before any locking
        transition_tuple = (current_ti_status, new_ti_status)

        # Check for untimely transitions (race conditions)
        if transition_tuple in cls.TI_UNTIMELY_TRANSITIONS:
            logger.warning(
                "Untimely TI transition rejected",
                task_instance_id=task_instance_id,
                current_ti_status=current_ti_status,
                new_ti_status=new_ti_status,
            )
            return {
                "ti_updated": False,
                "task_transitioned": False,
                "task_status": None,
                "error": "untimely_transition",
                "orphaned": False,
            }

        # Check for invalid TI transitions
        if transition_tuple not in cls.TI_VALID_TRANSITIONS:
            logger.warning(
                "Invalid TI transition",
                task_instance_id=task_instance_id,
                current_ti_status=current_ti_status,
                new_ti_status=new_ti_status,
            )
            return {
                "ti_updated": False,
                "task_transitioned": False,
                "task_status": None,
                "error": "invalid_ti_transition",
                "orphaned": False,
            }

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
                if cls._is_lock_error(e) and attempt < max_retries - 1:
                    delay = cls._calculate_backoff_delay(attempt)
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
            "error": "max_retries_exceeded",
            "orphaned": False,
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

        # 3. Check for orphaned TI scenario (Task is terminal)
        orphaned = False
        actual_ti_status = new_ti_status
        if task_row.status in cls.TASK_TERMINAL_STATES:
            # Task is already DONE or ERROR_FATAL
            # If TI is transitioning to an error state, force it to ERROR_FATAL
            if (
                new_ti_status in cls.TI_ERROR_STATES
                or new_ti_status == TaskInstanceStatus.ERROR_FATAL
            ):
                logger.info(
                    "Task already terminal, orphaned TI transitioning to ERROR_FATAL",
                    task_instance_id=task_instance_id,
                    task_id=task_id,
                    current_task_status=task_row.status,
                    original_ti_status=new_ti_status,
                )
                actual_ti_status = TaskInstanceStatus.ERROR_FATAL
                orphaned = True

        # 4. Update TI status
        ti_values: Dict[str, Any] = {
            "status": actual_ti_status,
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

        # 5. Update Task if transition needed (skip if orphaned)
        task_transitioned = False
        if not orphaned and final_t_status and task_row.status != final_t_status:
            # Validate FSM gate
            if TaskFSM.is_valid_transition(task_row.status, final_t_status):
                session.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(status=final_t_status, status_date=func.now())
                    .execution_options(synchronize_session=False)
                )

                # 6. Audit Task transition (closes previous, creates new)
                # For error-retry, record ERROR_RECOVERABLE as transient
                if cls._is_error_retry_transition(new_ti_status, final_t_status):
                    # First: record ERROR_RECOVERABLE (transient state)
                    cls.create_audit_record_with_immediate_exit(
                        session=session,
                        task_id=task_id,
                        workflow_id=task_row.workflow_id,
                        previous_status=task_row.status,
                        new_status=TaskStatus.ERROR_RECOVERABLE,
                    )
                    # Second: record final status (REGISTERING or ADJUSTING_RESOURCES)
                    cls.create_audit_record(
                        session=session,
                        task_id=task_id,
                        workflow_id=task_row.workflow_id,
                        previous_status=TaskStatus.ERROR_RECOVERABLE,
                        new_status=final_t_status,
                    )
                else:
                    cls.create_audit_record(
                        session=session,
                        task_id=task_id,
                        workflow_id=task_row.workflow_id,
                        previous_status=task_row.status,
                        new_status=final_t_status,
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
            "orphaned": orphaned,
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
                if cls._is_lock_error(e) and attempt < max_retries - 1:
                    delay = cls._calculate_backoff_delay(attempt)
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

        # 5. Bulk audit insert (closes previous records, creates new ones)
        audit_values = [
            {
                "task_id": row.id,
                "workflow_id": row.workflow_id,
                "previous_status": row.status,
                "new_status": to_status,
            }
            for row in eligible
        ]
        cls.create_audit_records_bulk(session=session, records=audit_values)

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

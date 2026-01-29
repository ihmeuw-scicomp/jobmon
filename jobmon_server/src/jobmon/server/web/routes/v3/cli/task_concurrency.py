"""Routes for Task Concurrency Timeline."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import structlog
from fastapi import Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from jobmon.core.constants import TaskStatus
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.schemas.task_concurrency import (
    TaskConcurrencyResponse,
    TaskStatusAuditRecord,
    TaskStatusAuditResponse,
    WorkflowTaskTemplatesResponse,
)

logger = structlog.get_logger(__name__)


# Active status categories (no terminal states)
# PENDING includes: Q (QUEUED), I (INSTANTIATING)
PENDING_STATUSES = frozenset({TaskStatus.QUEUED, TaskStatus.INSTANTIATING})

# Valid group_by option
GROUP_BY_STATUS = "status"


def _is_active_at_time(
    period_start: datetime, period_end: Optional[datetime], bucket_time: datetime
) -> bool:
    """Check if a status period was active at a specific point in time.

    A period is active at bucket_time if:
    entered_at <= bucket_time AND (exited_at IS NULL OR exited_at > bucket_time)
    """
    return period_start <= bucket_time and (
        period_end is None or period_end > bucket_time
    )


def _categorize_status(status: str) -> Optional[str]:
    """Map a task status to its display category.

    Returns the category name or None if the status should not be displayed.
    """
    if status in PENDING_STATUSES:
        return "PENDING"
    if status == TaskStatus.LAUNCHED:
        return "LAUNCHED"
    if status == TaskStatus.RUNNING:
        return "RUNNING"
    return None


@api_v3_router.get("/workflow/{workflow_id}/task_concurrency")
async def get_task_concurrency(
    workflow_id: int,
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range"),
    bucket_seconds: int = Query(
        None,
        ge=1,
        le=3600,
        description="Bucket size in seconds (overrides bucket_minutes)",
    ),
    bucket_minutes: int = Query(1, ge=1, le=60, description="Bucket size in minutes"),
    task_template_name: Optional[str] = Query(
        None, description="Filter by task template name"
    ),
    db: Session = Depends(get_db),
) -> TaskConcurrencyResponse:
    """Get concurrent task counts over time, grouped by status.

    Returns time-bucketed counts for timeseries visualization.
    Uses task_status_audit table with ix_task_status_audit_workflow_time index.

    Groups by active status category (PENDING, LAUNCHED, RUNNING).
    Note: Only active task statuses are shown (no DONE/ERROR terminal states).

    The response includes:
    - buckets: List of time bucket start times
    - series: Dict mapping status names to lists of concurrent counts
    - template_breakdown: Dict mapping status to list of {template: count} per bucket

    Example response:
    {
        "buckets": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", ...],
        "series": {
            "PENDING": [5, 8, 10, 7, ...],
            "LAUNCHED": [3, 5, 4, 2, ...],
            "RUNNING": [2, 3, 4, 3, ...]
        },
        "template_breakdown": {
            "RUNNING": [{"template_a": 2, "template_b": 1}, ...]
        }
    }
    """
    # Determine bucket size
    if bucket_seconds is not None:
        bucket_delta = timedelta(seconds=bucket_seconds)
    else:
        bucket_delta = timedelta(minutes=bucket_minutes)

    logger.info(
        "Getting task concurrency timeline",
        workflow_id=workflow_id,
        start_time=start_time,
        end_time=end_time,
        bucket_delta_seconds=bucket_delta.total_seconds(),
        task_template_name=task_template_name,
    )

    # If no time range specified, use workflow's time range from audit table
    if start_time is None or end_time is None:
        time_range_query = select(
            func.min(TaskStatusAudit.entered_at),
            func.max(
                func.coalesce(TaskStatusAudit.exited_at, TaskStatusAudit.entered_at)
            ),
        ).where(TaskStatusAudit.workflow_id == workflow_id)
        time_range = db.execute(time_range_query).one()

        if time_range[0] is None:
            # No audit records for this workflow
            return TaskConcurrencyResponse(buckets=[], series={})

        if start_time is None:
            start_time = time_range[0]
        if end_time is None:
            # Extend end time by one bucket to ensure we capture final state
            end_time = time_range[1] + bucket_delta

    # Generate time buckets
    buckets: List[datetime] = []
    current = start_time
    while current < end_time:
        buckets.append(current)
        current = current + bucket_delta

    series, template_breakdown = _get_series_by_status(
        db,
        workflow_id,
        start_time,
        end_time,
        buckets,
        task_template_name=task_template_name,
    )

    # Convert bucket datetimes to ISO strings for JSON
    bucket_strings = [b.isoformat() for b in buckets]

    logger.info(
        "Task concurrency timeline generated",
        workflow_id=workflow_id,
        num_buckets=len(buckets),
        series_count=len(series),
    )

    return TaskConcurrencyResponse(
        buckets=bucket_strings,
        series=series,
        template_breakdown=template_breakdown,
    )


def _get_series_by_status(
    db: Session,
    workflow_id: int,
    start_time: datetime,
    end_time: datetime,
    buckets: List[datetime],
    task_template_name: Optional[str] = None,
) -> tuple[Dict[str, List[int]], Dict[str, List[Dict[str, int]]]]:
    """Get series data grouped by status category using point-in-time sampling.

    For each bucket timestamp, count what status each task is in at that exact moment.
    A task is in status S at time T if:
    entered_at <= T AND (exited_at IS NULL OR exited_at > T)

    Args:
        task_template_name: Optional filter to only include tasks from a specific template

    Returns:
        Tuple of (series, template_breakdown):
        - series: Dict mapping status to list of counts per bucket
        - template_breakdown: Dict mapping status to list of {template: count} dicts per bucket
    """
    # Always join to get template names for breakdown
    status_periods_query = (
        select(
            TaskStatusAudit.task_id,
            TaskStatusAudit.new_status,
            TaskStatusAudit.entered_at,
            TaskStatusAudit.exited_at,
            TaskTemplate.name.label("template_name"),
        )
        .join(Task, Task.id == TaskStatusAudit.task_id)
        .join(Node, Node.id == Task.node_id)
        .join(
            TaskTemplateVersion, TaskTemplateVersion.id == Node.task_template_version_id
        )
        .join(TaskTemplate, TaskTemplate.id == TaskTemplateVersion.task_template_id)
        .where(
            and_(
                TaskStatusAudit.workflow_id == workflow_id,
                TaskStatusAudit.entered_at <= end_time,
                (
                    (TaskStatusAudit.exited_at.is_(None))
                    | (TaskStatusAudit.exited_at >= start_time)
                ),
            )
        )
    )

    if task_template_name:
        status_periods_query = status_periods_query.where(
            TaskTemplate.name == task_template_name
        )

    status_periods = db.execute(status_periods_query).all()

    # Initialize series and template breakdown
    series: Dict[str, List[int]] = {
        "PENDING": [0] * len(buckets),
        "LAUNCHED": [0] * len(buckets),
        "RUNNING": [0] * len(buckets),
    }
    template_breakdown: Dict[str, List[Dict[str, int]]] = {
        "PENDING": [{} for _ in buckets],
        "LAUNCHED": [{} for _ in buckets],
        "RUNNING": [{} for _ in buckets],
    }

    for i, bucket_time in enumerate(buckets):
        for task_id, status, period_start, period_end, template_name in status_periods:
            if _is_active_at_time(period_start, period_end, bucket_time):
                category = _categorize_status(status)
                if category:
                    series[category][i] += 1
                    # Track template breakdown
                    if template_name not in template_breakdown[category][i]:
                        template_breakdown[category][i][template_name] = 0
                    template_breakdown[category][i][template_name] += 1

    return series, template_breakdown


@api_v3_router.get("/workflow/{workflow_id}/task_status_audit")
async def get_task_status_audit(
    workflow_id: int,
    task_id: Optional[int] = Query(None, description="Filter by specific task ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    db: Session = Depends(get_db),
) -> TaskStatusAuditResponse:
    """Get task status audit records for a workflow.

    Returns the audit trail showing all task status transitions.
    Useful for debugging and understanding task lifecycle.

    Args:
        workflow_id: The workflow ID to get audit records for
        task_id: Optional filter for a specific task
        limit: Maximum number of records to return

    Returns:
        List of audit records with task_id, previous_status, new_status, timestamps
    """
    logger.info(
        "Getting task status audit",
        workflow_id=workflow_id,
        task_id=task_id,
        limit=limit,
    )

    query = select(
        TaskStatusAudit.task_id,
        TaskStatusAudit.previous_status,
        TaskStatusAudit.new_status,
        TaskStatusAudit.entered_at,
        TaskStatusAudit.exited_at,
    ).where(TaskStatusAudit.workflow_id == workflow_id)

    if task_id is not None:
        query = query.where(TaskStatusAudit.task_id == task_id)

    query = query.order_by(TaskStatusAudit.entered_at.desc()).limit(limit)

    records = db.execute(query).all()

    audit_records = [
        TaskStatusAuditRecord(
            task_id=r[0],
            previous_status=r[1],
            new_status=r[2],
            entered_at=r[3].isoformat() if r[3] else None,
            exited_at=r[4].isoformat() if r[4] else None,
        )
        for r in records
    ]

    return TaskStatusAuditResponse(audit_records=audit_records)


@api_v3_router.get("/workflow/{workflow_id}/task_templates")
async def get_workflow_task_templates(
    workflow_id: int,
    db: Session = Depends(get_db),
) -> WorkflowTaskTemplatesResponse:
    """Get list of task template names used in a workflow.

    Returns distinct task template names for tasks in this workflow.
    Used for filtering the concurrency view by template.

    Args:
        workflow_id: The workflow ID to get templates for

    Returns:
        List of distinct task template names, sorted alphabetically
    """
    logger.info(
        "Getting workflow task templates",
        workflow_id=workflow_id,
    )

    # Get distinct task template names for this workflow
    query = (
        select(TaskTemplate.name)
        .distinct()
        .join(
            TaskTemplateVersion, TaskTemplateVersion.task_template_id == TaskTemplate.id
        )
        .join(Node, Node.task_template_version_id == TaskTemplateVersion.id)
        .join(Task, Task.node_id == Node.id)
        .where(Task.workflow_id == workflow_id)
        .order_by(TaskTemplate.name)
    )

    result = db.execute(query).all()
    template_names = [row[0] for row in result]

    return WorkflowTaskTemplatesResponse(task_templates=template_names)

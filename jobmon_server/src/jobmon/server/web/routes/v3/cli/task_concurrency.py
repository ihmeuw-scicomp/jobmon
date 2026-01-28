"""Routes for Task Concurrency Timeline."""

from datetime import datetime, timedelta
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Optional

import structlog
from fastapi import Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core.constants import TaskStatus
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router

logger = structlog.get_logger(__name__)


@api_v3_router.get("/workflow/{workflow_id}/task_concurrency")
async def get_task_concurrency(
    workflow_id: int,
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range"),
    bucket_minutes: int = Query(1, ge=1, le=60, description="Bucket size in minutes"),
    db: Session = Depends(get_db),
) -> Any:
    """Get concurrent RUNNING task counts by task_template over time.

    Returns time-bucketed counts for timeseries visualization.
    Uses task_status_audit table with ix_task_status_audit_workflow_time index.

    The response includes:
    - buckets: List of time bucket start times
    - series: Dict mapping task_template names to lists of concurrent counts

    Example response:
    {
        "buckets": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", ...],
        "series": {
            "my_template": [5, 8, 10, 7, ...],
            "other_template": [2, 3, 4, 3, ...]
        }
    }
    """
    logger.info(
        "Getting task concurrency timeline",
        workflow_id=workflow_id,
        start_time=start_time,
        end_time=end_time,
        bucket_minutes=bucket_minutes,
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
            return JSONResponse(
                content={"buckets": [], "series": {}},
                status_code=StatusCodes.OK,
            )

        if start_time is None:
            start_time = time_range[0]
        if end_time is None:
            end_time = time_range[1]

    # Get RUNNING periods from audit table
    # A task is RUNNING when new_status = RUNNING (entered_at)
    # and stops being RUNNING when exited_at is set
    running_periods_query = (
        select(
            TaskStatusAudit.task_id,
            TaskStatusAudit.entered_at,
            TaskStatusAudit.exited_at,
            TaskTemplate.name.label("task_template_name"),
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
                TaskStatusAudit.new_status == TaskStatus.RUNNING,
                TaskStatusAudit.entered_at <= end_time,
                # Include periods that haven't ended or ended after start
                (
                    (TaskStatusAudit.exited_at.is_(None))
                    | (TaskStatusAudit.exited_at >= start_time)
                ),
            )
        )
    )
    running_periods = db.execute(running_periods_query).all()

    # Generate time buckets
    bucket_delta = timedelta(minutes=bucket_minutes)
    buckets: List[datetime] = []
    current = start_time
    while current < end_time:
        buckets.append(current)
        current = current + bucket_delta

    # Initialize series data structure
    # template_name -> list of counts (one per bucket)
    series: Dict[str, List[int]] = {}

    # Process each running period
    for task_id, period_start, period_end, template_name in running_periods:
        if template_name not in series:
            series[template_name] = [0] * len(buckets)

        # Use end_time if period hasn't ended yet
        effective_end = period_end if period_end is not None else end_time

        # Count which buckets this period overlaps with
        for i, bucket_start in enumerate(buckets):
            bucket_end = bucket_start + bucket_delta

            # Check if period overlaps with this bucket
            # Overlap exists if: period_start < bucket_end AND effective_end > bucket_start
            if period_start < bucket_end and effective_end > bucket_start:
                series[template_name][i] += 1

    # Convert bucket datetimes to ISO strings for JSON
    bucket_strings = [b.isoformat() for b in buckets]

    logger.info(
        "Task concurrency timeline generated",
        workflow_id=workflow_id,
        num_buckets=len(buckets),
        num_templates=len(series),
    )

    return JSONResponse(
        content={
            "buckets": bucket_strings,
            "series": series,
        },
        status_code=StatusCodes.OK,
    )


@api_v3_router.get("/workflow/{workflow_id}/task_status_audit")
async def get_task_status_audit(
    workflow_id: int,
    task_id: Optional[int] = Query(None, description="Filter by specific task ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    db: Session = Depends(get_db),
) -> Any:
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
        {
            "task_id": r[0],
            "previous_status": r[1],
            "new_status": r[2],
            "entered_at": r[3].isoformat() if r[3] else None,
            "exited_at": r[4].isoformat() if r[4] else None,
        }
        for r in records
    ]

    return JSONResponse(
        content={"audit_records": audit_records},
        status_code=StatusCodes.OK,
    )

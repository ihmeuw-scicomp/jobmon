"""Routes for Task Concurrency Timeline."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import structlog
from fastapi import Depends, HTTPException, Query
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
    TemplateTimelineResponse,
    TemplateTimelineRow,
    WorkflowTaskTemplatesResponse,
)

logger = structlog.get_logger(__name__)


# Active status categories (no terminal states)
# PENDING includes: Q (QUEUED), I (INSTANTIATING)
PENDING_STATUSES = frozenset({TaskStatus.QUEUED, TaskStatus.INSTANTIATING})

# Error statuses - both recoverable (E) and fatal (F) combined into "ERROR"
ERROR_STATUSES = frozenset({TaskStatus.ERROR_RECOVERABLE, TaskStatus.ERROR_FATAL})

# Terminal status for completion counting
DONE_STATUS = TaskStatus.DONE

# Valid group_by option
GROUP_BY_STATUS = "status"


def _is_active_during_interval(
    period_start: datetime,
    period_end: Optional[datetime],
    interval_start: datetime,
    interval_end: datetime,
) -> bool:
    """Check if a status period overlaps with a time interval.

    A period overlaps with [interval_start, interval_end) if:
    period_start < interval_end AND (period_end IS NULL OR period_end > interval_start)

    This counts tasks that were in the status at ANY point during the interval,
    not just at a single sample point.
    """
    return period_start < interval_end and (
        period_end is None or period_end > interval_start
    )


def _categorize_status(status: str) -> Optional[str]:
    """Map a task status to its display category.

    Returns the category name or None if the status should not be displayed.
    """
    if status == TaskStatus.REGISTERING:
        return "REGISTERED"
    if status in PENDING_STATUSES:
        return "PENDING"
    if status == TaskStatus.LAUNCHED:
        return "LAUNCHED"
    if status == TaskStatus.RUNNING:
        return "RUNNING"
    if status in ERROR_STATUSES:
        return "ERROR"
    if status == DONE_STATUS:
        return "DONE"
    return None


@api_v3_router.get("/workflow/{workflow_id}/task_concurrency")
async def get_task_concurrency(
    workflow_id: int,
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range"),
    group_by: str = Query(
        GROUP_BY_STATUS,
        description='Grouping strategy for the series (only "status" supported)',
    ),
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

    Groups by status category (PENDING, LAUNCHED, RUNNING, ERROR, DONE).
    The only supported grouping strategy is "status".
    Note: DONE and ERROR show tasks that entered those states within each bucket
    (not cumulative or interval overlap).

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
            "RUNNING": [2, 3, 4, 3, ...],
            "ERROR": [0, 1, 0, 2, ...],  // error events within each bucket
            "DONE": [0, 2, 5, 3, ...]  // tasks completed within each bucket
        },
        "template_breakdown": {
            "RUNNING": [{"template_a": 2, "template_b": 1}, ...],
            "ERROR": [{"template_a": 0, "template_b": 1}, ...],
            "DONE": [{"template_a": 0, "template_b": 2}, ...]
        }
    }
    """
    # Determine bucket size
    if group_by != GROUP_BY_STATUS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported group_by value '{group_by}'. "
            f"Only '{GROUP_BY_STATUS}' is supported.",
        )

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
        group_by=group_by,
    )

    # If no time range specified, use workflow's time range from audit table.
    # Use entered_at (not exited_at) for the max — exited_at can be inflated
    # when a record is closed by a non-displayed transition (e.g. reset to
    # REGISTERING closes DONE records at the reset time, stretching the axis).
    # Exclude REGISTERING — it's a non-transient parking state that produces
    # no visible series data.
    if start_time is None or end_time is None:
        time_range_query = select(
            func.min(TaskStatusAudit.entered_at),
            func.max(TaskStatusAudit.entered_at),
        ).where(
            TaskStatusAudit.workflow_id == workflow_id,
            TaskStatusAudit.new_status != TaskStatus.REGISTERING,
        )
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
        bucket_delta,
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
    bucket_delta: timedelta,
    task_template_name: Optional[str] = None,
) -> tuple[Dict[str, List[int]], Dict[str, List[Dict[str, int]]]]:
    """Get series data grouped by status category.

    For active statuses (PENDING, LAUNCHED, RUNNING): interval-based counting.
    A task is counted if it was in status S at any point during the bucket interval.
    period_start < bucket_end AND (period_end IS NULL OR period_end > bucket_start)

    For DONE: count tasks that completed within each bucket interval.
    A task completed in bucket [T, T+delta) if entered_at is in that range.

    Args:
        bucket_delta: Duration of each bucket (for DONE interval counting)
        task_template_name: Optional filter to only include tasks from a specific template

    Returns:
        Tuple of (series, template_breakdown):
        - series: Dict mapping status to list of counts per bucket
        - template_breakdown: Dict mapping status to list of {template: count} dicts per bucket
    """
    # Initialize series and template breakdown
    series: Dict[str, List[int]] = {
        "PENDING": [0] * len(buckets),
        "LAUNCHED": [0] * len(buckets),
        "RUNNING": [0] * len(buckets),
        "ERROR": [0] * len(buckets),
        "DONE": [0] * len(buckets),
    }
    template_breakdown: Dict[str, List[Dict[str, int]]] = {
        "PENDING": [{} for _ in buckets],
        "LAUNCHED": [{} for _ in buckets],
        "RUNNING": [{} for _ in buckets],
        "ERROR": [{} for _ in buckets],
        "DONE": [{} for _ in buckets],
    }

    # Fetch all relevant status periods with template names
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

    for i, bucket_time in enumerate(buckets):
        bucket_end = bucket_time + bucket_delta
        # Track which tasks we've already counted per category to avoid
        # double-counting when a task has multiple status periods in the
        # same category (e.g., Q then I both map to PENDING)
        counted_tasks: Dict[str, set] = {
            "PENDING": set(),
            "LAUNCHED": set(),
            "RUNNING": set(),
            "ERROR": set(),
            "DONE": set(),
        }

        for task_id, status, period_start, period_end, template_name in status_periods:
            category = _categorize_status(status)

            # Skip categories not tracked by the concurrency view
            # (REGISTERED is only used by the timeline endpoint)
            if category not in counted_tasks:
                continue

            # For active statuses (PENDING, LAUNCHED, RUNNING): count tasks active
            # at any point during the interval (interval overlap)
            if category in {"PENDING", "LAUNCHED", "RUNNING"}:
                if _is_active_during_interval(
                    period_start, period_end, bucket_time, bucket_end
                ):
                    if task_id not in counted_tasks[category]:
                        counted_tasks[category].add(task_id)
                        series[category][i] += 1
                        if template_name not in template_breakdown[category][i]:
                            template_breakdown[category][i][template_name] = 0
                        template_breakdown[category][i][template_name] += 1

            # For ERROR: count tasks that entered an error state within this bucket
            # (point-in-time counting, like DONE)
            if category == "ERROR":
                if (
                    bucket_time <= period_start < bucket_end
                    and task_id not in counted_tasks["ERROR"]
                ):
                    counted_tasks["ERROR"].add(task_id)
                    series["ERROR"][i] += 1
                    if template_name not in template_breakdown["ERROR"][i]:
                        template_breakdown["ERROR"][i][template_name] = 0
                    template_breakdown["ERROR"][i][template_name] += 1

            # For DONE: count tasks that entered DONE within this bucket interval
            if status == DONE_STATUS:
                if (
                    bucket_time <= period_start < bucket_end
                    and task_id not in counted_tasks["DONE"]
                ):
                    counted_tasks["DONE"].add(task_id)
                    series["DONE"][i] += 1
                    if template_name not in template_breakdown["DONE"][i]:
                        template_breakdown["DONE"][i][template_name] = 0
                    template_breakdown["DONE"][i][template_name] += 1

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


TIMELINE_CATEGORIES = [
    "REGISTERED",
    "PENDING",
    "LAUNCHED",
    "RUNNING",
    "ERROR",
    "DONE",
]


@api_v3_router.get("/workflow/{workflow_id}/template_timeline")
async def get_template_timeline(
    workflow_id: int,
    db: Session = Depends(get_db),
) -> TemplateTimelineResponse:
    """Get template execution timeline for a workflow.

    Returns per-template event-driven time series: at every
    status-transition timestamp the exact task count per status
    category is recorded.  No bucketing — the frontend renders
    a continuous stacked area chart from the raw events.
    """
    logger.info("Getting template timeline", workflow_id=workflow_id)

    # Fetch every status transition, ordered chronologically.
    records_query = (
        select(
            TaskStatusAudit.task_id,
            TaskStatusAudit.new_status,
            TaskStatusAudit.entered_at,
            TaskTemplate.name.label("template_name"),
        )
        .join(Task, Task.id == TaskStatusAudit.task_id)
        .join(Node, Node.id == Task.node_id)
        .join(
            TaskTemplateVersion,
            TaskTemplateVersion.id == Node.task_template_version_id,
        )
        .join(
            TaskTemplate,
            TaskTemplate.id == TaskTemplateVersion.task_template_id,
        )
        .where(TaskStatusAudit.workflow_id == workflow_id)
        .order_by(TaskStatusAudit.entered_at)
    )
    records = db.execute(records_query).all()

    if not records:
        return TemplateTimelineResponse(templates=[])

    # Group records by template
    tmpl_events: Dict[str, list] = {}
    tmpl_task_ids: Dict[str, set] = {}
    for task_id, status, entered_at, template_name in records:
        category = _categorize_status(status)
        if category is None:
            continue
        tmpl_events.setdefault(template_name, []).append(
            (task_id, category, entered_at)
        )
        tmpl_task_ids.setdefault(template_name, set()).add(task_id)

    # Build per-template event series
    rows: List[TemplateTimelineRow] = []
    first_exec_ts: Dict[str, datetime] = {}

    for tname, events in tmpl_events.items():
        # events already sorted by entered_at (from ORDER BY)
        task_status: Dict[int, str] = {}
        counts: Dict[str, int] = {c: 0 for c in TIMELINE_CATEGORIES}
        timestamps: List[str] = []
        series: Dict[str, List[int]] = {c: [] for c in TIMELINE_CATEGORIES}
        exec_started = False

        i = 0
        while i < len(events):
            ts = events[i][2]
            # Apply all transitions at this timestamp
            while i < len(events) and events[i][2] == ts:
                tid, cat, _ = events[i]
                old = task_status.get(tid)
                # If a task goes ERROR -> REGISTERED it's a retry;
                # keep showing it as ERROR until it truly progresses.
                if old == "ERROR" and cat == "REGISTERED":
                    cat = "ERROR"
                if old is not None:
                    counts[old] -= 1
                task_status[tid] = cat
                counts[cat] += 1
                if not exec_started and cat != "REGISTERED":
                    exec_started = True
                    first_exec_ts[tname] = ts
                i += 1

            # Only emit data points once execution has started
            if exec_started:
                timestamps.append(ts.isoformat())
                for c in TIMELINE_CATEGORIES:
                    series[c].append(counts[c])

        if not timestamps:
            continue

        # Extend each template by 1 second so the final state
        # (typically all-DONE) has visible width in the step chart.
        last_ts = events[-1][2]
        end_ts = (last_ts + timedelta(seconds=1)).isoformat()
        timestamps.append(end_ts)
        for c in TIMELINE_CATEGORIES:
            series[c].append(series[c][-1])

        rows.append(
            TemplateTimelineRow(
                template_name=tname,
                total_tasks=len(tmpl_task_ids.get(tname, set())),
                timestamps=timestamps,
                series=series,
            )
        )

    rows.sort(key=lambda r: first_exec_ts.get(r.template_name, datetime.max))

    logger.info(
        "Template timeline generated",
        workflow_id=workflow_id,
        num_templates=len(rows),
    )

    return TemplateTimelineResponse(templates=rows)

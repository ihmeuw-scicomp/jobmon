import json
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd  # type: ignore
import scipy.stats as st  # type: ignore
import structlog
from sqlalchemy import Float, String, and_, case, exists, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement, Label

from jobmon.server.web.error_log_clustering import cluster_error_logs
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.schemas.task_template import (
    CoreInfoItem,
    ErrorLogItem,
    ErrorLogResponse,
    FatalErrorBreakdownResponse,
    MostPopularQueueResponse,
    QueueInfoItem,
    RequestedCoresResponse,
    ResourceClusterItem,
    ResourceEfficiencyMetrics,
    TaskResourceDetailItem,
    TaskResourceVizItem,
    TaskTemplateDetailsResponse,
    TaskTemplateResourceAggregatesResponse,
    TaskTemplateResourceUsageRequest,
    TaskTemplateVersionItem,
    TaskTemplateVersionResponse,
    WorkflowTaskTemplateStatusItem,
)

logger = structlog.get_logger(__name__)


def _js_round_tenth(x: float) -> float:
    """Round ``x`` to one decimal using JS ``Math.round`` semantics.

    JS rounds half up toward +∞ (``Math.round(0.5) === 1``), while
    Python's ``round`` uses banker's rounding (``round(0.5) == 0``).
    Cluster boundaries must align with the frontend's rounding so a
    task-instance row the UI classifies as cluster A is counted as
    cluster A on the server too. ``requested_resources`` values are
    always non-negative here, so ``floor(x + 0.5)`` suffices.
    """
    return math.floor(x * 10 + 0.5) / 10


@dataclass
class ResourceUsageStatistics:
    """Clean data class for resource usage statistics."""

    num_tasks: Optional[int] = None
    min_mem: Optional[int] = None
    max_mem: Optional[int] = None
    mean_mem: Optional[float] = None
    min_runtime: Optional[int] = None
    max_runtime: Optional[int] = None
    mean_runtime: Optional[float] = None
    median_mem: Optional[float] = None
    median_runtime: Optional[float] = None
    ci_mem: Optional[List[Union[float, None]]] = None
    ci_runtime: Optional[List[Union[float, None]]] = None
    viz_data: Optional[List[TaskResourceVizItem]] = None


class TaskTemplateRepository:
    def __init__(self, session: Session) -> None:
        """Initialize the TaskTemplateRepository with a database session."""
        self.session = session

    def _convert_to_task_resource_detail_item(
        self, row_data: Dict[str, Any]
    ) -> Optional[TaskResourceDetailItem]:
        """Convert raw database row to TaskResourceDetailItem, with error handling."""
        try:
            return TaskResourceDetailItem(
                wallclock=(
                    float(row_data["r_orig"])
                    if row_data["r_orig"] is not None
                    else None
                ),
                maxrss=(
                    int(row_data["m_orig"]) if row_data["m_orig"] is not None else None
                ),
                node_id=row_data["node_id"],
                task_id=row_data["task_id"],
                task_instance_id=row_data.get("task_instance_id"),
                task_name=row_data.get("task_name"),
                requested_resources=row_data["requested_resources"],
                attempt_number_of_instance=row_data.get("attempt_number_of_instance"),
                status=row_data.get("status_orig"),
                task_status_date=row_data.get("task_status_date"),
                task_command=row_data.get("task_command"),
                task_num_attempts=row_data.get("task_num_attempts"),
                task_max_attempts=row_data.get("task_max_attempts"),
                workflow_run_id=row_data.get("workflow_run_id"),
            )
        except Exception as e:
            logger.error(
                f"Error parsing data for task_id {row_data.get('task_id', 'unknown')}: {e}."
            )
            return None

    def get_task_resource_details(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Optional[Dict[str, List[Any]]],
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[TaskResourceDetailItem]:
        """Fetch and filter task resource details with optimized single-query approach.

        Uses LEFT JOINs on TaskInstance and TaskResources so that every
        task appears in the result set regardless of instance state:
        - Tasks with terminal instances → full resource data
        - Tasks with running/launched instances → instance row, null resources
        - Tasks with no instances yet (registered) → single row, null instance fields
        Status falls back to Task.status when no instance exists.

        Pagination: when ``limit`` is provided, rows are ordered by
        ``(Task.id, TaskInstance.id)`` for deterministic paging and the
        supplied ``offset`` / ``limit`` are applied.
        """
        base_filters = [
            TaskTemplateVersion.id == task_template_version_id,
        ]

        if workflows:
            base_filters.append(Task.workflow_id.in_(workflows))

        # COALESCE gives instance status when present, task status otherwise
        status_col = func.coalesce(TaskInstance.status, Task.status).label("status_col")

        attempt_number_col = (
            func.row_number()
            .over(partition_by=Task.id, order_by=TaskInstance.id)
            .label("attempt_number_of_instance")
        )

        # Narrow ``requested_resources`` to the two fields the UI actually
        # uses on the viz path (memory + runtime). The full blob averages
        # ~700 bytes/row on templates like ``pixel`` (paths, project
        # config, stderr/stdout paths, command args) and dominated the
        # per-row payload size. MySQL ``JSON_OBJECT`` builds the narrow
        # JSON server-side so we ship ~30 bytes/row instead.
        narrow_req_res = func.json_object(
            "memory",
            func.json_extract(TaskResources.requested_resources, "$.memory"),
            "runtime",
            func.json_extract(TaskResources.requested_resources, "$.runtime"),
        ).label("requested_resources_col")

        if not node_args:
            # No node_args filtering - use optimized single query
            query = (
                select(
                    # TaskInstance resource usage fields
                    TaskInstance.wallclock,
                    TaskInstance.maxrss,
                    status_col,
                    # Node and Task identifiers
                    Node.id.label("node_id_col"),
                    Task.id.label("task_id_col"),
                    Task.name.label("task_name_col"),
                    # Task metadata fields
                    Task.status_date.label("task_status_date_col"),
                    Task.command.label("task_command_col"),
                    Task.num_attempts.label("task_num_attempts_col"),
                    Task.max_attempts.label("task_max_attempts_col"),
                    # Requested resources (narrowed to memory+runtime only)
                    narrow_req_res,
                    attempt_number_col,
                    TaskInstance.id.label("task_instance_id_col"),
                    TaskInstance.workflow_run_id.label("workflow_run_id_col"),
                )
                .select_from(TaskTemplateVersion)
                .join(
                    Node,
                    TaskTemplateVersion.id == Node.task_template_version_id,
                )
                .join(Task, Node.id == Task.node_id)
                .outerjoin(TaskInstance, Task.id == TaskInstance.task_id)
                .outerjoin(
                    TaskResources,
                    TaskInstance.task_resources_id == TaskResources.id,
                )
                .where(and_(*base_filters))
            )

            if limit is not None:
                query = (
                    query.order_by(Task.id, TaskInstance.id).offset(offset).limit(limit)
                )

            rows = self.session.execute(query).all()

            result = []
            for row in rows:
                # Map query results to row_data dictionary
                # Column order matches select() statement above
                row_data = {
                    "r_orig": row[0],  # wallclock
                    "m_orig": row[1],  # maxrss
                    "node_id": row[3],  # node_id_col
                    "task_id": row[4],  # task_id_col
                    "task_name": row[5],  # task_name_col
                    "requested_resources": row[10],  # requested_resources_col
                    "attempt_number_of_instance": row[11],  # attempt_number_col
                    "status_orig": row[2],  # status_col (coalesced)
                    "task_status_date": row[6],  # task_status_date_col
                    "task_command": row[7],  # task_command_col
                    "task_num_attempts": row[8],  # task_num_attempts_col
                    "task_max_attempts": row[9],  # task_max_attempts_col
                    "task_instance_id": row[12],  # task_instance_id_col
                    "workflow_run_id": row[13],  # workflow_run_id_col
                }
                item = self._convert_to_task_resource_detail_item(row_data)
                if item:
                    result.append(item)

            return result

        else:
            # node_args filtering - use optimized join approach
            return self._get_task_resource_details_with_node_args(
                task_template_version_id,
                workflows,
                node_args,
                base_filters,
                status_col,
                attempt_number_col,
                limit=limit,
                offset=offset,
            )

    def _get_task_resource_details_with_node_args(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Dict[str, List[Any]],
        base_filters: List[ColumnElement],
        status_col: Label,
        attempt_number_col: Label,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[TaskResourceDetailItem]:
        """Optimized node_args filtering using database-level joins and filtering."""
        # See ``get_task_resource_details`` for rationale — ship only the
        # two requested-resource fields the UI needs on the viz path.
        narrow_req_res = func.json_object(
            "memory",
            func.json_extract(TaskResources.requested_resources, "$.memory"),
            "runtime",
            func.json_extract(TaskResources.requested_resources, "$.runtime"),
        ).label("requested_resources")
        base_query = (
            select(
                TaskInstance.wallclock,
                TaskInstance.maxrss,
                Node.id.label("node_id"),
                Task.id.label("task_id"),
                Task.name.label("task_name"),
                narrow_req_res,
                attempt_number_col,
                status_col,
                Task.status_date.label("task_status_date"),
                Task.command.label("task_command"),
                Task.num_attempts.label("task_num_attempts"),
                Task.max_attempts.label("task_max_attempts"),
                TaskInstance.id.label("task_instance_id"),
                TaskInstance.workflow_run_id,
            )
            .select_from(TaskTemplateVersion)
            .join(
                Node,
                TaskTemplateVersion.id == Node.task_template_version_id,
            )
            .join(Task, Node.id == Task.node_id)
            .outerjoin(TaskInstance, Task.id == TaskInstance.task_id)
            .outerjoin(
                TaskResources,
                TaskInstance.task_resources_id == TaskResources.id,
            )
            .where(and_(*base_filters))
        ).cte("base_tasks")

        # For each node_arg filter, create a subquery that validates the constraint
        node_arg_subqueries = []
        for arg_name, arg_values in node_args.items():
            str_values = [str(v) for v in arg_values]

            # Create subquery to find nodes that have this arg with any of the specified values
            subquery = (
                select(NodeArg.node_id)
                .join(Arg, NodeArg.arg_id == Arg.id)
                .where(
                    and_(
                        Arg.name == arg_name,
                        func.cast(NodeArg.val, String).in_(str_values),
                    )
                )
            )
            node_arg_subqueries.append(subquery)

        # Build the final query that joins base tasks with node arg constraints
        final_query = select(
            base_query.c.wallclock,
            base_query.c.maxrss,
            base_query.c.node_id,
            base_query.c.task_id,
            base_query.c.task_name,
            base_query.c.requested_resources,
            base_query.c.attempt_number_of_instance,
            base_query.c.status_col,
            base_query.c.task_status_date,
            base_query.c.task_command,
            base_query.c.task_num_attempts,
            base_query.c.task_max_attempts,
            base_query.c.task_instance_id,
            base_query.c.workflow_run_id,
        ).select_from(base_query)

        for subquery in node_arg_subqueries:
            final_query = final_query.where(base_query.c.node_id.in_(subquery))

        if limit is not None:
            final_query = (
                final_query.order_by(
                    base_query.c.task_id, base_query.c.task_instance_id
                )
                .offset(offset)
                .limit(limit)
            )

        rows = self.session.execute(final_query).all()

        result = []
        for row in rows:
            # Map query results to row_data dictionary
            # Column order matches final_query select() statement above
            row_data = {
                "r_orig": row[0],  # wallclock
                "m_orig": row[1],  # maxrss
                "node_id": row[2],  # node_id
                "task_id": row[3],  # task_id
                "task_name": row[4],  # task_name
                "requested_resources": row[5],  # requested_resources
                "attempt_number_of_instance": row[6],  # attempt_number_of_instance
                "status_orig": row[7],  # status
                "task_status_date": row[8],  # task_status_date
                "task_command": row[9],  # task_command
                "task_num_attempts": row[10],  # task_num_attempts
                "task_max_attempts": row[11],  # task_max_attempts
                "task_instance_id": row[12],  # task_instance_id
                "workflow_run_id": row[13],  # workflow_run_id
            }
            item = self._convert_to_task_resource_detail_item(row_data)
            if item:
                result.append(item)

        return result

    def calculate_resource_statistics(
        self,
        task_details: List[TaskResourceDetailItem],
        confidence_interval: Optional[str] = None,
        task_template_version_id: Optional[int] = None,
    ) -> ResourceUsageStatistics:
        """Calculate statistics from task details using scipy.stats."""
        if not task_details:
            # Distinguish between "no tasks exist" vs "no tasks match filter"
            if task_template_version_id is not None:
                # Check if this task template version has ANY tasks at all
                has_any_tasks_query = (
                    select(Task.id)
                    .select_from(TaskTemplateVersion)
                    .join(Node, TaskTemplateVersion.id == Node.task_template_version_id)
                    .join(Task, Node.id == Task.node_id)
                    .where(TaskTemplateVersion.id == task_template_version_id)
                    .limit(1)
                )
                has_any_tasks = (
                    self.session.execute(has_any_tasks_query).first() is not None
                )

                if has_any_tasks:
                    # Task template has tasks, but none match the filter
                    num_tasks_value = 0
                else:
                    # Task template has never been used
                    num_tasks_value = None
            else:
                # Fallback to None if no task_template_version_id provided
                num_tasks_value = None

            return ResourceUsageStatistics(
                num_tasks=num_tasks_value,
                min_mem=None,
                max_mem=None,
                mean_mem=None,
                min_runtime=None,
                max_runtime=None,
                mean_runtime=None,
                median_mem=None,
                median_runtime=None,
                ci_mem=None,
                ci_runtime=None,
            )

        # Extract numeric data with business logic to handle edge cases (matching V2 behavior)
        runtimes = [
            float(item.r) for item in task_details if item.r is not None and item.r != 0
        ]

        # Calculate statistics separately for each data type
        stats = ResourceUsageStatistics(num_tasks=len(task_details))

        # Handle memory data: distinguish between "no data" vs "invalid data"
        has_any_memory_data = any(item.m is not None for item in task_details)
        if has_any_memory_data:
            # We have memory data - clamp negative values to 0, then filter zeros for stats
            memories_raw = [
                max(0.0, float(item.m)) for item in task_details if item.m is not None
            ]
            memories_for_stats = [
                m for m in memories_raw if m != 0
            ]  # Filter out zeros for statistical calculations

            # Calculate memory statistics
            if memories_for_stats:
                stats.min_mem = int(min(memories_for_stats))
                stats.max_mem = int(max(memories_for_stats))
                stats.mean_mem = float(np.mean(memories_for_stats))
                stats.median_mem = float(np.median(memories_for_stats))
            else:
                # Had memory data but all were <= 0 (clamped to 0)
                stats.min_mem = 0
                stats.max_mem = 0
                stats.mean_mem = 0.0
                stats.median_mem = 0.0
        # else: No memory data at all - leave as None (default in ResourceUsageStatistics)

        # Calculate runtime statistics (default to 0 if no data, matching V2 behavior)
        if runtimes:
            stats.min_runtime = int(min(runtimes))
            stats.max_runtime = int(max(runtimes))
            stats.mean_runtime = float(np.mean(runtimes))
            stats.median_runtime = float(np.median(runtimes))
        else:
            # Default to 0 when no runtime data (matching V2 behavior)
            stats.min_runtime = 0
            stats.max_runtime = 0
            stats.mean_runtime = 0.0
            stats.median_runtime = 0.0

        # Calculate confidence intervals if requested
        if confidence_interval:
            ci_value = float(confidence_interval)

            # Memory confidence interval (only if we have enough memory data)
            if has_any_memory_data and len(memories_for_stats) > 1:
                mem_mean = np.mean(memories_for_stats)
                mem_std = np.std(memories_for_stats, ddof=1)
                mem_ci = st.t.interval(
                    ci_value,
                    len(memories_for_stats) - 1,
                    loc=mem_mean,
                    scale=mem_std / np.sqrt(len(memories_for_stats)),
                )
                stats.ci_mem = [round(float(mem_ci[0]), 2), round(float(mem_ci[1]), 2)]

            # Runtime confidence interval (only if we have enough runtime data)
            if len(runtimes) > 1:
                runtime_mean = np.mean(runtimes)
                runtime_std = np.std(runtimes, ddof=1)
                runtime_ci = st.t.interval(
                    ci_value,
                    len(runtimes) - 1,
                    loc=runtime_mean,
                    scale=runtime_std / np.sqrt(len(runtimes)),
                )
                stats.ci_runtime = [
                    round(float(runtime_ci[0]), 2),
                    round(float(runtime_ci[1]), 2),
                ]

        return stats

    def count_task_resource_details(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Optional[Dict[str, List[Any]]],
    ) -> int:
        """Count rows that get_task_resource_details would return.

        Used by the paginated endpoint to report ``total_count`` without
        materializing the full detail payload.
        """
        base_filters = [TaskTemplateVersion.id == task_template_version_id]
        if workflows:
            base_filters.append(Task.workflow_id.in_(workflows))

        count_query = (
            select(func.count())
            .select_from(TaskTemplateVersion)
            .join(Node, TaskTemplateVersion.id == Node.task_template_version_id)
            .join(Task, Node.id == Task.node_id)
            .outerjoin(TaskInstance, Task.id == TaskInstance.task_id)
            .where(and_(*base_filters))
        )

        if node_args:
            for arg_name, arg_values in node_args.items():
                str_values = [str(v) for v in arg_values]
                subquery = (
                    select(NodeArg.node_id)
                    .join(Arg, NodeArg.arg_id == Arg.id)
                    .where(
                        and_(
                            Arg.name == arg_name,
                            func.cast(NodeArg.val, String).in_(str_values),
                        )
                    )
                )
                count_query = count_query.where(Node.id.in_(subquery))

        return int(self.session.execute(count_query).scalar_one())

    def get_task_resource_aggregates(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Optional[Dict[str, List[Any]]] = None,
        latest_only: bool = True,
    ) -> TaskTemplateResourceAggregatesResponse:
        """Compute compact aggregates for a task template.

        Aggregation runs entirely in MySQL: a single main query returns
        one row of stats + efficiency totals + outlier count, and a
        second query groups by ``task_resources_id`` for cluster
        breakdown. Avoids the previous 78K-row ship-to-Python pattern
        which was bottlenecked on row serialization (~10s for pixel).

        Median / p95 for wallclock and maxrss are not computed
        server-side — percentiles don't compose across GROUP BYs and
        running separate ``ORDER BY LIMIT OFFSET`` scans per column
        erodes the win. The frontend computes these client-side from
        the streaming viz rows instead, falling back to ``undefined``
        on the initial paint.

        When ``latest_only`` is True (the UI default), only the latest
        TaskInstance per Task is included, so the numbers match the
        latest-only scatter / table view.
        """
        inner = self._build_aggregates_rowset(
            task_template_version_id, workflows, node_args, latest_only
        )

        stats = (
            self.session.execute(self._main_aggregates_query(inner.subquery("t")))
            .mappings()
            .one_or_none()
        )
        if stats is None or (stats["num_tasks"] or 0) == 0:
            has_any = (
                self.session.execute(
                    select(Task.id)
                    .select_from(TaskTemplateVersion)
                    .join(Node, TaskTemplateVersion.id == Node.task_template_version_id)
                    .join(Task, Node.id == Task.node_id)
                    .where(TaskTemplateVersion.id == task_template_version_id)
                    .limit(1)
                ).first()
                is not None
            )
            return TaskTemplateResourceAggregatesResponse(
                num_tasks=0 if has_any else None
            )

        cluster_counts = self.session.execute(
            self._cluster_groupby_query(
                self._build_aggregates_rowset(
                    task_template_version_id, workflows, node_args, latest_only
                ).subquery("t_clust")
            )
        ).all()
        cluster_resources = self._fetch_cluster_resources(
            [int(r.tr_id) for r in cluster_counts if r.tr_id is not None]
        )

        return self._build_aggregates_response_from_sql(
            stats, cluster_counts, cluster_resources
        )

    def _build_aggregates_rowset(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Optional[Dict[str, List[Any]]],
        latest_only: bool,
    ) -> Any:
        """Build the scoped inner SELECT for the aggregate queries.

        This is the per-task_instance rowset the main aggregate and the
        cluster-GROUP BY queries both project onto. Values are CAST
        here so outer queries see typed numeric columns (``wallclock``
        and ``maxrss`` are VARCHAR(50) in the schema).
        """
        wc_num = func.cast(TaskInstance.wallclock, Float).label("wc")
        mr_num = func.cast(TaskInstance.maxrss, Float).label("mr")
        req_rt_num = func.cast(
            func.json_extract(TaskResources.requested_resources, "$.runtime"),
            Float,
        ).label("req_rt")
        req_mem_num = func.cast(
            func.json_extract(TaskResources.requested_resources, "$.memory"),
            Float,
        ).label("req_mem")

        base_filters: List[ColumnElement] = [
            TaskTemplateVersion.id == task_template_version_id,
        ]
        if workflows:
            base_filters.append(Task.workflow_id.in_(workflows))

        q = (
            select(
                wc_num,
                mr_num,
                req_rt_num,
                req_mem_num,
                TaskInstance.task_resources_id.label("tr_id"),
            )
            .select_from(TaskTemplateVersion)
            .join(Node, TaskTemplateVersion.id == Node.task_template_version_id)
            .join(Task, Node.id == Task.node_id)
            .join(TaskInstance, Task.id == TaskInstance.task_id)
            .outerjoin(
                TaskResources,
                TaskInstance.task_resources_id == TaskResources.id,
            )
            .where(and_(*base_filters))
        )

        if latest_only:
            latest_ti_id = (
                select(func.max(TaskInstance.id))
                .where(TaskInstance.task_id == Task.id)
                .correlate(Task)
                .scalar_subquery()
            )
            q = q.where(TaskInstance.id == latest_ti_id)

        if node_args:
            for arg_name, arg_values in node_args.items():
                str_values = [str(v) for v in arg_values]
                subquery = (
                    select(NodeArg.node_id)
                    .join(Arg, NodeArg.arg_id == Arg.id)
                    .where(
                        and_(
                            Arg.name == arg_name,
                            func.cast(NodeArg.val, String).in_(str_values),
                        )
                    )
                )
                q = q.where(Node.id.in_(subquery))

        return q

    @staticmethod
    def _main_aggregates_query(t: Any) -> Any:
        """Single-row aggregate: KPIs + efficiency totals + outlier count.

        Outlier gate requires the mean and stddev of wallclock and
        (maxrss/GiB) over the efficiency subset. MySQL 8 window
        aggregates let us compute those alongside row filtering in one
        scan — no need for a second pass.
        """
        GIB = 1024.0**3
        wc = t.c.wc
        mr = t.c.mr
        req_rt = t.c.req_rt
        req_mem = t.c.req_mem
        eff = and_(wc > 0, mr > 0, req_rt > 0, req_mem > 0)
        wc_eff = case((eff, wc), else_=None)
        mem_gib_eff = case((eff, mr / GIB), else_=None)

        # Ranked subquery to compute window aggregates (mean/stddev of
        # efficiency-gated wc and mem_gib) in the same scan. Main outer
        # query then counts outliers using those window values.
        ranked = (
            select(
                wc.label("wc"),
                mr.label("mr"),
                req_rt.label("req_rt"),
                req_mem.label("req_mem"),
                func.avg(wc_eff).over().label("mean_eff_wc"),
                func.coalesce(func.stddev_pop(wc_eff).over(), 0.0).label("std_eff_wc"),
                func.avg(mem_gib_eff).over().label("mean_eff_mem"),
                func.coalesce(func.stddev_pop(mem_gib_eff).over(), 0.0).label(
                    "std_eff_mem"
                ),
            )
            .select_from(t)
            .subquery("r")
        )

        r_wc = ranked.c.wc
        r_mr = ranked.c.mr
        r_rrt = ranked.c.req_rt
        r_rmem = ranked.c.req_mem
        r_eff = and_(r_wc > 0, r_mr > 0, r_rrt > 0, r_rmem > 0)
        r_mem_gib = r_mr / GIB
        r_mem_util = case((r_eff, r_mem_gib / r_rmem * 100), else_=None)
        r_rt_util = case((r_eff, r_wc / r_rrt * 100), else_=None)
        outlier_gate = or_(
            func.abs(r_wc - ranked.c.mean_eff_wc) > 2 * ranked.c.std_eff_wc,
            func.abs(r_mem_gib - ranked.c.mean_eff_mem) > 2 * ranked.c.std_eff_mem,
        )

        return select(
            func.count().label("num_tasks"),
            # memory stats
            func.count(case((r_mr > 0, 1), else_=None)).label("n_mr"),
            func.min(case((r_mr > 0, r_mr), else_=None)).label("min_mem"),
            func.max(case((r_mr > 0, r_mr), else_=None)).label("max_mem"),
            func.avg(case((r_mr > 0, r_mr), else_=None)).label("mean_mem"),
            # runtime stats
            func.count(case((r_wc > 0, 1), else_=None)).label("n_wc"),
            func.min(case((r_wc > 0, r_wc), else_=None)).label("min_rt"),
            func.max(case((r_wc > 0, r_wc), else_=None)).label("max_rt"),
            func.avg(case((r_wc > 0, r_wc), else_=None)).label("mean_rt"),
            # requested stats (mean is cheap and matches UI's median
            # expectation closely for uniform-resource templates)
            func.avg(case((r_rrt > 0, r_rrt), else_=None)).label("mean_req_rt"),
            func.avg(case((r_rmem > 0, r_rmem), else_=None)).label("mean_req_mem"),
            # efficiency totals
            func.sum(case((r_eff, 1), else_=0)).label("eff_n"),
            func.sum(case((r_eff, r_mem_gib), else_=0)).label("tot_actual_mem"),
            func.sum(case((r_eff, r_rmem), else_=0)).label("tot_req_mem"),
            func.sum(case((r_eff, r_wc), else_=0)).label("tot_actual_rt"),
            func.sum(case((r_eff, r_rrt), else_=0)).label("tot_req_rt"),
            func.sum(case((and_(r_eff, r_mem_util < 50), 1), else_=0)).label(
                "over_mem"
            ),
            func.sum(case((and_(r_eff, r_mem_util > 90), 1), else_=0)).label(
                "under_mem"
            ),
            func.sum(case((and_(r_eff, r_rt_util < 50), 1), else_=0)).label("over_rt"),
            func.sum(case((and_(r_eff, r_rt_util > 90), 1), else_=0)).label("under_rt"),
            func.sum(case((and_(r_eff, outlier_gate), 1), else_=0)).label("outliers"),
        ).select_from(ranked)

    @staticmethod
    def _cluster_groupby_query(t: Any) -> Any:
        """Count rows per task_resources_id, skipping NULLs.

        Represents one cluster per unique task_resources row; the
        ``requested_resources`` JSON fields are fetched separately to
        avoid JSON_EXTRACT per row.
        """
        return (
            select(
                t.c.tr_id.label("tr_id"),
                func.count().label("n"),
            )
            .select_from(t)
            .where(t.c.tr_id.is_not(None))
            .group_by(t.c.tr_id)
        )

    def _fetch_cluster_resources(self, tr_ids: List[int]) -> Dict[int, tuple]:
        """Fetch (runtime, memory) per unique task_resources_id.

        One query returning ``len(tr_ids)`` rows — typically a handful
        even for workflows with tens of thousands of task_instances.
        """
        if not tr_ids:
            return {}
        rows = self.session.execute(
            select(
                TaskResources.id,
                func.cast(
                    func.json_extract(TaskResources.requested_resources, "$.runtime"),
                    Float,
                ),
                func.cast(
                    func.json_extract(TaskResources.requested_resources, "$.memory"),
                    Float,
                ),
            ).where(TaskResources.id.in_(tr_ids))
        ).all()
        return {
            int(r[0]): (
                float(r[1]) if r[1] is not None else None,
                float(r[2]) if r[2] is not None else None,
            )
            for r in rows
        }

    def _build_aggregates_response_from_sql(
        self,
        stats: Any,
        cluster_counts: Any,
        cluster_resources: Dict[int, tuple],
    ) -> TaskTemplateResourceAggregatesResponse:
        """Assemble the response from the two SQL aggregate results.

        ``stats``: single-row mapping of KPIs + efficiency totals.
        ``cluster_counts``: one row per task_resources_id with count.
        ``cluster_resources``: tr_id → (runtime, memory) lookup.

        Percentiles are left as ``None`` on the response — the frontend
        fills them in from streaming viz rows so the aggregate endpoint
        can complete without a second full scan.
        """
        num_tasks = int(stats["num_tasks"])
        resp = TaskTemplateResourceAggregatesResponse(num_tasks=num_tasks)

        if (stats["n_mr"] or 0) > 0:
            resp.min_mem = int(stats["min_mem"])
            resp.max_mem = int(stats["max_mem"])
            resp.mean_mem = float(stats["mean_mem"])
        if (stats["n_wc"] or 0) > 0:
            resp.min_runtime = int(stats["min_rt"])
            resp.max_runtime = int(stats["max_rt"])
            resp.mean_runtime = float(stats["mean_rt"])

        if stats["mean_req_rt"] is not None:
            resp.median_requested_runtime = float(stats["mean_req_rt"])
        if stats["mean_req_mem"] is not None:
            resp.median_requested_memory = float(stats["mean_req_mem"])

        # Build clusters by binning unique (runtime, memory) pairs using
        # the same JS-round formula as the frontend's cluster-key builder.
        # Merging multiple task_resources rows that share the same bin
        # avoids duplicate chips when only ancillary fields differ.
        merged: Dict[tuple, Dict[str, Any]] = {}
        for row in cluster_counts:
            tr_id = int(row.tr_id)
            rt_mem = cluster_resources.get(tr_id)
            if rt_mem is None:
                continue
            runtime, memory = rt_mem
            if runtime is None or runtime <= 0 or memory is None or memory <= 0:
                continue
            key = (_js_round_tenth(memory), _js_round_tenth(runtime / 3600))
            bucket = merged.setdefault(
                key,
                {"runtime": runtime, "memory": memory, "count": 0},
            )
            bucket["count"] += int(row.n)
        resp.resource_clusters = sorted(
            [
                ResourceClusterItem(
                    runtime=b["runtime"],
                    memory=b["memory"],
                    task_count=b["count"],
                )
                for b in merged.values()
            ],
            key=lambda c: c.task_count,
            reverse=True,
        )

        eff_n = int(stats["eff_n"] or 0)
        if eff_n > 0:
            tot_actual_mem = float(stats["tot_actual_mem"] or 0.0)
            tot_req_mem = float(stats["tot_req_mem"] or 0.0)
            tot_actual_rt = float(stats["tot_actual_rt"] or 0.0)
            tot_req_rt = float(stats["tot_req_rt"] or 0.0)
            resp.efficiency = ResourceEfficiencyMetrics(
                memory_utilization=(
                    (tot_actual_mem / tot_req_mem * 100) if tot_req_mem > 0 else 0.0
                ),
                runtime_utilization=(
                    (tot_actual_rt / tot_req_rt * 100) if tot_req_rt > 0 else 0.0
                ),
                over_allocated_memory=round(int(stats["over_mem"] or 0) / eff_n * 100),
                under_allocated_memory=round(
                    int(stats["under_mem"] or 0) / eff_n * 100
                ),
                over_allocated_runtime=round(int(stats["over_rt"] or 0) / eff_n * 100),
                under_allocated_runtime=round(
                    int(stats["under_rt"] or 0) / eff_n * 100
                ),
                outlier_count=int(stats["outliers"] or 0),
            )

        return resp

    def get_task_template_resource_usage(
        self, req: TaskTemplateResourceUsageRequest
    ) -> Optional[List[TaskResourceVizItem]]:
        task_details_list: List[TaskResourceDetailItem] = (
            self.get_task_resource_details(
                task_template_version_id=req.task_template_version_id,
                workflows=req.workflows,
                node_args=req.node_args,
            )
        )

        viz_data: Optional[List[TaskResourceVizItem]] = None
        if req.viz:
            viz_data = []
            for detail_item in task_details_list:
                viz_data.append(
                    TaskResourceVizItem(
                        r=detail_item.r,
                        m=detail_item.m,
                        node_id=detail_item.node_id,
                        task_id=detail_item.task_id,
                        task_instance_id=detail_item.task_instance_id,
                        task_name=detail_item.task_name,
                        requested_resources=detail_item.requested_resources,
                        attempt_number_of_instance=detail_item.attempt_number_of_instance,
                        status=detail_item.status,
                        task_status_date=detail_item.task_status_date,
                        task_command=detail_item.task_command,
                        task_num_attempts=detail_item.task_num_attempts,
                        task_max_attempts=detail_item.task_max_attempts,
                    )
                )

        return viz_data

    def _find_ttvid(self, workflow_id: int, task_template_id: int) -> Optional[int]:
        """Find task template version ID for a workflow + task template.

        Two-step approach: fetch all TTVs for the template (small PK
        lookup), then EXISTS check against the workflow's tasks.
        """
        ttv_ids = [
            row[0]
            for row in self.session.execute(
                select(TaskTemplateVersion.id).where(
                    TaskTemplateVersion.task_template_id == task_template_id
                )
            ).all()
        ]
        if not ttv_ids:
            return None

        sql = (
            select(Node.task_template_version_id)
            .where(
                Node.task_template_version_id.in_(ttv_ids),
                exists(
                    select(Task.id).where(
                        Task.node_id == Node.id,
                        Task.workflow_id == workflow_id,
                    )
                ),
            )
            .limit(1)
        )
        row = self.session.execute(sql).one_or_none()
        return row[0] if row else None

    def get_task_template_details(
        self,
        workflow_id: int,
        task_template_id: int,
        task_template_version_id: Optional[int] = None,
    ) -> Optional[TaskTemplateDetailsResponse]:
        """Get task template details."""
        sql1 = select(
            TaskTemplate.name,
        ).where(TaskTemplate.id == task_template_id)

        row = self.session.execute(sql1).one_or_none()

        if row is None:
            return None
        else:
            tt_name = row.name

        if task_template_version_id is None:
            task_template_version_id = self._find_ttvid(workflow_id, task_template_id)
        if task_template_version_id is None:
            return None

        tt_details_data = TaskTemplateDetailsResponse(
            task_template_id=task_template_id,
            task_template_name=tt_name,
            task_template_version_id=task_template_version_id,
        )

        return tt_details_data

    def get_task_template_versions(
        self, task_id: Optional[int] = None, workflow_id: Optional[int] = None
    ) -> Optional[TaskTemplateVersionResponse]:
        """Get task template version IDs and names for a task or workflow.

        Args:
            task_id: Optional task ID to get task template version for
            workflow_id: Optional workflow ID to get all task template versions for

        Returns:
            TaskTemplateVersionResponse with list of task template versions,
            or None if no data found.

        Note:
            If both task_id and workflow_id are provided, task_id takes precedence.
        """
        if task_id:
            # Get task template version for specific task
            query_filter = [
                Task.id == task_id,
                Task.node_id == Node.id,
                Node.task_template_version_id == TaskTemplateVersion.id,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            ]
            sql = select(
                TaskTemplateVersion.id,
                TaskTemplate.name,
            ).where(*query_filter)
        elif workflow_id:
            # Get all task template versions for workflow
            query_filter = [
                Task.workflow_id == workflow_id,
                Task.node_id == Node.id,
                Node.task_template_version_id == TaskTemplateVersion.id,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            ]
            sql = (
                select(
                    TaskTemplateVersion.id,
                    TaskTemplate.name,
                ).where(*query_filter)
            ).distinct()
        else:
            # Neither task_id nor workflow_id provided
            return None

        rows = self.session.execute(sql).all()

        if not rows:
            return None

        # Convert rows to Pydantic models
        task_template_versions = []
        for row in rows:
            task_template_versions.append(
                TaskTemplateVersionItem(id=row.id, name=row.name)
            )

        return TaskTemplateVersionResponse(
            task_template_version_ids=task_template_versions
        )

    def get_requested_cores(
        self, task_template_version_ids: List[int]
    ) -> RequestedCoresResponse:
        """Get the min, max, and avg of requested cores for task template versions.

        Args:
            task_template_version_ids: List of task template version IDs

        Returns:
            RequestedCoresResponse with core information for each task template version
        """
        query_filter = [
            TaskTemplateVersion.id.in_(task_template_version_ids),
            TaskTemplateVersion.id == Node.task_template_version_id,
            Task.node_id == Node.id,
            Task.task_resources_id == TaskResources.id,
        ]

        sql = select(TaskTemplateVersion.id, TaskResources.requested_resources).where(
            *query_filter
        )

        rows_raw = self.session.execute(sql).all()

        core_info = []
        if rows_raw:
            result_dir: Dict[int, List[int]] = {}

            for row in rows_raw:
                ttv_id = row[0]
                requested_resources = row[1]

                # Parse the JSON string, replace single quotes with double quotes
                j_str = requested_resources.replace("'", '"')
                j_dir = json.loads(j_str)

                # Default to 1 core if num_cores not specified
                core = 1 if "num_cores" not in j_dir else int(j_dir["num_cores"])

                if ttv_id in result_dir:
                    result_dir[ttv_id].append(core)
                else:
                    result_dir[ttv_id] = [core]

            # Calculate statistics for each task template version
            for ttv_id, cores in result_dir.items():
                item_min = int(np.min(cores))
                item_max = int(np.max(cores))
                item_mean = round(np.mean(cores))

                core_info.append(
                    CoreInfoItem(id=ttv_id, min=item_min, max=item_max, avg=item_mean)
                )

        return RequestedCoresResponse(core_info=core_info)

    def get_most_popular_queue(
        self, task_template_version_ids: List[int]
    ) -> MostPopularQueueResponse:
        """Get the most popular queue for task template versions.

        Args:
            task_template_version_ids: List of task template version IDs

        Returns:
            MostPopularQueueResponse with queue information for each task template version
        """
        query_filter = [
            TaskTemplateVersion.id.in_(task_template_version_ids),
            TaskTemplateVersion.id == Node.task_template_version_id,
            Task.node_id == Node.id,
            TaskInstance.task_id == Task.id,
            TaskInstance.task_resources_id == TaskResources.id,
            TaskResources.queue_id.isnot(None),
        ]

        sql = select(TaskTemplateVersion.id, TaskResources.queue_id).where(
            *query_filter
        )
        rows_raw = self.session.execute(sql).all()

        queue_info = []
        if rows_raw:
            result_dir: Dict[int, Dict[int, int]] = {}

            # Count queue usage per task template version
            for row in rows_raw:
                ttv_id = row[0]
                queue_id = row[1]

                if ttv_id in result_dir:
                    if queue_id in result_dir[ttv_id]:
                        result_dir[ttv_id][queue_id] += 1
                    else:
                        result_dir[ttv_id][queue_id] = 1
                else:
                    result_dir[ttv_id] = {queue_id: 1}

            # Find most popular queue for each task template version
            for ttv_id, queue_counts in result_dir.items():
                popular_queue_id = max(
                    queue_counts.keys(), key=lambda q: queue_counts[q]
                )

                # Get queue name
                queue_name_query = select(Queue.name).where(
                    Queue.id == popular_queue_id
                )
                popular_queue_name = self.session.execute(queue_name_query).scalar_one()

                queue_info.append(
                    QueueInfoItem(
                        id=ttv_id, queue=popular_queue_name, queue_id=popular_queue_id
                    )
                )

        return MostPopularQueueResponse(queue_info=queue_info)

    def get_workflow_tt_status_viz(
        self,
        workflow_id: int,
        dialect: str = "sqlite",
    ) -> Dict[int, WorkflowTaskTemplateStatusItem]:
        """Get the status of workflow task templates for GUI visualization.

        Optimized version using single query with SQL aggregation instead of two separate
        queries and Python-level aggregation.

        Args:
            workflow_id: ID of the workflow
            dialect: Database dialect for optimization hints

        Returns:
            Dictionary mapping task template ID to WorkflowTaskTemplateStatusItem
        """

        def serialize_decimal(value: Union[Decimal, float]) -> float:
            """Convert Decimal to float for JSON serialization."""
            if isinstance(value, Decimal):
                return float(value)
            return value

        # Query for task template data with Array join (outer join for backward compatibility)
        sub_query = (
            select(
                Array.id,
                Array.task_template_version_id,
                Array.max_concurrently_running,
            ).where(Array.workflow_id == workflow_id)
        ).subquery()

        join_table = (
            Task.__table__.join(Node, Task.node_id == Node.id)
            .join(
                TaskTemplateVersion,
                Node.task_template_version_id == TaskTemplateVersion.id,
            )
            .join(
                TaskTemplate,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            )
            .join(
                sub_query,
                sub_query.c.task_template_version_id == TaskTemplateVersion.id,
                isouter=True,
            )
        )

        # Single optimized query with SQL aggregation.
        optimized_sql = (
            select(
                TaskTemplate.id.label("task_template_id"),
                TaskTemplate.name.label("task_template_name"),
                TaskTemplateVersion.id.label("task_template_version_id"),
                sub_query.c.max_concurrently_running,
                func.count(Task.id).label("total_tasks"),
                func.sum(case((Task.status == "G", 1), else_=0)).label("pending_count"),
                func.sum(
                    case(
                        (
                            Task.status.in_(
                                ["I", "Q", "K", "A", "H", "S", "U", "W", "T", "O"]
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("scheduled_count"),
                func.sum(case((Task.status == "R", 1), else_=0)).label("running_count"),
                func.sum(case((Task.status == "D", 1), else_=0)).label("done_count"),
                func.sum(case((Task.status == "F", 1), else_=0)).label("fatal_count"),
                func.min(Task.num_attempts).label("min_attempts"),
                func.max(Task.num_attempts).label("max_attempts"),
                func.avg(Task.num_attempts).label("avg_attempts"),
            )
            .select_from(join_table)
            .where(Task.workflow_id == workflow_id)
            .group_by(
                TaskTemplate.id,
                TaskTemplate.name,
                TaskTemplateVersion.id,
                sub_query.c.max_concurrently_running,
            )
            .order_by(TaskTemplate.id)
        )

        # Add STRAIGHT_JOIN for MySQL optimization
        if dialect == "mysql":
            optimized_sql = optimized_sql.prefix_with("STRAIGHT_JOIN")

        # Single database query instead of two - much faster
        rows = self.session.execute(optimized_sql).all()

        # Process results (much faster - no loops, direct aggregation from SQL)
        result_dict = {}
        for row in rows:
            result_dict[row.task_template_id] = WorkflowTaskTemplateStatusItem(
                id=row.task_template_id,
                name=row.task_template_name,
                tasks=row.total_tasks,
                PENDING=row.pending_count,
                SCHEDULED=row.scheduled_count,
                RUNNING=row.running_count,
                DONE=row.done_count,
                FATAL=row.fatal_count,
                MAXC=(
                    row.max_concurrently_running
                    if row.max_concurrently_running is not None
                    else "NA"
                ),
                num_attempts_min=(
                    serialize_decimal(row.min_attempts)
                    if row.min_attempts is not None
                    else None
                ),
                num_attempts_max=(
                    serialize_decimal(row.max_attempts)
                    if row.max_attempts is not None
                    else None
                ),
                num_attempts_avg=(
                    serialize_decimal(row.avg_attempts)
                    if row.avg_attempts is not None
                    else None
                ),
                task_template_version_id=row.task_template_version_id,
            )

        return result_dict

    def get_fatal_error_breakdown_for_tt(
        self,
        workflow_id: int,
        task_template_version_id: int,
        workflow_run_id: Optional[int] = None,
    ) -> FatalErrorBreakdownResponse:
        """Classify fatal tasks for one template by last TI status.

        Scoped to workflow + template for fast indexed lookup.
        """
        latest_ti_id_subq = (
            select(
                TaskInstance.task_id,
                func.max(TaskInstance.id).label("max_ti_id"),
            )
            .join(Task, TaskInstance.task_id == Task.id)
            .join(Node, Task.node_id == Node.id)
            .where(
                Task.workflow_id == workflow_id,
                Task.status == TaskStatus.ERROR_FATAL,
                Node.task_template_version_id == task_template_version_id,
            )
            .group_by(TaskInstance.task_id)
            .subquery()
        )

        sql = (
            select(
                TaskInstance.status.label("ti_status"),
                func.count(TaskInstance.id).label("cnt"),
            )
            .join(
                latest_ti_id_subq,
                TaskInstance.id == latest_ti_id_subq.c.max_ti_id,
            )
            .group_by(TaskInstance.status)
        )

        resource = 0
        app = 0
        infra = 0
        for row in self.session.execute(sql).all():
            if row.ti_status == TaskInstanceStatus.RESOURCE_ERROR:
                resource += row.cnt
            elif row.ti_status == TaskInstanceStatus.ERROR:
                app += row.cnt
            else:
                infra += row.cnt

        z_filters = [
            Task.workflow_id == workflow_id,
            Node.task_template_version_id == task_template_version_id,
            TaskInstance.status == TaskInstanceStatus.RESOURCE_ERROR,
        ]
        if workflow_run_id is not None:
            z_filters.append(TaskInstance.workflow_run_id == workflow_run_id)

        z_count_sql = (
            select(func.count(TaskInstance.id))
            .join(Task, TaskInstance.task_id == Task.id)
            .join(Node, Task.node_id == Node.id)
            .where(*z_filters)
        )
        resource_error_total = self.session.execute(z_count_sql).scalar() or 0

        retried_sql = (
            select(func.count(func.distinct(Task.id)))
            .join(TaskInstance, TaskInstance.task_id == Task.id)
            .join(Node, Task.node_id == Node.id)
            .where(*z_filters, Task.status != TaskStatus.ERROR_FATAL)
        )
        resource_error_retried = self.session.execute(retried_sql).scalar() or 0

        resource_error_ti_ids: List[int] = []
        if resource_error_total > 0:
            z_ids_sql = (
                select(TaskInstance.id)
                .join(Task, TaskInstance.task_id == Task.id)
                .join(Node, Task.node_id == Node.id)
                .where(*z_filters)
                .order_by(TaskInstance.id.desc())
                .limit(20)
            )
            resource_error_ti_ids = [
                row[0] for row in self.session.execute(z_ids_sql).all()
            ]

        return FatalErrorBreakdownResponse(
            resource=resource,
            app=app,
            infra=infra,
            resource_error_total=resource_error_total,
            resource_error_retried=resource_error_retried,
            resource_error_ti_ids=resource_error_ti_ids,
        )

    @staticmethod
    def _cluster_and_paginate(
        rows: List[Dict[str, Any]],
        workflow_id: int,
        offset: int,
        page: int,
        page_size: int,
    ) -> ErrorLogResponse:
        """Cluster error rows via TF-IDF and return a paginated response.

        Clustering always runs on the full dataset first, then pagination
        is applied to the resulting clusters (not the raw rows).
        """
        df = pd.DataFrame(rows)
        if len(df) == 0 or not df["error"].notna().any():
            return ErrorLogResponse(
                error_logs=[], total_count=0, page=page, page_size=page_size
            )

        clustered = cluster_error_logs(df)
        total_clusters = len(clustered)
        if total_clusters == 0:
            return ErrorLogResponse(
                error_logs=[], total_count=0, page=page, page_size=page_size
            )

        paged = clustered.iloc[offset : offset + page_size]
        error_logs = []
        for _, crow in paged.iterrows():
            error_logs.append(
                ErrorLogItem(
                    task_id=None,
                    task_instance_id=None,
                    task_instance_err_id=None,
                    error_time=None,
                    error=None,
                    task_instance_stderr_log=None,
                    workflow_run_id=int(crow["workflow_run_id"]),
                    workflow_id=int(crow["workflow_id"]),
                    error_score=float(crow["error_score"]),
                    group_instance_count=int(crow["group_instance_count"]),
                    task_instance_ids=list(map(int, crow["task_instance_ids"])),
                    task_ids=list(map(int, crow["task_ids"])),
                    sample_error=(
                        str(crow["sample_error"])
                        if crow["sample_error"] is not None
                        else None
                    ),
                    first_error_time=crow["first_error_time"],
                )
            )
        return ErrorLogResponse(
            error_logs=error_logs,
            total_count=total_clusters,
            page=page,
            page_size=page_size,
        )

    def get_tt_error_log_viz(
        self,
        workflow_id: int,
        task_template_id: int,
        task_instance_id: Optional[int] = None,
        page: int = 1,
        page_size: int = 10,
        recent_errors_only: bool = False,
        cluster_errors: bool = False,
        fatal_tasks_only: bool = False,
        workflow_run_id: Optional[int] = None,
        task_template_version_id: Optional[int] = None,  # Required from GUI callers
    ) -> ErrorLogResponse:
        """Get error logs for a task template ID for GUI visualization.

        Args:
            workflow_id: ID of the workflow
            task_template_id: ID of the task template
            task_instance_id: Optional specific task instance ID
            page: Page number for pagination
            page_size: Number of items per page
            recent_errors_only: Whether to show only recent errors
            cluster_errors: Whether to cluster similar errors

        Returns:
            ErrorLogResponse with paginated error log data
        """
        offset = (page - 1) * page_size

        ttv_id = task_template_version_id or self._find_ttvid(
            workflow_id, task_template_id
        )
        if ttv_id is None:
            return ErrorLogResponse(
                error_logs=[],
                total_count=0,
                page=page,
                page_size=page_size,
            )

        error_logs = []
        # if task instance id is provided, just check that instance
        if task_instance_id is not None:
            sql = (
                select(
                    TaskInstance.task_id,
                    TaskInstance.id,
                    TaskInstanceErrorLog.id,
                    TaskInstanceErrorLog.error_time,
                    TaskInstanceErrorLog.description,
                    TaskInstance.stderr_log,
                    TaskInstance.workflow_run_id,
                    Task.workflow_id,
                )
                .join_from(
                    TaskInstanceErrorLog,
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .join_from(
                    TaskInstance,
                    Task,
                    TaskInstance.task_id == Task.id,
                )
                .where(TaskInstance.id == task_instance_id)
            )
            rows = self.session.execute(sql).all()

            # Fallback: no error log entries but TI exists —
            # return stderr_log directly from the task instance.
            if len(rows) == 0:
                fallback_sql = (
                    select(
                        TaskInstance.task_id,
                        TaskInstance.id,
                        TaskInstance.stderr_log,
                        TaskInstance.stderr,
                        TaskInstance.workflow_run_id,
                        Task.workflow_id,
                    )
                    .join(
                        Task,
                        TaskInstance.task_id == Task.id,
                    )
                    .where(TaskInstance.id == task_instance_id)
                )
                fb_row = self.session.execute(fallback_sql).one_or_none()
                if fb_row is None:
                    return ErrorLogResponse(
                        error_logs=[],
                        total_count=0,
                        page=page,
                        page_size=page_size,
                    )
                stderr_text = fb_row.stderr_log or fb_row.stderr
                error_logs.append(
                    ErrorLogItem(
                        task_id=fb_row.task_id,
                        task_instance_id=task_instance_id,
                        task_instance_err_id=None,
                        error_time=None,
                        error=stderr_text,
                        task_instance_stderr_log=stderr_text,
                        workflow_run_id=(fb_row.workflow_run_id),
                        workflow_id=fb_row.workflow_id,
                    )
                )
                return ErrorLogResponse(
                    error_logs=error_logs,
                    total_count=1,
                    page=page,
                    page_size=page_size,
                )

            total_count = len(rows)

            # Slice rows according to the requested page and page_size
            paged_rows = rows[offset : offset + page_size]

            for row in paged_rows:
                error_logs.append(
                    ErrorLogItem(
                        task_id=row.task_id,
                        task_instance_id=task_instance_id,
                        task_instance_err_id=row.id,
                        error_time=row.error_time,
                        error=row.description,
                        task_instance_stderr_log=row.stderr_log,
                        workflow_run_id=row.workflow_run_id,
                        workflow_id=row.workflow_id,
                    )
                )
            return ErrorLogResponse(
                error_logs=error_logs,
                total_count=total_count,
                page=page,
                page_size=page_size,
            )

        # Latest error per fatal task — one error per task regardless
        # of workflow run, using the most recent error log entry.
        if fatal_tasks_only:
            query = (
                select(
                    func.max(TaskInstanceErrorLog.id).label("ttel_id"),
                    Task.id.label("task_id"),
                )
                .select_from(TaskInstanceErrorLog)
                .join(
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .join(Task, TaskInstance.task_id == Task.id)
                .join(Node, Task.node_id == Node.id)
                .where(
                    Task.workflow_id == workflow_id,
                    Task.status == TaskStatus.ERROR_FATAL,
                    Node.task_template_version_id == ttv_id,
                )
                .group_by(Task.id)
                .order_by("ttel_id")
            )
            rows = self.session.execute(query).all()
            total_count = len(rows)

            if total_count == 0:
                return ErrorLogResponse(
                    error_logs=[], total_count=0, page=page, page_size=page_size
                )

            # For clustering, use all rows; for non-clustered, paginate
            all_ttel_map = {row.ttel_id: row.task_id for row in rows}

            detail_query = (
                select(
                    TaskInstanceErrorLog.id,
                    TaskInstanceErrorLog.task_instance_id,
                    TaskInstanceErrorLog.error_time,
                    TaskInstanceErrorLog.description,
                    TaskInstance.stderr_log,
                    TaskInstance.workflow_run_id,
                )
                .join(
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .where(
                    TaskInstanceErrorLog.id.in_(
                        list(all_ttel_map.keys())
                        if cluster_errors
                        else [r.ttel_id for r in rows[offset : offset + page_size]]
                    )
                )
            )
            detail_rows = self.session.execute(detail_query).all()

            if cluster_errors:
                rows_dicts = [
                    {
                        "task_instance_err_id": r[0],
                        "task_instance_id": r[1],
                        "error_time": r[2],
                        "error": str(r[3]) if r[3] is not None else None,
                        "task_instance_stderr_log": r[4],
                        "workflow_run_id": r[5],
                        "workflow_id": workflow_id,
                        "task_id": all_ttel_map.get(r[0]),
                    }
                    for r in detail_rows
                ]
                return self._cluster_and_paginate(
                    rows_dicts, workflow_id, offset, page, page_size
                )

            error_logs = []
            for r in detail_rows:
                error_logs.append(
                    ErrorLogItem(
                        task_id=all_ttel_map.get(r[0]),
                        task_instance_id=r[1],
                        task_instance_err_id=r[0],
                        error_time=r[2],
                        error=r[3],
                        task_instance_stderr_log=r[4],
                        workflow_run_id=r[5],
                        workflow_id=workflow_id,
                    )
                )
            return ErrorLogResponse(
                error_logs=error_logs,
                total_count=total_count,
                page=page,
                page_size=page_size,
            )

        # Scope to a specific workflow run (explicit or latest)
        if recent_errors_only or workflow_run_id is not None:
            if workflow_run_id is None:
                workflow_run_id = self.session.execute(
                    select(func.max(WorkflowRun.id)).where(
                        WorkflowRun.workflow_id == workflow_id
                    )
                ).scalar_one()

            if workflow_run_id is None:
                return ErrorLogResponse(
                    error_logs=[],
                    total_count=0,
                    page=page,
                    page_size=page_size,
                )

            # Explaination of the optimized query
            """
            Using:
                workflow_id = 492916
                task_template_id = 870
                workflow_run_id = 351882
                task_template_version_id = 22938253

            Original query (in raw sql format):

                SELECT COUNT(tiel.id)
                FROM task_instance_error_log tiel
                JOIN task_instance ti ON tiel.task_instance_id = ti.id
                JOIN task ti2 ON ti.task_id = ti2.id
                JOIN workflow_run wr ON ti.workflow_run_id = wr.id
                JOIN node n ON ti2.node_id = n.id
                JOIN task_template_version ttv ON n.task_template_version_id = ttv.id
                JOIN task_template tt ON ttv.task_template_id = tt.id
                JOIN (
                    SELECT task_id, MAX(id) as latest_task_instance_id
                    FROM task_instance
                    GROUP BY task_id
                ) lti ON ti.id = lti.latest_task_instance_id
                JOIN (
                    SELECT workflow_id, MAX(id) as latest_workflow_run_id
                    FROM workflow_run
                    GROUP BY workflow_id
                ) lwr ON ti.workflow_run_id = lwr.latest_workflow_run_id
                WHERE tt.id = 870
                AND ti2.workflow_id = 492916
                AND ti.id = lti.latest_task_instance_id
                AND ti.workflow_run_id = lwr.latest_workflow_run_id
            takes 3 minutes.

            The optimized query (in raw sql format):
                select count(distinct task.id)
                from task_instance_error_log
                join task_instance on
                    task_instance_error_log.task_instance_id=task_instance.id
                join task on task.id=task_instance.task_id
                join node on task.node_id=node.id
                where task_instance.workflow_run_id=351882
                and node.task_template_version_id=22938253;
            takes 0.006s.

            Jon's trouble some query that can not return within db timeout:
                select count(distinct task.id)
                from task_instance_error_log
                join task_instance on
                    task_instance_error_log.task_instance_id=task_instance.id
                join task on task.id=task_instance.task_id
                join node on task.node_id=node.id
                where task_instance.workflow_run_id=351092
                and node.task_template_version_id=23456408
            takes 0.293sec.

            If combine the count and the acture query:
                select max(task_instance_error_log.id) as ttel_id, task_id
                from task_instance_error_log
                join task_instance on
                     task_instance_error_log.task_instance_id=task_instance.id
                join task on task.id=task_instance.task_id
                join node on task.node_id=node.id
                 where task_instance.workflow_run_id=351882
                 and node.task_template_version_id=22938253
                 group by task.id
                 order by ttel_id
            takes 0.006s (0.108s for Jon's).

            At last, test a case of many failures, wf 492914 ttv 53173:
                Original count query: 3 min 13.148 sec;
                New combined query: 0.021 sec
            """
            wfr_filters = [
                TaskInstance.workflow_run_id == workflow_run_id,
                Node.task_template_version_id == ttv_id,
            ]
            # recent_errors_only: diagnostic view — show all errors
            # from the run regardless of current task status.
            # Explicit workflow_run_id (from workflow details page):
            # only show errors for tasks still in fatal state,
            # since resolved errors are noise in the current-state view.
            if not recent_errors_only:
                wfr_filters.append(Task.status == TaskStatus.ERROR_FATAL)
            query = (
                select(
                    func.max(TaskInstanceErrorLog.id).label("ttel_id"),
                    Task.id.label("task_id"),
                )
                .join_from(
                    TaskInstanceErrorLog,
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .join_from(TaskInstance, Task, TaskInstance.task_id == Task.id)
                .join_from(Task, Node, Task.node_id == Node.id)
                .where(*wfr_filters)
                .group_by(Task.id)
                .order_by("ttel_id")
            )
            rows = self.session.execute(query).all()

            total_count = len(rows)

            if total_count == 0:
                return ErrorLogResponse(
                    error_logs=[],
                    total_count=0,
                    page=page,
                    page_size=page_size,
                )

            # For clustering, use all rows; for non-clustered, paginate
            all_ttel_map = {row.ttel_id: row.task_id for row in rows}

            detail_query = (
                select(
                    TaskInstanceErrorLog.id,
                    TaskInstanceErrorLog.task_instance_id,
                    TaskInstanceErrorLog.error_time,
                    TaskInstanceErrorLog.description,
                    TaskInstance.stderr_log,
                    TaskInstance.workflow_run_id,
                )
                .join(
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .where(
                    TaskInstanceErrorLog.id.in_(
                        list(all_ttel_map.keys())
                        if cluster_errors
                        else [r.ttel_id for r in rows[offset : offset + page_size]]
                    )
                )
            )
            detail_rows = self.session.execute(detail_query).all()

            if cluster_errors:
                rows_dicts = [
                    {
                        "task_instance_err_id": r[0],
                        "task_instance_id": r[1],
                        "error_time": r[2],
                        "error": (str(r[3]) if r[3] is not None else None),
                        "task_instance_stderr_log": (
                            str(r[4]) if r[4] is not None else None
                        ),
                        "workflow_run_id": r[5],
                        "workflow_id": workflow_id,
                        "task_id": all_ttel_map.get(r[0]),
                    }
                    for r in detail_rows
                ]
                return self._cluster_and_paginate(
                    rows_dicts, workflow_id, offset, page, page_size
                )
            else:
                paged_ttel_map = {
                    row.ttel_id: row.task_id
                    for row in rows[offset : offset + page_size]
                }
                for row in detail_rows:
                    error_logs.append(
                        ErrorLogItem(
                            task_id=paged_ttel_map[row[0]],
                            task_instance_id=row[1],
                            task_instance_err_id=row[0],
                            error_time=row[2],
                            error=str(row[3]),
                            task_instance_stderr_log=str(row[4]),
                            workflow_run_id=row[5],
                            workflow_id=workflow_id,
                        )
                    )
                return ErrorLogResponse(
                    error_logs=error_logs,
                    total_count=total_count,
                    page=page,
                    page_size=page_size,
                )
        else:
            # this one is similar without latest wfr and ti restriction
            # Benchmark:
            """
            Use Jon's quiery as example:
                workflow_id: 490688
                task_template_id: 9739

            Original query (in raw sql format):
                SELECT COUNT(tiel.id)
                FROM task_instance_error_log AS tiel
                JOIN task_instance AS ti
                ON tiel.task_instance_id = ti.id
                JOIN task AS t
                ON ti.task_id = t.id
                JOIN workflow_run AS wr
                ON ti.workflow_run_id = wr.id
                JOIN node AS n
                ON t.node_id = n.id
                JOIN task_template_version AS ttv
                ON n.task_template_version_id = ttv.id
                JOIN task_template AS tt
                ON ttv.task_template_id = tt.id
                WHERE t.workflow_id = 490688
                AND ttv.id = 9739;
            takes 42.110sec.

            Optimized query (in raw sql format):
                select count(task.id)
                from task_instance_error_log
                join task_instance on
                    task_instance_error_log.task_instance_id=task_instance.id
                join task on task.id=task_instance.task_id
                join node on task.node_id=node.id
                where task.workflow_id=490688
                and node.task_template_version_id=23456408;
            takes 0.37sec.

            However, when using a smaller workflow, like wf 492916 and tt 870,
            both queries takes 0.006s.
            """
            query = (
                select(
                    TaskInstanceErrorLog.id,
                    TaskInstanceErrorLog.task_instance_id,
                    TaskInstanceErrorLog.error_time,
                    TaskInstanceErrorLog.description,
                    TaskInstance.stderr_log,
                    TaskInstance.workflow_run_id,
                    TaskInstance.task_id,
                )
                .join(
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .join(Task, Task.id == TaskInstance.task_id)
                .join(Node, Node.id == Task.node_id)
                .where(
                    Task.workflow_id == workflow_id,
                    Node.task_template_version_id == ttv_id,
                )
                .order_by(TaskInstanceErrorLog.id)
            )

            rows = self.session.execute(query).all()

            # Fallback: no TaskInstanceErrorLog entries — read
            # stderr directly from task instances for failed tasks.
            if not rows:
                fallback_query = (
                    select(
                        TaskInstance.id.label("task_instance_id"),
                        TaskInstance.task_id,
                        TaskInstance.stderr_log,
                        TaskInstance.stderr,
                        TaskInstance.status_date,
                        TaskInstance.workflow_run_id,
                    )
                    .join(
                        Task,
                        TaskInstance.task_id == Task.id,
                    )
                    .join(Node, Task.node_id == Node.id)
                    .where(
                        Task.workflow_id == workflow_id,
                        Node.task_template_version_id == ttv_id,
                        Task.status == "F",
                    )
                    .order_by(TaskInstance.id.desc())
                    .limit(1000)
                )
                fb_rows = self.session.execute(fallback_query).all()

                if not fb_rows:
                    return ErrorLogResponse(
                        error_logs=[],
                        total_count=0,
                        page=page,
                        page_size=page_size,
                    )

                if cluster_errors:
                    rows_dicts = [
                        {
                            "task_instance_err_id": r.task_instance_id,
                            "task_instance_id": r.task_instance_id,
                            "error_time": r.status_date,
                            "error": (
                                str(r.stderr_log or r.stderr)
                                if (r.stderr_log or r.stderr)
                                else None
                            ),
                            "task_instance_stderr_log": (
                                str(r.stderr_log or r.stderr)
                                if (r.stderr_log or r.stderr)
                                else None
                            ),
                            "workflow_run_id": r.workflow_run_id,
                            "workflow_id": workflow_id,
                            "task_id": r.task_id,
                        }
                        for r in fb_rows
                    ]
                    return self._cluster_and_paginate(
                        rows_dicts, workflow_id, offset, page, page_size
                    )

                # Non-clustered fallback
                total_count = len(fb_rows)
                paged = fb_rows[offset : offset + page_size]
                for r in paged:
                    stderr_text = str(r.stderr_log or r.stderr or "")
                    error_logs.append(
                        ErrorLogItem(
                            task_id=r.task_id,
                            task_instance_id=r.task_instance_id,
                            task_instance_err_id=None,
                            error_time=r.status_date,
                            error=stderr_text,
                            task_instance_stderr_log=stderr_text,
                            workflow_run_id=r.workflow_run_id,
                            workflow_id=workflow_id,
                        )
                    )
                return ErrorLogResponse(
                    error_logs=error_logs,
                    total_count=total_count,
                    page=page,
                    page_size=page_size,
                )

            if cluster_errors and rows:
                rows_dicts = [
                    {
                        "task_instance_err_id": r[0],
                        "task_instance_id": r[1],
                        "error_time": r[2],
                        "error": (str(r[3]) if r[3] is not None else None),
                        "task_instance_stderr_log": (
                            str(r[4]) if r[4] is not None else None
                        ),
                        "workflow_run_id": r[5],
                        "workflow_id": workflow_id,
                        "task_id": r[6],
                    }
                    for r in rows
                ]
                return self._cluster_and_paginate(
                    rows_dicts, workflow_id, offset, page, page_size
                )

            total_count = len(rows)
            if total_count == 0:
                return ErrorLogResponse(
                    error_logs=[],
                    total_count=0,
                    page=page,
                    page_size=page_size,
                )
            paged_rows = rows[offset : offset + page_size]
            for row in paged_rows:
                error_logs.append(
                    ErrorLogItem(
                        task_id=row[6],
                        task_instance_id=row[1],
                        task_instance_err_id=row[0],
                        error_time=row[2],
                        error=str(row[3]),
                        task_instance_stderr_log=str(row[4]),
                        workflow_run_id=row[5],
                        workflow_id=workflow_id,
                    )
                )
            return ErrorLogResponse(
                error_logs=error_logs,
                total_count=total_count,
                page=page,
                page_size=page_size,
            )

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import numpy as np
import scipy.stats as st  # type: ignore
import structlog
from sqlalchemy import String, and_, func, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement, Label

from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.schemas.task_template import (
    TaskResourceDetailItem,
    TaskResourceVizItem,
    TaskTemplateResourceUsageRequest,
)

logger = structlog.get_logger(__name__)


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
                task_name=row_data.get("task_name"),
                requested_resources=row_data["requested_resources"],
                attempt_number_of_instance=row_data.get("attempt_number_of_instance"),
                status=row_data.get("status_orig"),
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
    ) -> List[TaskResourceDetailItem]:
        """Fetch and filter task resource details with optimized single-query approach."""
        base_filters = [
            TaskTemplateVersion.id == task_template_version_id,
            TaskInstance.status.in_(
                [
                    TaskInstanceStatus.DONE,
                    TaskInstanceStatus.RESOURCE_ERROR,
                    TaskInstanceStatus.NO_HEARTBEAT,
                    TaskInstanceStatus.UNKNOWN_ERROR,
                    TaskInstanceStatus.ERROR_FATAL,
                    TaskInstanceStatus.ERROR,
                ]
            ),
        ]

        if workflows:
            base_filters.append(Task.workflow_id.in_(workflows))

        attempt_number_col = (
            func.row_number()
            .over(partition_by=Task.id, order_by=TaskInstance.id)
            .label("attempt_number_of_instance")
        )

        if not node_args:
            # no node_args filtering - use original optimized query
            query = (
                select(
                    TaskInstance.wallclock,
                    TaskInstance.maxrss,
                    Node.id.label("node_id_col"),
                    Task.id.label("task_id_col"),
                    Task.name.label("task_name_col"),
                    TaskInstance.id.label("task_instance_id_col"),
                    TaskResources.requested_resources.label("requested_resources_col"),
                    attempt_number_col,
                    TaskInstance.status.label("status_col"),
                )
                .select_from(TaskTemplateVersion)
                .join(Node, TaskTemplateVersion.id == Node.task_template_version_id)
                .join(Task, Node.id == Task.node_id)
                .join(TaskInstance, Task.id == TaskInstance.task_id)
                .join(TaskResources, TaskInstance.task_resources_id == TaskResources.id)
                .where(and_(*base_filters))
            )

            rows = self.session.execute(query).all()

            result = []
            for row in rows:
                row_data = {
                    "r_orig": row[0],
                    "m_orig": row[1],
                    "node_id": row[2],
                    "task_id": row[3],
                    "task_name": row[4],
                    "requested_resources": row[6],
                    "attempt_number_of_instance": row[7],
                    "status_orig": row[8],
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
                attempt_number_col,
            )

    def _get_task_resource_details_with_node_args(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Dict[str, List[Any]],
        base_filters: List[ColumnElement],
        attempt_number_col: Label,
    ) -> List[TaskResourceDetailItem]:
        """Optimized node_args filtering using database-level joins and filtering."""
        base_query = (
            select(
                TaskInstance.wallclock,
                TaskInstance.maxrss,
                Node.id.label("node_id"),
                Task.id.label("task_id"),
                Task.name.label("task_name"),
                TaskResources.requested_resources,
                attempt_number_col,
                TaskInstance.status,
            )
            .select_from(TaskTemplateVersion)
            .join(Node, TaskTemplateVersion.id == Node.task_template_version_id)
            .join(Task, Node.id == Task.node_id)
            .join(TaskInstance, Task.id == TaskInstance.task_id)
            .join(TaskResources, TaskInstance.task_resources_id == TaskResources.id)
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
            base_query.c.status,
        ).select_from(base_query)

        for subquery in node_arg_subqueries:
            final_query = final_query.where(base_query.c.node_id.in_(subquery))

        rows = self.session.execute(final_query).all()

        result = []
        for row in rows:
            row_data = {
                "r_orig": row[0],
                "m_orig": row[1],
                "node_id": row[2],
                "task_id": row[3],
                "task_name": row[4],
                "requested_resources": row[5],
                "attempt_number_of_instance": row[6],
                "status_orig": row[7],
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
                        task_name=detail_item.task_name,
                        requested_resources=detail_item.requested_resources,
                        attempt_number_of_instance=detail_item.attempt_number_of_instance,
                        status=detail_item.status,
                    )
                )

        return viz_data

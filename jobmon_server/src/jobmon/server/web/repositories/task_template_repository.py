from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import numpy as np
import scipy.stats as st  # type: ignore
import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.schemas.task_template import (
    TaskResourceDetailItem,
    TaskResourceVizItem,
    TaskTemplateResourceUsageRequest,
    TaskTemplateDetailsResponse,
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

    def get_task_resource_details(
        self,
        task_template_version_id: int,
        workflows: Optional[List[int]],
        node_args: Optional[Dict[str, List[Any]]],
    ) -> List[TaskResourceDetailItem]:
        """Fetch and filter task resource details."""
        query_filter = [
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
            query_filter.append(Task.workflow_id.in_(workflows))

        attempt_number_col = (
            func.row_number()
            .over(partition_by=Task.id, order_by=TaskInstance.id)
            .label("attempt_number_of_instance")
        )

        query_base = (
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
        )

        sql = query_base.where(*query_filter)

        rows_from_db = self.session.execute(sql).all()

        intermediate_data: List[Dict[str, Any]] = []
        for row in rows_from_db:
            intermediate_data.append(
                {
                    "r_orig": row[0],
                    "m_orig": row[1],
                    "node_id": row[2],
                    "task_id": row[3],
                    "task_name": row[4],
                    "requested_resources": row[6],
                    "attempt_number_of_instance": row[7],
                    "status_orig": row[8],
                }
            )

        fetched_tasks_data: List[TaskResourceDetailItem] = []

        # Apply node_args filtering if present
        if node_args:
            node_args_filtered_tasks_intermediate: List[Dict[str, Any]] = []
            for task_dict in intermediate_data:
                # Node args filtering logic
                node_f = [
                    NodeArg.arg_id == Arg.id,
                    NodeArg.node_id == task_dict["node_id"],
                ]
                node_s = select(Arg.name, NodeArg.val).where(*node_f)
                actual_node_args_rows = self.session.execute(node_s).all()

                node_args_on_db: Dict[str, List[str]] = {}
                for name, val_db in actual_node_args_rows:
                    if name not in node_args_on_db:
                        node_args_on_db[name] = []
                    node_args_on_db[name].append(str(val_db))

                include_task_item = True
                # Use AND logic - task matches only if ALL filter conditions match
                for filter_arg_name, filter_arg_values_any in node_args.items():
                    filter_arg_values = [str(v) for v in filter_arg_values_any]

                    if filter_arg_name not in node_args_on_db:
                        include_task_item = False
                        break

                    match_found_for_this_filter_arg = False
                    for actual_val_for_arg in node_args_on_db[filter_arg_name]:
                        if actual_val_for_arg in filter_arg_values:
                            match_found_for_this_filter_arg = True
                            break

                    if not match_found_for_this_filter_arg:
                        include_task_item = False
                        break

                if include_task_item:
                    node_args_filtered_tasks_intermediate.append(task_dict)
                else:
                    continue

            # Convert to Pydantic models after filtering
            for task_item_dict in node_args_filtered_tasks_intermediate:
                try:
                    detail_item = TaskResourceDetailItem(
                        wallclock=(
                            float(task_item_dict["r_orig"])
                            if task_item_dict["r_orig"] is not None
                            else None
                        ),
                        maxrss=(
                            int(task_item_dict["m_orig"])
                            if task_item_dict["m_orig"] is not None
                            else None
                        ),
                        node_id=task_item_dict["node_id"],
                        task_id=task_item_dict["task_id"],
                        task_name=task_item_dict.get("task_name"),
                        requested_resources=task_item_dict["requested_resources"],
                        attempt_number_of_instance=task_item_dict.get(
                            "attempt_number_of_instance"
                        ),
                        status=task_item_dict.get("status_orig"),
                    )
                    fetched_tasks_data.append(detail_item)
                except Exception as e:
                    logger.error(
                        f"Error parsing task data after node_args filter for "
                        f"task_id {task_item_dict['task_id']}: {e}. "
                        f"Data: {task_item_dict}"
                    )
                    continue

            return fetched_tasks_data
        else:
            # Convert to Pydantic models if no node_args filtering
            for task_item_dict in intermediate_data:
                try:
                    detail_item = TaskResourceDetailItem(
                        wallclock=(
                            float(task_item_dict["r_orig"])
                            if task_item_dict["r_orig"] is not None
                            else None
                        ),
                        maxrss=(
                            int(task_item_dict["m_orig"])
                            if task_item_dict["m_orig"] is not None
                            else None
                        ),
                        node_id=task_item_dict["node_id"],
                        task_id=task_item_dict["task_id"],
                        task_name=task_item_dict.get("task_name"),
                        requested_resources=task_item_dict["requested_resources"],
                        attempt_number_of_instance=task_item_dict.get(
                            "attempt_number_of_instance"
                        ),
                        status=task_item_dict.get("status_orig"),
                    )
                    fetched_tasks_data.append(detail_item)
                except Exception as e:
                    logger.error(
                        f"Error parsing task data for "
                        f"task_id {task_item_dict['task_id']}: {e}. "
                        f"Data: {task_item_dict}"
                    )
                    continue
            return fetched_tasks_data

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

    def get_task_template_details(self, 
            workflow_id: int, 
            task_template_id: int) -> Optional[TaskTemplateDetailsResponse]:
        """Get task template details."""
        query_filter = [
            Task.workflow_id == workflow_id,
            Task.node_id == Node.id,
            Node.task_template_version_id == TaskTemplateVersion.id,
            TaskTemplateVersion.task_template_id == task_template_id,
            TaskTemplateVersion.task_template_id == TaskTemplate.id,
        ]

        sql = (
            select(
                TaskTemplate.id,
                TaskTemplate.name,
                TaskTemplateVersion.id.label("task_template_version_id"),
            )
            .where(*query_filter)
            .distinct()
        )

        row = self.session.execute(sql).one_or_none()

        if row is None:
            return None

        tt_details_data = TaskTemplateDetailsResponse(
            task_template_id=row.id,
            task_template_name=row.name,
            task_template_version_id=row.task_template_version_id,
        )

        return tt_details_data

    
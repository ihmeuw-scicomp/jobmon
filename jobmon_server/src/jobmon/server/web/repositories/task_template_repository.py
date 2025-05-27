from typing import Any, Dict, List, Optional

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
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.schemas.task_template import (
    TaskResourceDetailItem,
    TaskResourceVizItem,
    TaskTemplateResourceUsageRequest,
)

logger = structlog.get_logger(__name__)


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
                    "requested_resources": row[5],
                    "attempt_number_of_instance": row[6],
                    "status_orig": row[7],
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
                        requested_resources=detail_item.requested_resources,
                        attempt_number_of_instance=detail_item.attempt_number_of_instance,
                        status=detail_item.status,
                    )
                )

        return viz_data

"""Repository for Task operations."""

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
import structlog
from sqlalchemy import and_, select, update
from sqlalchemy.orm import Session

from jobmon.core import constants
from jobmon.core.constants import Direction
from jobmon.core.serializers import SerializeTaskResourceUsage
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.schemas.task import (
    DownstreamTasksResponse,
    TaskDependenciesResponse,
    TaskDependencyItem,
    TaskDetailItem,
    TaskDetailsResponse,
    TaskInstanceDetailItem,
    TaskInstanceDetailsResponse,
    TaskResourceUsageResponse,
    TaskStatusResponse,
    TaskSubdagResponse,
)
from jobmon.server.web.server_side_exception import InvalidUsage
from jobmon.server.web.services.transition_service import TransitionService

logger = structlog.get_logger(__name__)

_task_instance_label_mapping = {
    "Q": "PENDING",
    "B": "PENDING",
    "I": "PENDING",
    "R": "RUNNING",
    "E": "FATAL",
    "Z": "FATAL",
    "W": "FATAL",
    "U": "FATAL",
    "K": "FATAL",
    "D": "DONE",
}

_reversed_task_instance_label_mapping = {
    "PENDING": ["Q", "B", "I"],
    "RUNNING": ["R"],
    "FATAL": ["E", "Z", "W", "U", "K"],
    "DONE": ["D"],
}


class TaskRepository:
    def __init__(self, session: Session) -> None:
        """Initialize the TaskRepository with a database session."""
        self.session = session

    def update_task_statuses(
        self,
        workflow_id: str,
        recursive: bool,
        workflow_status: Optional[str],
        task_ids: Union[List[int], str],
        new_status: str,
    ) -> None:
        """Update the status of tasks with business logic.

        Description:

        - When ``task_ids='all'``, it updates all tasks in the workflow with
          ``recursive=False``. This improves performance.
        - When ``recursive=True``, it updates the tasks and its dependencies all
          the way up or down the DAG.
        - When ``recursive=False``, it updates only the tasks in the task_ids list.
        - When ``workflow_status`` is None, it gets the workflow status from the db.
        - After updating the tasks, it checks the workflow status and updates it.
        """
        # Get all task IDs if task_ids is "all"
        if task_ids == "all":
            task_ids = self._get_all_task_ids(workflow_id)

        if isinstance(task_ids, str):
            raise InvalidUsage(f"Invalid task_ids value: {task_ids}")

        task_ids = list(task_ids)

        # Get recursive task IDs if needed
        if recursive:
            task_ids = self._get_recursive_task_ids(task_ids, new_status)

        # Update task statuses
        self._update_task_statuses_in_db(task_ids, new_status, workflow_id)

        # Handle special cases based on status
        if new_status == constants.TaskStatus.REGISTERING:
            self._handle_registering_status(workflow_id, task_ids, workflow_status)
        elif new_status == constants.TaskStatus.DONE:
            self._handle_done_status(workflow_id, new_status)

    def _get_all_task_ids(self, workflow_id: str) -> List[int]:
        """Get all task IDs for a workflow."""
        task_ids = (
            self.session.query(Task.id).filter(Task.workflow_id == workflow_id).all()
        )
        return [task_id for task_id, in task_ids]

    def _get_recursive_task_ids(
        self, task_ids: List[int], new_status: str
    ) -> List[int]:
        """Get task IDs including dependencies based on status direction."""
        if new_status == constants.TaskStatus.DONE:
            logger.info("recursive update including upstream tasks")
            direction = constants.Direction.UP
        elif new_status == constants.TaskStatus.REGISTERING:
            logger.info("recursive update including downstream tasks")
            direction = constants.Direction.DOWN
        else:
            raise InvalidUsage(
                f"Invalid new_status {new_status} for recursive update",
                status_code=400,
            )

        task_ids = list(self._get_tasks_recursive(set(task_ids), direction))
        logger.info(f"reset status to new_status: {new_status}")
        return task_ids

    def _update_task_statuses_in_db(
        self, task_ids: List[int], new_status: str, workflow_id: str
    ) -> None:
        """Update task statuses in the database."""
        # Get previous statuses for audit (only tasks that will change)
        prev_statuses = self.session.execute(
            select(Task.id, Task.status).where(
                Task.id.in_(task_ids), Task.status != new_status
            )
        ).all()

        # Update
        update_stmt = update(Task).where(
            and_(Task.id.in_(task_ids), Task.status != new_status)
        )
        self.session.execute(update_stmt.values(status=new_status))
        self.session.flush()

        # Audit
        if prev_statuses:
            audit_records = [
                {
                    "task_id": task_id,
                    "workflow_id": int(workflow_id),
                    "previous_status": prev_status,
                    "new_status": new_status,
                }
                for task_id, prev_status in prev_statuses
            ]
            TransitionService.create_audit_records_bulk(
                session=self.session, records=audit_records
            )

    def _get_workflow_run(self, workflow_id: str) -> WorkflowRun | None:
        """Get the latest workflow run for a workflow."""
        return (
            self.session.query(WorkflowRun)
            .filter(WorkflowRun.workflow_id == workflow_id)
            .order_by(WorkflowRun.id.desc())
            .first()
        )

    def _kill_active_task_instances(
        self, task_ids: List[int], workflow_run_id: int
    ) -> None:
        """Kill active task instances for the given tasks."""
        active_statuses = [
            constants.TaskInstanceStatus.SUBMITTED_TO_BATCH_DISTRIBUTOR,
            constants.TaskInstanceStatus.INSTANTIATED,
            constants.TaskInstanceStatus.LAUNCHED,
            constants.TaskInstanceStatus.QUEUED,
            constants.TaskInstanceStatus.RUNNING,
            constants.TaskInstanceStatus.TRIAGING,
            constants.TaskInstanceStatus.NO_HEARTBEAT,
        ]

        # Process task_ids in batches to reduce lock duration
        batch_size = 100
        for i in range(0, len(task_ids), batch_size):
            batch_task_ids = task_ids[i : i + batch_size]

            # First, get the IDs of rows that need updating using a subquery
            subquery = (
                self.session.query(TaskInstance.id)
                .filter(
                    TaskInstance.workflow_run_id == workflow_run_id,
                    TaskInstance.task_id.in_(batch_task_ids),
                    TaskInstance.status.in_(active_statuses),
                )
                .subquery()
            )

            # Update TIs to "K" status
            task_instance_update_stmt = update(TaskInstance).where(
                TaskInstance.id.in_(self.session.query(subquery.c.id))
            )
            vals = {"status": constants.TaskInstanceStatus.KILL_SELF}
            self.session.execute(task_instance_update_stmt.values(**vals))
            self.session.flush()

    def _handle_registering_status(
        self,
        workflow_id: str,
        task_ids: List[int],
        workflow_status: Optional[str],
    ) -> None:
        """Handle special logic for REGISTERING status."""
        wfr = self._get_workflow_run(workflow_id)
        if wfr is None:
            raise ValueError(f"No workflow run found for workflow_id: {workflow_id}")
        self._kill_active_task_instances(task_ids, wfr.id)

        # Get workflow status from db if not provided
        if workflow_status is None:
            workflow_status = (
                self.session.query(Workflow.status)
                .filter(Workflow.id == workflow_id)
                .scalar()
            )

        # If workflow is done, need to set it to an error state before resuming
        if workflow_status == constants.WorkflowStatus.DONE:
            logger.info(f"reset workflow status for workflow_id: {workflow_id}")
            workflow_update_stmt = update(Workflow).where(Workflow.id == workflow_id)
            vals = {"status": constants.WorkflowStatus.FAILED}
            self.session.execute(workflow_update_stmt.values(**vals))

    def _handle_done_status(self, workflow_id: str, new_status: str) -> None:
        """Handle special logic for DONE status."""
        tasks_done = (
            self.session.query(Task.id)
            .filter(Task.workflow_id == workflow_id, Task.status != new_status)
            .all()
        )

        if not tasks_done:
            logger.info(f"set workflow status to DONE for workflow_id: {workflow_id}")
            workflow_update_stmt = update(Workflow).where(Workflow.id == workflow_id)
            vals = {"status": constants.WorkflowStatus.DONE}
            self.session.execute(workflow_update_stmt.values(**vals))

    def _get_tasks_recursive(
        self, task_ids: Set[int], direction: Direction
    ) -> Set[int]:
        """Get all task IDs connected in the specified direction iteratively.

        Starting with the given task_ids, the function traverses the dependency graph
        and returns all tasks found, including the input set. It also verifies that all
        tasks belong to the same workflow.

        Args:
            task_ids (Set[int]): Initial set of task IDs.
            direction (Direction): Either Direction.UP or Direction.DOWN.

        Returns:
            Set[int]: The complete set of task IDs connected in the specified direction.
        """
        # make sure all tasks belong to the same workflow
        distinct_workflow_ids = (
            self.session.query(Task.workflow_id)
            .filter(Task.id.in_(task_ids))
            .distinct()
            .all()
        )

        if len(distinct_workflow_ids) == 1:
            # All tasks share the same workflow_id.
            workflow_id = distinct_workflow_ids[0][0]  # Extract the workflow_id value
        else:
            # The tasks belong to different workflows.
            raise InvalidUsage(
                f"{task_ids} in request belong to different workflow_ids ",
                status_code=400,
            )

        # get dag_id of the workflow_id
        dag_id = (
            self.session.query(Workflow.dag_id)
            .filter(Workflow.id == workflow_id)
            .scalar()
        )

        # get the node_ids of the task_ids
        rows = self.session.query(Task.node_id).filter(Task.id.in_(task_ids)).all()
        node_ids = [int(row[0]) for row in rows]

        # This set will accumulate all discovered node IDs.
        nodes_recursive: Set[int] = set()
        # Use a stack (list) for iterative traversal.
        stack = list(node_ids)

        while stack:
            current_node = stack.pop()
            # Skip if we've already processed this task.
            if current_node in nodes_recursive:
                continue

            # Mark the current node as visited.
            nodes_recursive.add(current_node)
            # Get the node dependencies for the current node based on the specified direction.
            node_deps = self._get_node_dependencies(
                {current_node},
                dag_id,
                (
                    Direction.DOWN
                    if direction == constants.Direction.DOWN
                    else Direction.UP
                ),
            )
            if node_deps:
                # Add the node dependencies to the stack for further processing.
                stack.extend(list(node_deps))

        # get task_ids from node_ids
        tasks_recursive = self._get_tasks_from_nodes(
            workflow_id, list(nodes_recursive), []
        )
        return set(tasks_recursive.keys())

    def _get_node_dependencies(
        self, nodes: set, dag_id: int, direction: Direction
    ) -> Set[int]:
        """Get all upstream or downstream nodes of a node.

        Args:
            nodes (set): set of nodes
            dag_id (int): ID of DAG
            direction (Direction): either up or down
        """
        select_stmt = select(Edge).where(
            Edge.dag_id == int(dag_id), Edge.node_id.in_(list(nodes))
        )
        node_ids: Set[int] = set()
        for row in self.session.execute(select_stmt).all():
            edges = row[0]
            if direction == Direction.UP:
                upstreams = edges.upstream_node_ids
                if upstreams:
                    node_ids.update(upstreams)
            elif direction == Direction.DOWN:
                downstreams = edges.downstream_node_ids
                if downstreams:
                    node_ids.update(downstreams)
            else:
                raise ValueError(
                    f"Invalid direction type. Expected one of: {Direction}"
                )
        return node_ids

    def _get_tasks_from_nodes(
        self, workflow_id: int, nodes: List, task_status: List
    ) -> dict:
        """Get task ids of the given node ids.

        Args:
            workflow_id (int): ID of the workflow
            nodes (list): list of nodes
            task_status (list): list of task statuses
        """
        if not nodes:
            return {}

        select_stmt = select(Task.id, Task.status, Task.name).where(
            Task.workflow_id == workflow_id, Task.node_id.in_(list(nodes))
        )

        result = self.session.execute(select_stmt).all()
        task_dict = {}
        for r in result:
            # When task_status not specified, return the full subdag
            if not task_status:
                task_dict[r[0]] = [r[1], r[2]]
            else:
                if r[1] in task_status:
                    task_dict[r[0]] = [r[1], r[2]]
        return task_dict

    def get_task_status(
        self,
        task_ids: Optional[Union[int, List[int]]],
        status: Optional[Union[str, List[str]]],
    ) -> TaskStatusResponse:
        """Get the status of tasks with filtering."""
        if task_ids is None:
            raise InvalidUsage("Missing task_ids in request", status_code=400)

        if isinstance(task_ids, int):
            task_ids = [task_ids]

        if len(task_ids) == 0:
            raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)

        if status and isinstance(status, str):
            status = [status]

        query_filter = [
            Task.id == TaskInstance.task_id,
            TaskInstanceStatus.id == TaskInstance.status,
        ]

        if status:
            if len(status) > 0:
                status_codes = [
                    i
                    for arg in status
                    for i in _reversed_task_instance_label_mapping[arg]
                ]
            query_filter.append(
                TaskInstance.status.in_([i for arg in status for i in status_codes])
            )

        if task_ids:
            query_filter.append(Task.id.in_(task_ids))

        sql = (
            select(
                Task.id,
                Task.status,
                TaskInstance.id,
                TaskInstance.distributor_id,
                TaskInstanceStatus.label,
                TaskInstance.usage_str,
                TaskInstance.stdout,
                TaskInstance.stderr,
                TaskInstanceErrorLog.description,
            )
            .join_from(
                TaskInstance,
                TaskInstanceErrorLog,
                TaskInstance.id == TaskInstanceErrorLog.task_instance_id,
                isouter=True,
            )
            .where(*query_filter)
        )
        rows = self.session.execute(sql).all()

        column_names = (
            "TASK_ID",
            "task_status",
            "TASK_INSTANCE_ID",
            "DISTRIBUTOR_ID",
            "STATUS",
            "RESOURCE_USAGE",
            "STDOUT",
            "STDERR",
            "ERROR_TRACE",
        )

        if rows and len(rows) > 0:
            # assign to dataframe for serialization
            df = pd.DataFrame(rows, columns=column_names)
            # remap to jobmon_cli statuses
            df.STATUS.replace(to_replace=_task_instance_label_mapping, inplace=True)
            task_instance_status = df.to_json()
        else:
            df = pd.DataFrame({}, columns=column_names)
            task_instance_status = df.to_json()

        return TaskStatusResponse(task_instance_status=task_instance_status)

    def get_task_subdag(
        self, task_ids: List[int], task_status: List[str]
    ) -> TaskSubdagResponse:
        """Get the sub DAG of given tasks."""
        if not task_ids:
            raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)

        if task_status is None:
            task_status = []

        select_stmt = (
            select(
                Task.workflow_id.label("workflow_id"),
                Workflow.dag_id.label("dag_id"),
                Task.node_id.label("node_id"),
            )
            .join_from(Task, Workflow, Task.workflow_id == Workflow.id)
            .where(Task.id.in_(task_ids))
        )

        # Initialize defaultdict to store information
        grouped_data: Dict = defaultdict(
            lambda: {"workflow_id": None, "dag_id": None, "node_ids": []}
        )

        for row in self.session.execute(select_stmt):
            key = (row.workflow_id, row.dag_id)
            grouped_data[key]["workflow_id"] = row.workflow_id
            grouped_data[key]["dag_id"] = row.dag_id
            if grouped_data[key]:
                grouped_data[key]["node_ids"].append(row.node_id)

        # If we find no results, we handle it here
        if not grouped_data:
            return TaskSubdagResponse(workflow_id=None, sub_task=None)

        # Since we have validated all the tasks belong to the same wf in status_command
        # before this call, assume they all belong to the same wf.
        if grouped_data:
            some_key = next(iter(grouped_data))
            workflow_id, dag_id = some_key
            node_ids = [int(node_id) for node_id in grouped_data[some_key]["node_ids"]]

            # Continue with your current processing logic
            sub_dag_tree = self._get_subdag(node_ids, dag_id)
            sub_task_tree = self._get_tasks_from_nodes(
                workflow_id, sub_dag_tree, task_status
            )

        return TaskSubdagResponse(workflow_id=workflow_id, sub_task=sub_task_tree)

    def _get_subdag(self, node_ids: List[int], dag_id: int) -> List[int]:
        """Get all descendants of given nodes.

        Args:
            node_ids (list): list of node IDs
            dag_id (int): ID of DAG
        """
        node_set = set(node_ids)
        node_descendants = node_set
        while len(node_descendants) > 0:
            node_descendants = self._get_node_dependencies(
                node_descendants, dag_id, Direction.DOWN
            )
            node_set = node_set.union(node_descendants)
        return list(node_set)

    def get_task_dependencies(self, task_id: int) -> TaskDependenciesResponse:
        """Get task's downstream and upstream tasks and their status."""
        dag_id, workflow_id, node_id = self._get_dag_and_wf_id(task_id)

        if dag_id is None or workflow_id is None or node_id is None:
            return TaskDependenciesResponse(up=[], down=[])

        up_nodes = self._get_node_dependencies({node_id}, dag_id, Direction.UP)
        down_nodes = self._get_node_dependencies({node_id}, dag_id, Direction.DOWN)
        up_task_dict = self._get_tasks_from_nodes(workflow_id, list(up_nodes), [])
        down_task_dict = self._get_tasks_from_nodes(workflow_id, list(down_nodes), [])

        # return a "standard" json format so that it can be reused by future GUI
        up = (
            []
            if up_task_dict is None or len(up_task_dict) == 0
            else [
                [
                    TaskDependencyItem(
                        id=k,
                        status=up_task_dict[k][0],
                        name=up_task_dict[k][1],
                    )
                ]
                for k in up_task_dict
            ]
        )
        down = (
            []
            if down_task_dict is None or len(down_task_dict) == 0
            else [
                [
                    TaskDependencyItem(
                        id=k,
                        status=down_task_dict[k][0],
                        name=down_task_dict[k][1],
                    )
                ]
                for k in down_task_dict
            ]
        )

        return TaskDependenciesResponse(up=up, down=down)

    def _get_dag_and_wf_id(self, task_id: int) -> tuple:
        """Get DAG ID, workflow ID, and node ID for a task."""
        select_stmt = (
            select(
                Workflow.dag_id.label("dag_id"),
                Task.workflow_id.label("workflow_id"),
                Task.node_id.label("node_id"),
            )
            .join_from(Task, Workflow, Task.workflow_id == Workflow.id)
            .where(Task.id == task_id)
        )
        row = self.session.execute(select_stmt).one_or_none()

        if row is None:
            return None, None, None
        return int(row.dag_id), int(row.workflow_id), int(row.node_id)

    def get_task_resource_usage(self, task_id: int) -> TaskResourceUsageResponse:
        """Return the resource usage for a given Task ID."""
        # Select the fields required by SerializeTaskResourceUsage.to_wire
        select_stmt = (
            select(
                Task.num_attempts,
                TaskInstance.nodename,
                TaskInstance.wallclock,
                TaskInstance.maxrss,
            )
            .join_from(
                TaskInstance,
                Task,  # Join Task table
                TaskInstance.task_id == Task.id,
            )
            .where(TaskInstance.task_id == task_id, TaskInstance.status == "D")
        )
        result = self.session.execute(select_stmt).one_or_none()

        if result is None:
            resource_usage = SerializeTaskResourceUsage.to_wire(None, None, None, None)
        else:
            resource_usage = SerializeTaskResourceUsage.to_wire(
                result.num_attempts,
                result.nodename,
                result.wallclock,
                result.maxrss,
            )

        return TaskResourceUsageResponse(resource_usage=list(resource_usage))

    def get_downstream_tasks(
        self, task_ids: List[int], dag_id: int, client_version: Optional[str] = None
    ) -> DownstreamTasksResponse:
        """Get only the direct downstreams of a task."""
        from jobmon.server.web.utils.json_compat import normalize_node_ids_for_client

        tasks_and_edges = self.session.execute(
            select(Task.id, Task.node_id, Edge.downstream_node_ids).where(
                Task.id.in_(task_ids),
                Task.node_id == Edge.node_id,
                Edge.dag_id == dag_id,
            )
        ).all()

        result = {}
        for row in tasks_and_edges:
            # Format downstream_node_ids based on client version
            formatted_downstream_ids = normalize_node_ids_for_client(
                row.downstream_node_ids, client_version
            )
            result[row.id] = [row.node_id, formatted_downstream_ids]

        return DownstreamTasksResponse(downstream_tasks=result)

    def get_task_instance_details(self, task_id: int) -> TaskInstanceDetailsResponse:
        """Get information about TaskInstances associated with specific Task ID."""
        query = (
            select(
                TaskInstance.id,
                TaskInstanceStatus.label,
                TaskInstance.stdout,
                TaskInstance.stderr,
                TaskInstance.stdout_log,
                TaskInstance.stderr_log,
                TaskInstance.distributor_id,
                TaskInstance.nodename,
                TaskInstanceErrorLog.description,
                TaskInstance.wallclock,
                TaskInstance.maxrss,
                TaskResources.requested_resources,
                TaskInstance.submitted_date,
                TaskInstance.status_date,
                Queue.name,
            )
            .outerjoin_from(
                TaskInstance,
                TaskInstanceErrorLog,
                TaskInstance.id == TaskInstanceErrorLog.task_instance_id,
            )
            .join(
                TaskResources,
                TaskInstance.task_resources_id == TaskResources.id,
            )
            .join(
                Queue,
                TaskResources.queue_id == Queue.id,
            )
            .where(
                TaskInstance.task_id == task_id,
                TaskInstance.status == TaskInstanceStatus.id,
            )
        )
        rows = self.session.execute(query).all()

        def serialize_datetime(dt: Any) -> str:
            if isinstance(dt, datetime):
                return dt.isoformat()
            return dt

        result = [
            TaskInstanceDetailItem(
                ti_id=row[0],
                ti_status=row[1],
                ti_stdout=row[2],
                ti_stderr=row[3],
                ti_stdout_log=row[4],
                ti_stderr_log=row[5],
                ti_distributor_id=row[6],
                ti_nodename=row[7],
                ti_error_log_description=row[8],
                ti_wallclock=row[9],
                ti_maxrss=row[10],
                ti_resources=row[11],
                ti_submit_date=serialize_datetime(row[12]),
                ti_status_date=serialize_datetime(row[13]),
                ti_queue_name=row[14],
            )
            for row in rows
        ]

        return TaskInstanceDetailsResponse(taskinstances=result)

    def get_task_details_viz(self, task_id: int) -> TaskDetailsResponse:
        """Get status of Task from Task ID."""
        query = (
            select(
                Task.status,
                Task.workflow_id,
                Task.name,
                Task.command,
                Task.status_date,
                TaskTemplate.id,
            )
            .join(Node, Task.node_id == Node.id)
            .join(
                TaskTemplateVersion,
                Node.task_template_version_id == TaskTemplateVersion.id,
            )
            .join(
                TaskTemplate,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            )
            .where(Task.id == task_id)
        )
        rows = self.session.execute(query).all()

        result = []
        for row in rows:
            status_date = row[4].isoformat() if isinstance(row[4], datetime) else row[4]
            result.append(
                TaskDetailItem(
                    task_status=row[0],
                    workflow_id=row[1],
                    task_name=row[2],
                    task_command=row[3],
                    task_status_date=status_date,
                    task_template_id=row[5],
                )
            )

        return TaskDetailsResponse(task_details=result)

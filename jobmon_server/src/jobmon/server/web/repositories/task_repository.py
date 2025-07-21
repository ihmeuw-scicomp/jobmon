"""Repository for Task operations."""

import json
from typing import List, Optional, Set, Union

import structlog
from sqlalchemy import and_, select, update
from sqlalchemy.orm import Session

from jobmon.core import constants
from jobmon.core.constants import Direction
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.server_side_exception import InvalidUsage
from jobmon.server.web.utils.json_utils import parse_node_ids

logger = structlog.get_logger(__name__)


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
            - When task_ids='all', it updates all tasks in the workflow with
            recursive=False. This improves performance.
            - When recursive=True, it updates the tasks and it's dependencies all
            the way up or down the DAG.
            - When recursive=False, it updates only the tasks in the task_ids list.
            - When workflow_status is None, it gets the workflow status from the db.
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
        self._update_task_statuses_in_db(task_ids, new_status)

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

    def _update_task_statuses_in_db(self, task_ids: List[int], new_status: str) -> None:
        """Update task statuses in the database."""
        update_stmt = update(Task).where(
            and_(Task.id.in_(task_ids), Task.status != new_status)
        )
        vals = {"status": new_status}
        self.session.execute(update_stmt.values(**vals))
        self.session.flush()

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
                upstreams = parse_node_ids(edges.upstream_node_ids)
                if upstreams:
                    node_ids.update(upstreams)
            elif direction == Direction.DOWN:
                downstreams = parse_node_ids(edges.downstream_node_ids)
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

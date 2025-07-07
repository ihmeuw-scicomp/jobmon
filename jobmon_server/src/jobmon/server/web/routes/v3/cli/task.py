"""Routes for Tasks."""

import json
from collections import defaultdict
from datetime import datetime
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Optional, Set, Union, cast

import pandas as pd
import structlog
from fastapi import Query, Request
from sqlalchemy import and_, select, update
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse

from jobmon.core import constants
from jobmon.core.constants import Direction
from jobmon.core.serializers import SerializeTaskResourceUsage
from jobmon.server.web.db import get_sessionmaker
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
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)
SessionMaker = get_sessionmaker()

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


@api_v3_router.get("/task_status")
def get_task_status(
    task_ids: Optional[Union[int, list[int]]] = Query(...),
    status: Optional[Union[str, list[str]]] = Query(None),
) -> Any:
    """Get the status of a task."""
    logger.info(f"task_ids: {task_ids}, status_request: {status}")
    if task_ids is None:
        raise InvalidUsage("Missing task_ids in request", status_code=400)

    if isinstance(task_ids, int):
        task_ids = [task_ids]

    if len(task_ids) == 0:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)

    if status and isinstance(status, str):
        status = [status]

    with SessionMaker() as session:
        with session.begin():
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
            rows = session.execute(sql).all()

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
                resp = JSONResponse(
                    content={"task_instance_status": df.to_json()},
                    status_code=StatusCodes.OK,
                )
            else:
                df = pd.DataFrame({}, columns=column_names)
                resp = JSONResponse(
                    content={"task_instance_status": df.to_json()},
                    status_code=StatusCodes.OK,
                )

    return resp


@api_v3_router.post("/task/subdag")
async def get_task_subdag(request: Request) -> Any:
    """Used to get the sub dag  of a given task.

    It returns a list of sub tasks as well as a list of sub nodes.
    """
    # Only return sub tasks in the following status. If empty or None, return all
    data = cast(Dict, await request.json())
    task_ids = data.get("task_ids", [])
    task_status = data.get("task_status", [])

    if not task_ids:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)
    if task_status is None:
        task_status = []
    with SessionMaker() as session:
        with session.begin():
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
            grouped_data: Dict = dict()
            grouped_data = defaultdict(
                lambda: {"workflow_id": None, "dag_id": None, "node_ids": []}
            )

            for row in session.execute(select_stmt):
                key = (
                    row.workflow_id,
                    row.dag_id,
                )  # Assuming this combination is unique for each group
                grouped_data[key]["workflow_id"] = row.workflow_id
                grouped_data[key]["dag_id"] = row.dag_id
                if grouped_data[key]:
                    grouped_data[key]["node_ids"].append(row.node_id)

            # If we find no results, we handle it here
            if not grouped_data:
                resp = JSONResponse(
                    content={"workflow_id": None, "sub_task": None},
                    status_code=StatusCodes.OK,
                )
                return resp

            # Since we have validated all the tasks belong to the same wf in status_command
            # before this call, assume they all belong to the same wf.
            if grouped_data:
                some_key = next(iter(grouped_data))
                workflow_id, dag_id = some_key
                node_ids = [
                    int(node_id) for node_id in grouped_data[some_key]["node_ids"]
                ]

                # Continue with your current processing logic
                sub_dag_tree = _get_subdag(node_ids, dag_id, session)
                sub_task_tree = _get_tasks_from_nodes(
                    workflow_id, sub_dag_tree, task_status, session
                )

        resp = JSONResponse(
            content={"workflow_id": workflow_id, "sub_task": sub_task_tree},
            status_code=StatusCodes.OK,
        )
    return resp


@api_v3_router.put("/task/update_statuses")
async def update_task_statuses(request: Request) -> Any:
    """Update the status of the tasks.

    Description:
        - When workflow_id='all', it updates all tasks in the workflow with
        recursive=False. This improves performance.
        - When recursive=True, it updates the tasks and it's dependencies all
        the way up or down the DAG.
        - When recursive=False, it updates only the tasks in the task_ids list.
        - When workflow_status is None, it gets the workflow status from the db.
        - After updating the tasks, it checks the workflow status and updates it.

    Notes:
        This API is different from v2.
        It integrated the logic in update_task_status from status_commands.py.
    TODO:
    - Once CLI moves to v3, simplify update_task_status to avoid duplication.
    """
    data = cast(Dict, await request.json())
    try:
        # workflow_id is required; if not set, raise KeyError
        workflow_id = data["workflow_id"]
        # get the recursive flag, if not provided, default to False
        recursive = data.get("recursive", False)
        # get workflow_status, if not provided, get it from db
        workflow_status = data.get("workflow_status", None)
        # task_ids is required; if not set, raise KeyError;
        # if it is an int, convert it to a list
        # if it is "all", get all tasks from the workflow_id, and disable recursive
        task_ids = data["task_ids"]
        if isinstance(task_ids, int):
            task_ids = [task_ids]
        if task_ids == "all":
            recursive = False
        # new_status is required; if not set, raise KeyError
        new_status = data["new_status"]

    except KeyError as e:
        raise InvalidUsage(
            f"problem with {str(e)} in request to {request.url.path}", status_code=400
        ) from e

    with SessionMaker() as session:
        with session.begin():
            # if task_ids is "all", get all tasks from the workflow_id
            if task_ids == "all":
                task_ids = (
                    session.query(Task.id).filter(Task.workflow_id == workflow_id).all()
                )
                task_ids = [task_id for task_id, in task_ids]
            # If recursive is True, appends all dependent task_ids
            # (upstream if new_status == 'D'; downstream if new_status == 'G').
            if recursive:
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
                task_ids = _get_tasks_recursive(set(task_ids), direction, session)

                logger.info(f"reset status to new_status: {new_status}")

            update_stmt = update(Task).where(
                and_(Task.id.in_(task_ids), Task.status != new_status)
            )
            vals = {"status": new_status}
            session.execute(update_stmt.values(**vals))
            session.flush()
            # If job is supposed to be rerun, set task instances to "K"
            if new_status == constants.TaskStatus.REGISTERING:
                task_instance_update_stmt = update(TaskInstance).where(
                    TaskInstance.task_id.in_(task_ids),
                    TaskInstance.status.notin_(
                        [
                            constants.TaskInstanceStatus.ERROR_FATAL,
                            constants.TaskInstanceStatus.DONE,
                            constants.TaskInstanceStatus.ERROR,
                            constants.TaskInstanceStatus.UNKNOWN_ERROR,
                            constants.TaskInstanceStatus.RESOURCE_ERROR,
                            constants.TaskInstanceStatus.KILL_SELF,
                            constants.TaskInstanceStatus.NO_DISTRIBUTOR_ID,
                        ]
                    ),
                )
                vals = {"status": constants.TaskInstanceStatus.KILL_SELF}
                session.execute(task_instance_update_stmt.values(**vals))
                session.flush()
                # if workflow status is None, get workflow status from db
                if workflow_status is None:
                    workflow_status = (
                        session.query(Workflow.status)
                        .filter(Workflow.id == workflow_id)
                        .scalar()
                    )
                # If workflow is done, need to set it to an error state before resuming
                if workflow_status == constants.WorkflowStatus.DONE:
                    logger.info(f"reset workflow status for workflow_id: {workflow_id}")
                    workflow_update_stmt = update(Workflow).where(
                        Workflow.id == workflow_id
                    )
                    vals = {"status": constants.WorkflowStatus.FAILED}
                    session.execute(workflow_update_stmt.values(**vals))
            # If new_status is "D", check if all the tasks in the wf are done, and set wf.
            if new_status == constants.TaskStatus.DONE:
                tasks_done = (
                    session.query(Task.id)
                    .filter(Task.workflow_id == workflow_id, Task.status != new_status)
                    .all()
                )
                if not tasks_done:
                    logger.info(
                        f"set workflow status to DONE for workflow_id: {workflow_id}"
                    )
                    workflow_update_stmt = update(Workflow).where(
                        Workflow.id == workflow_id
                    )
                    vals = {"status": constants.WorkflowStatus.DONE}
                    session.execute(workflow_update_stmt.values(**vals))

        message = f"updated to status {new_status}"
        resp = JSONResponse(content={"message": message}, status_code=StatusCodes.OK)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "PUT, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp


@api_v3_router.get("/task_dependencies/{task_id}")
def get_task_dependencies(task_id: int) -> Any:
    """Get task's downstream and upstream tasks and their status."""
    with SessionMaker() as session:
        with session.begin():
            dag_id, workflow_id, node_id = _get_dag_and_wf_id(task_id, session)
            up_nodes = _get_node_dependencies({node_id}, dag_id, session, Direction.UP)
            down_nodes = _get_node_dependencies(
                {node_id}, dag_id, session, Direction.DOWN
            )
            up_task_dict = _get_tasks_from_nodes(
                workflow_id, list(up_nodes), [], session
            )
            down_task_dict = _get_tasks_from_nodes(
                workflow_id, list(down_nodes), [], session
            )
            print(up_nodes, down_nodes, up_task_dict, down_task_dict)

            # return a "standard" json format so that it can be reused by future GUI
            up = (
                []
                if up_task_dict is None or len(up_task_dict) == 0
                else [
                    [
                        {
                            "id": k,
                            "status": up_task_dict[k][0],
                            "name": up_task_dict[k][1],
                        }
                    ]
                    for k in up_task_dict
                ]
            )
            down = (
                []
                if down_task_dict is None or len(down_task_dict) == 0
                else [
                    [
                        {
                            "id": k,
                            "status": down_task_dict[k][0],
                            "name": down_task_dict[k][1],
                        }
                    ]
                    for k in down_task_dict
                ]
            )

        resp = JSONResponse(
            content={"up": up, "down": down}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.put("/tasks_recursive/{direction}")
async def get_tasks_recursive(direction: str, request: Request) -> Any:
    """Get all input task_ids'.

    Either downstream or upsteam tasks based on direction;
    return all recursive(including input set) task_ids in the defined direction.
    """
    direct = constants.Direction.UP if direction == "up" else constants.Direction.DOWN
    data = await request.json()
    # define task_ids as set in order to eliminate dups
    task_ids = set(data.get("task_ids", []))

    try:
        with SessionMaker() as session:
            with session.begin():
                tasks_recursive = _get_tasks_recursive(task_ids, direct, session)
            resp = JSONResponse(
                content={"task_ids": list(tasks_recursive)}, status_code=StatusCodes.OK
            )
        return resp
    except InvalidUsage as e:
        raise e


@api_v3_router.get("/task_resource_usage")
def get_task_resource_usage(task_id: int) -> Any:
    """Return the resource usage for a given Task ID."""
    with SessionMaker() as session:
        with session.begin():
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
            result = session.execute(select_stmt).one_or_none()

            if result is None:
                resource_usage = SerializeTaskResourceUsage.to_wire(
                    None, None, None, None
                )
            else:
                resource_usage = SerializeTaskResourceUsage.to_wire(
                    result.num_attempts,
                    result.nodename,
                    result.wallclock,
                    result.maxrss,
                )

        resp = JSONResponse(
            content={"resource_usage": resource_usage},
            status_code=StatusCodes.OK,
        )
    return resp


def _get_tasks_recursive(
    task_ids: Set[int], direction: Direction, session: Session
) -> Set[int]:
    """Get all task IDs connected in the specified direction iteratively.

    Starting with the given task_ids, the function traverses the dependency graph
    and returns all tasks found, including the input set. It also verifies that all
    tasks belong to the same workflow.

    Args:
        task_ids (Set[int]): Initial set of task IDs.
        direction (Direction): Either Direction.UP or Direction.DOWN.
        session (Session): SQLAlchemy session for database operations.

    Returns:
        Set[int]: The complete set of task IDs connected in the specified direction.
    """
    # recusrive on nodes instead of tasks, which is more efficient

    # make sure all tasks belong to the same workflow
    distinct_workflow_ids = (
        session.query(Task.workflow_id).filter(Task.id.in_(task_ids)).distinct().all()
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
    dag_id = session.query(Workflow.dag_id).filter(Workflow.id == workflow_id).scalar()

    # get the node_ids of the task_ids
    rows = session.query(Task.node_id).filter(Task.id.in_(task_ids)).all()
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
        node_deps = _get_node_dependencies(
            {current_node},
            dag_id,
            session,
            Direction.DOWN if direction == constants.Direction.DOWN else Direction.UP,
        )
        if node_deps:
            # Add the node dependencies to the stack for further processing.
            stack.extend(list(node_deps))
    # get task_ids from node_ids
    tasks_recursive = _get_tasks_from_nodes(
        workflow_id, list(nodes_recursive), [], session
    )
    return set(tasks_recursive.keys())


def _get_dag_and_wf_id(task_id: int, session: Session) -> tuple:
    select_stmt = (
        select(
            Workflow.dag_id.label("dag_id"),
            Task.workflow_id.label("workflow_id"),
            Task.node_id.label("node_id"),
        )
        .join_from(Task, Workflow, Task.workflow_id == Workflow.id)
        .where(Task.id == task_id)
    )
    row = session.execute(select_stmt).one_or_none()

    if row is None:
        return None, None, None
    return int(row.dag_id), int(row.workflow_id), int(row.node_id)


def _get_node_dependencies(
    nodes: set, dag_id: int, session: Session, direction: Direction
) -> Set[int]:
    """Get all upstream or downstream nodes of a node.

    Args:
        nodes (set): set of nodes
        dag_id (int): ID of DAG
        session (Session): SQLAlchemy session
        direction (Direction): either up or down
    """
    select_stmt = select(Edge).where(
        Edge.dag_id == int(dag_id), Edge.node_id.in_(list(nodes))
    )
    node_ids: Set[int] = set()
    for row in session.execute(select_stmt).all():
        edges = row[0]
        if direction == Direction.UP:
            upstreams = (
                json.loads(edges.upstream_node_ids)
                if isinstance(edges.upstream_node_ids, str)
                else edges.upstream_node_ids
            )
            if upstreams:
                node_ids.update(upstreams)
        elif direction == Direction.DOWN:
            downstreams = (
                json.loads(edges.downstream_node_ids)
                if isinstance(edges.downstream_node_ids, str)
                else edges.downstream_node_ids
            )
            if downstreams:
                node_ids.update(downstreams)
        else:
            raise ValueError(f"Invalid direction type. Expected one of: {Direction}")
    return node_ids


def _get_subdag(node_ids: list, dag_id: int, session: Session) -> list:
    """Get all descendants of a given nodes.

    It only queries the primary keys on the edge table without join.

    Args:
        node_ids (list): list of node IDs
        dag_id (int): ID of DAG
        session (Session): SQLAlchemy sessions
    """
    node_set = set(node_ids)
    node_descendants = node_set
    while len(node_descendants) > 0:
        node_descendants = _get_node_dependencies(
            node_descendants, dag_id, session, Direction.DOWN
        )
        node_set.update(node_descendants)
    return list(node_set)


def _get_tasks_from_nodes(
    workflow_id: int, nodes: List, task_status: List, session: Session
) -> dict:
    """Get task ids of the given node ids.

    Args:
        workflow_id (int): ID of the workflow
        nodes (list): list of nodes
        task_status (list): list of task statuses
        session (Session): SQLAlchemy session
    """
    if not nodes:
        return {}

    select_stmt = select(Task.id, Task.status, Task.name).where(
        Task.workflow_id == workflow_id, Task.node_id.in_(list(nodes))
    )

    result = session.execute(select_stmt).all()
    task_dict = {}
    for r in result:
        # When task_status not specified, return the full subdag
        if not task_status:
            task_dict[r[0]] = [r[1], r[2]]
        else:
            if r[1] in task_status:
                task_dict[r[0]] = [r[1], r[2]]
    return task_dict


@api_v3_router.post("/task/get_downstream_tasks")
async def get_downstream_tasks(request: Request) -> Any:
    """Get only the direct downstreams of a task."""
    data = cast(Dict, await request.json())

    task_ids = data["task_ids"]
    dag_id = data["dag_id"]
    with SessionMaker() as session:
        with session.begin():
            tasks_and_edges = session.execute(
                select(Task.id, Task.node_id, Edge.downstream_node_ids).where(
                    Task.id.in_(task_ids),
                    Task.node_id == Edge.node_id,
                    Edge.dag_id == dag_id,
                )
            ).all()
            result = {
                row.id: [row.node_id, row.downstream_node_ids]
                for row in tasks_and_edges
            }

        resp = JSONResponse(
            content={"downstream_tasks": result}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.get("/task/get_ti_details_viz/{task_id}")
def get_task_details(task_id: int) -> Any:
    """Get information about TaskInstances associated with specific Task ID."""
    with SessionMaker() as session:
        with session.begin():
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
            rows = session.execute(query).all()

        column_names = (
            "ti_id",
            "ti_status",
            "ti_stdout",
            "ti_stderr",
            "ti_stdout_log",
            "ti_stderr_log",
            "ti_distributor_id",
            "ti_nodename",
            "ti_error_log_description",
            "ti_wallclock",
            "ti_maxrss",
            "ti_resources",
            "ti_submit_date",
            "ti_status_date",
            "ti_queue_name",
        )

        def serialize_row(row: Any) -> Dict[str, Any]:
            row_dict = dict(zip(column_names, row))
            for key in ("ti_submit_date", "ti_status_date"):
                if isinstance(row_dict[key], datetime):
                    row_dict[key] = row_dict[key].isoformat()
            return row_dict

        result = [serialize_row(row) for row in rows]

        resp = JSONResponse(
            content={"taskinstances": result}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.get("/task/get_task_details_viz/{task_id}")
def get_task_details_viz(task_id: int) -> Any:
    """Get status of Task from Task ID."""
    with SessionMaker() as session:
        with session.begin():
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
            rows = session.execute(query).all()

        column_names = (
            "task_status",
            "workflow_id",
            "task_name",
            "task_command",
            "task_status_date",
            "task_template_id",
        )
        result = [dict(zip(column_names, row)) for row in rows]

        if result and "task_status_date" in result[0]:
            result[0]["task_status_date"] = result[0]["task_status_date"].isoformat()

        resp = JSONResponse(
            content={"task_details": result}, status_code=StatusCodes.OK
        )
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
    return resp

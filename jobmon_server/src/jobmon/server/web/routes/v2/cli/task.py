"""Routes for Tasks."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
import json
from typing import Any, cast, Dict, List, Set

from flask import jsonify, request
from flask_cors import cross_origin
import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session
import structlog

from jobmon.core import constants
from jobmon.core.constants import Direction
from jobmon.core.serializers import SerializeTaskResourceUsage
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.routes.v1 import api_v1_blueprint
from jobmon.server.web.routes.v2 import api_v2_blueprint
from jobmon.server.web.routes.v2 import SessionLocal
from jobmon.server.web.server_side_exception import InvalidUsage

# new structlog logger per flask request context. internally stored as flask.g.logger
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


@api_v1_blueprint.route("/task_status", methods=["GET"])
@api_v2_blueprint.route("/task_status", methods=["GET"])
def get_task_status() -> Any:
    """Get the status of a task."""
    task_ids = request.args.getlist("task_ids")
    if len(task_ids) == 0:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)

    # status is an optional arg
    status_request = request.args.getlist("status", None)

    session = SessionLocal()
    with session.begin():
        query_filter = [
            Task.id == TaskInstance.task_id,
            TaskInstanceStatus.id == TaskInstance.status,
        ]
        if status_request:
            if len(status_request) > 0:
                status_codes = [
                    i
                    for arg in status_request
                    for i in _reversed_task_instance_label_mapping[arg]
                ]
            query_filter.append(
                TaskInstance.status.in_(
                    [i for arg in status_request for i in status_codes]
                )
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
        resp = jsonify(task_instance_status=df.to_json())
    else:
        df = pd.DataFrame({}, columns=column_names)
        resp = jsonify(task_instance_status=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


@api_v1_blueprint.route("/task/subdag", methods=["POST"])
@api_v2_blueprint.route("/task/subdag", methods=["POST"])
def get_task_subdag() -> Any:
    """Used to get the sub dag  of a given task.

    It returns a list of sub tasks as well as a list of sub nodes.
    """
    # Only return sub tasks in the following status. If empty or None, return all
    data = cast(Dict, request.get_json())
    task_ids = data.get("task_ids", [])
    task_status = data.get("task_status", [])

    if not task_ids:
        raise InvalidUsage(f"Missing {task_ids} in request", status_code=400)
    if task_status is None:
        task_status = []
    session = SessionLocal()
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
            resp = jsonify(workflow_id=None, sub_task=None)
            resp.status_code = StatusCodes.OK
            return resp

        # Since we have validated all the tasks belong to the same wf in status_command before
        # this call, assume they all belong to the same wf.
        if grouped_data:
            some_key = next(iter(grouped_data))
            workflow_id, dag_id = some_key
            node_ids = [int(node_id) for node_id in grouped_data[some_key]["node_ids"]]

            # Continue with your current processing logic
            sub_dag_tree = _get_subdag(node_ids, dag_id, session)
            sub_task_tree = _get_tasks_from_nodes(
                workflow_id, sub_dag_tree, task_status, session
            )

    resp = jsonify(workflow_id=workflow_id, sub_task=sub_task_tree)
    resp.status_code = StatusCodes.OK
    return resp


@api_v1_blueprint.route("/task/update_statuses", methods=["PUT"])
@api_v2_blueprint.route("/task/update_statuses", methods=["PUT"])
def update_task_statuses() -> Any:
    """Update the status of the tasks."""
    data = cast(Dict, request.get_json())
    try:
        task_ids = data["task_ids"]
        new_status = data["new_status"]
        workflow_status = data["workflow_status"]
        workflow_id = data["workflow_id"]
    except KeyError as e:
        raise InvalidUsage(
            f"problem with {str(e)} in request to {request.path}", status_code=400
        ) from e

    session = SessionLocal()
    with SessionLocal.begin():
        try:
            update_stmt = update(Task).where(
                Task.id.in_(task_ids), Task.status != new_status
            )
            vals = {"status": new_status}
            session.execute(update_stmt.values(**vals))

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

                # If workflow is done, need to set it to an error state before resume
                if workflow_status == constants.WorkflowStatus.DONE:
                    workflow_update_stmt = update(Workflow).where(
                        Workflow.id == workflow_id
                    )
                    vals = {"status": constants.WorkflowStatus.FAILED}
                    session.execute(workflow_update_stmt.values(**vals))

            session.commit()
        except KeyError as e:
            session.rollback()
            raise InvalidUsage(
                f"{str(e)} in request to {request.path}", status_code=400
            ) from e

    message = f"updated to status {new_status}"
    resp = jsonify(message)
    resp.status_code = StatusCodes.OK
    return resp


@api_v1_blueprint.route("/task_dependencies/<task_id>", methods=["GET"])
@api_v2_blueprint.route("/task_dependencies/<task_id>", methods=["GET"])
@cross_origin()
def get_task_dependencies(task_id: int) -> Any:
    """Get task's downstream and upstream tasks and their status."""
    session = SessionLocal()
    with session.begin():
        dag_id, workflow_id, node_id = _get_dag_and_wf_id(task_id, session)
        logger.info(f"task_id: {task_id}, dag_id: {dag_id}, workflow_id: {workflow_id}")
        up_nodes = _get_node_dependencies({node_id}, dag_id, session, Direction.UP)
        down_nodes = _get_node_dependencies({node_id}, dag_id, session, Direction.DOWN)
        up_task_dict = _get_tasks_from_nodes(workflow_id, list(up_nodes), [], session)
        down_task_dict = _get_tasks_from_nodes(
            workflow_id, list(down_nodes), [], session
        )
    print(up_nodes, down_nodes, up_task_dict, down_task_dict)

    # return a "standard" json format so that it can be reused by future GUI
    up = (
        []
        if up_task_dict is None or len(up_task_dict) == 0
        else [
            [{"id": k, "status": up_task_dict[k][0], "name": up_task_dict[k][1]}]
            for k in up_task_dict
        ]
    )
    down = (
        []
        if down_task_dict is None or len(down_task_dict) == 0
        else [
            [{"id": k, "status": down_task_dict[k][0], "name": down_task_dict[k][1]}]
            for k in down_task_dict
        ]
    )
    resp = jsonify({"up": up, "down": down})
    resp.status_code = 200
    return resp


@api_v1_blueprint.route("/tasks_recursive/<direction>", methods=["PUT"])
@api_v2_blueprint.route("/tasks_recursive/<direction>", methods=["PUT"])
def get_tasks_recursive(direction: str) -> Any:
    """Get all input task_ids'.

    Either downstream or upsteam tasks based on direction;
    return all recursive(including input set) task_ids in the defined direction.
    """
    direct = constants.Direction.UP if direction == "up" else constants.Direction.DOWN
    data = request.get_json()
    # define task_ids as set in order to eliminate dups
    task_ids = set(data.get("task_ids", []))

    try:
        session = SessionLocal()
        with session.begin():
            tasks_recursive = _get_tasks_recursive(task_ids, direct, session)
        resp = jsonify({"task_ids": list(tasks_recursive)})
        resp.status_code = 200
        return resp
    except InvalidUsage as e:
        raise e


@api_v1_blueprint.route("/task_resource_usage", methods=["GET"])
@api_v2_blueprint.route("/task_resource_usage", methods=["GET"])
def get_task_resource_usage() -> Any:
    """Return the resource usage for a given Task ID."""
    try:
        task_id = int(request.args["task_id"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to /task_resource_usage", status_code=400
        ) from e

    session = SessionLocal()
    with SessionLocal.begin():
        select_stmt = (
            select(
                Task.num_attempts,
                TaskInstance.nodename,
                TaskInstance.wallclock,
                TaskInstance.maxpss,
            )
            .join_from(Task, TaskInstance, Task.id == TaskInstance.task_id)
            .where(
                TaskInstance.task_id == task_id,
                TaskInstance.status == constants.TaskInstanceStatus.DONE,
            )
        )
        result = session.execute(select_stmt).one_or_none()

        if result is None:
            resource_usage = SerializeTaskResourceUsage.to_wire(None, None, None, None)
        else:
            resource_usage = SerializeTaskResourceUsage.to_wire(
                result.num_attempts, result.nodename, result.wallclock, result.maxpss
            )

    resp = jsonify(resource_usage)
    resp.status_code = StatusCodes.OK
    return resp


def _get_tasks_recursive(
    task_ids: Set[int], direction: Direction, session: Session
) -> set:
    """Get all input task_ids'.

    Either downstream or upsteam tasks based on direction;
    return all recursive(including input set) task_ids in the defined direction.
    """
    tasks_recursive = set()
    next_nodes = set()
    _workflow_id_first = None
    for task_id in task_ids:
        dag_id, workflow_id, node_id = _get_dag_and_wf_id(task_id, session)
        next_nodes_sub = (
            _get_node_dependencies({node_id}, dag_id, session, Direction.DOWN)
            if direction == constants.Direction.DOWN
            else _get_node_dependencies({node_id}, dag_id, session, Direction.UP)
        )
        if _workflow_id_first is None:
            workflow_id_first = workflow_id
        elif workflow_id != workflow_id_first:
            raise InvalidUsage(
                f"{task_ids} in request belong to different workflow_ids"
                f"({workflow_id_first}, {workflow_id})",
                status_code=400,
            )
        next_nodes.update(next_nodes_sub)

    if len(next_nodes) > 0:
        next_task_dict = _get_tasks_from_nodes(
            workflow_id_first, list(next_nodes), [], session
        )
        if len(next_task_dict) > 0:
            task_recursive_sub = _get_tasks_recursive(
                set(next_task_dict.keys()), direction, session
            )
            tasks_recursive.update(task_recursive_sub)

    tasks_recursive.update(task_ids)

    return tasks_recursive


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
    """Get all upstream nodes of a node.

    Args:
        nodes (set): set of nodes
        dag_id (int): ID of DAG
        session (Session): SQLAlchemy session
        direction (Direction): either up or down
    """
    select_stmt = select(Edge).where(
        Edge.dag_id == dag_id, Edge.node_id.in_(list(nodes))
    )
    node_ids: Set[int] = set()
    for (edges,) in session.execute(select_stmt).all():
        if direction == Direction.UP:
            upstreams = (
                json.loads(edges.upstream_node_ids)
                if isinstance(edges.upstream_node_ids, str)
                else edges.upstream_node_ids
            )
            if upstreams:
                node_ids = node_ids.union(set(upstreams))
        elif direction == Direction.DOWN:
            downstreams = (
                json.loads(edges.downstream_node_ids)
                if isinstance(edges.downstream_node_ids, str)
                else edges.downstream_node_ids
            )
            if downstreams:
                node_ids = node_ids.union(set(downstreams))
        else:
            raise ValueError(f"Invalid direction type. Expected one of: {Direction}")
        print(node_ids)
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
        node_set = node_set.union(node_descendants)
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


@api_v1_blueprint.route("/task/get_downstream_tasks", methods=["POST"])
@api_v2_blueprint.route("/task/get_downstream_tasks", methods=["POST"])
def get_downstream_tasks() -> Any:
    """Get only the direct downstreams of a task."""
    data = cast(Dict, request.get_json())

    task_ids = data["task_ids"]
    dag_id = data["dag_id"]
    session = SessionLocal()
    with session.begin():
        tasks_and_edges = session.execute(
            select(Task.id, Task.node_id, Edge.downstream_node_ids).where(
                Task.id.in_(task_ids),
                Task.node_id == Edge.node_id,
                Edge.dag_id == dag_id,
            )
        ).all()
        result = {
            row.id: [row.node_id, row.downstream_node_ids] for row in tasks_and_edges
        }

    resp = jsonify(downstream_tasks=result)
    resp.status_code = StatusCodes.OK
    return resp


@api_v1_blueprint.route("/task/get_ti_details_viz/<task_id>", methods=["GET"])
@api_v2_blueprint.route("/task/get_ti_details_viz/<task_id>", methods=["GET"])
@cross_origin()
def get_task_details(task_id: int) -> Any:
    """Get information about TaskInstances associated with specific Task ID."""
    session = SessionLocal()
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
    )
    result = [dict(zip(column_names, row)) for row in rows]
    resp = jsonify(taskinstances=result)
    resp.status_code = 200
    return resp


@api_v1_blueprint.route("/task/get_task_details_viz/<task_id>", methods=["GET"])
@api_v2_blueprint.route("/task/get_task_details_viz/<task_id>", methods=["GET"])
@cross_origin()
def get_task_details_viz(task_id: int) -> Any:
    """Get status of Task from Task ID."""
    session = SessionLocal()
    with session.begin():
        query = select(
            Task.status,
            Task.workflow_id,
            Task.name,
            Task.command,
            Task.status_date,
        ).where(
            Task.id == task_id,
        )
        rows = session.execute(query).all()

    column_names = (
        "task_status",
        "workflow_id",
        "task_name",
        "task_command",
        "task_status_date",
    )
    result = [dict(zip(column_names, row)) for row in rows]
    resp = jsonify(task_details=result)
    resp.status_code = 200
    return resp

"""Routes for Workflows."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict, List, Tuple

from flask import jsonify, request
import sqlalchemy
from sqlalchemy import func, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
import structlog

from jobmon.server.web.models.array import Array
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import InvalidUsage


logger = structlog.get_logger(__name__)


def _add_workflow_attributes(
    workflow_id: int, workflow_attributes: Dict[str, str], session: Session
) -> None:
    # add attribute
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    logger.info(f"Add Attributes: {workflow_attributes}")
    wf_attributes_list = []
    with session.begin_nested():
        for name, val in workflow_attributes.items():
            wf_type_id = _add_or_get_wf_attribute_type(name, session)
            wf_attribute = WorkflowAttribute(
                workflow_id=workflow_id,
                workflow_attribute_type_id=wf_type_id,
                value=val,
            )
            wf_attributes_list.append(wf_attribute)
            logger.debug(f"Attribute name: {name}, value: {val}")
        session.add_all(wf_attributes_list)


@blueprint.route("/workflow", methods=["POST"])
def bind_workflow() -> Any:
    """Bind a workflow to the database."""
    try:
        data = cast(Dict, request.get_json())
        tv_id = int(data["tool_version_id"])
        dag_id = int(data["dag_id"])
        whash = str(data["workflow_args_hash"])
        thash = str(data["task_hash"])
        description = data["description"]
        name = data["name"]
        workflow_args = data["workflow_args"]
        max_concurrently_running = data["max_concurrently_running"]
        workflow_attributes = data["workflow_attributes"]

    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    structlog.contextvars.bind_contextvars(
        dag_id=dag_id,
        tool_version_id=tv_id,
        workflow_args_hash=str(whash),
        task_hash=str(thash),
    )
    logger.info("Bind workflow")
    session = SessionLocal()
    with session.begin():
        select_stmt = select(Workflow).where(
            Workflow.tool_version_id == tv_id,
            Workflow.dag_id == dag_id,
            Workflow.workflow_args_hash == whash,
            Workflow.task_hash == thash,
        )
        workflow = session.execute(select_stmt).scalars().one_or_none()
        if workflow is None:
            # create a new workflow
            workflow = Workflow(
                tool_version_id=tv_id,
                dag_id=dag_id,
                workflow_args_hash=whash,
                task_hash=thash,
                description=description,
                name=name,
                workflow_args=workflow_args,
                max_concurrently_running=max_concurrently_running,
            )
            session.add(workflow)
            session.flush()
            logger.info("Created new workflow")

            # update attributes
            if workflow_attributes:
                _add_workflow_attributes(workflow.id, workflow_attributes, session)
                session.flush()
            newly_created = True
        else:
            # set mutable attributes. Moved here from the set_resume method
            workflow.description = description
            workflow.name = name
            workflow.max_concurrently_running = max_concurrently_running
            session.flush()

            # upsert attributes
            if workflow_attributes:
                logger.info("Upsert attributes for workflow")
                if workflow_attributes:
                    for name, val in workflow_attributes.items():
                        _upsert_wf_attribute(workflow.id, name, val, session)
            newly_created = False

    resp = jsonify(
        {
            "workflow_id": workflow.id,
            "status": workflow.status,
            "newly_created": newly_created,
        }
    )
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_args_hash>", methods=["GET"])
def get_matching_workflows_by_workflow_args(workflow_args_hash: str) -> Any:
    """Return any dag hashes that are assigned to workflows with identical workflow args."""
    try:
        workflow_args_hash = str(int(workflow_args_hash))
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    structlog.contextvars.bind_contextvars(workflow_args_hash=str(workflow_args_hash))
    logger.info(f"Looking for wf with hash {workflow_args_hash}")

    session = SessionLocal()
    with session.begin():
        select_stmt = (
            select(Workflow.task_hash, Workflow.tool_version_id, Dag.hash)
            .join_from(Workflow, Dag, Workflow.dag_id == Dag.id)
            .where(Workflow.workflow_args_hash == workflow_args_hash)
        )
        res = []
        for row in session.execute(select_stmt).all():
            res.append((row.task_hash, row.tool_version_id, row.hash))

    if len(res) > 0:
        logger.debug(
            f"Found {res} workflow for " f"workflow_args_hash {workflow_args_hash}"
        )

    resp = jsonify(matching_workflows=res)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_wf_attribute_type(name: str, session: Session) -> int:
    try:
        with session.begin_nested():
            wf_attrib_type = WorkflowAttributeType(name=name)
            session.add(wf_attrib_type)
    except sqlalchemy.exc.IntegrityError:
        with session.begin_nested():
            select_stmt = select(WorkflowAttributeType).where(
                WorkflowAttributeType.name == name
            )
            wf_attrib_type = session.execute(select_stmt).scalars().one()

    return wf_attrib_type.id


def _upsert_wf_attribute(
    workflow_id: int, name: str, value: str, session: Session
) -> None:
    with session.begin_nested():
        wf_attrib_id = _add_or_get_wf_attribute_type(name, session)
        if SessionLocal.bind.dialect.name == "mysql":
            insert_vals = mysql_insert(WorkflowAttribute).values(
                workflow_id=workflow_id,
                workflow_attribute_type_id=wf_attrib_id,
                value=value,
            )
            upsert_stmt = insert_vals.on_duplicate_key_update(
                value=insert_vals.inserted.value
            )
        elif SessionLocal.bind.dialect.name == "sqlite":
            insert_vals = sqlite_insert(WorkflowAttribute).values(
                workflow_id=workflow_id,
                workflow_attribute_type_id=wf_attrib_id,
                value=value,
            )
            upsert_stmt = insert_vals.on_conflict_do_update(
                index_elements=["workflow_id", "workflow_attribute_type_id"],
                set_=dict(value=value),
            )
        session.execute(upsert_stmt)


@blueprint.route("/workflow/<workflow_id>/workflow_attributes", methods=["PUT"])
def update_workflow_attribute(workflow_id: int) -> Any:
    """Update the attributes for a given workflow."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    try:
        workflow_id = int(workflow_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e
    """ Add/update attributes for a workflow """
    data = cast(Dict, request.get_json())

    logger.debug("Update attributes")
    attributes = data["workflow_attributes"]
    if attributes:
        session = SessionLocal()
        with session.begin():
            for name, val in attributes.items():
                _upsert_wf_attribute(workflow_id, name, val, session)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/set_resume", methods=["POST"])
def set_resume(workflow_id: int) -> Any:
    """Set resume on a workflow."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    try:
        data = cast(Dict, request.get_json())
        logger.info("Set resume for workflow")
        reset_running_jobs = bool(data["reset_running_jobs"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    session = SessionLocal()
    with session.begin():
        select_stmt = select(Workflow).where(Workflow.id == workflow_id)
        workflow = session.execute(select_stmt).scalars().one_or_none()
        if workflow:
            # trigger resume on active workflow run
            workflow.resume(reset_running_jobs)
            session.flush()
            logger.info(f"Resume set for wf {workflow_id}")

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/is_resumable", methods=["GET"])
def workflow_is_resumable(workflow_id: int) -> Any:
    """Check if a workflow is in a resumable state."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)

    session = SessionLocal()
    with session.begin():
        select_stmt = select(Workflow).where(Workflow.id == workflow_id)
        workflow = session.execute(select_stmt).scalars().one()

    logger.info(f"Workflow is resumable: {workflow.is_resumable}")
    resp = jsonify(workflow_is_resumable=workflow.is_resumable)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow/<workflow_id>/get_max_concurrently_running", methods=["GET"]
)
def get_max_concurrently_running(workflow_id: int) -> Any:
    """Return the maximum concurrency of this workflow."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)

    session = SessionLocal()
    with session.begin():
        select_stmt = select(Workflow).where(Workflow.id == workflow_id)
        workflow = session.execute(select_stmt).scalars().one()

    resp = jsonify(max_concurrently_running=workflow.max_concurrently_running)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "workflow/<workflow_id>/update_max_concurrently_running", methods=["PUT"]
)
def update_max_running(workflow_id: int) -> Any:
    """Update the number of tasks that can be running concurrently for a given workflow."""
    data = cast(Dict, request.get_json())
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    logger.debug("Update workflow max concurrently running")

    try:
        new_limit = data["max_tasks"]
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    session = SessionLocal()
    with session.begin():
        update_stmt = (
            update(Workflow)
            .where(Workflow.id == workflow_id)
            .values(max_concurrently_running=new_limit)
        )
        res = session.execute(update_stmt)

    if res.rowcount == 0:  # Return a warning message if no update was performed
        message = (
            f"No update performed for workflow ID {workflow_id}, max_concurrently_running is "
            f"{new_limit}"
        )
    else:
        message = (
            f"Workflow ID {workflow_id} max concurrently running updated to {new_limit}"
        )

    resp = jsonify(message=message)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/task_status_updates", methods=["POST"])
def task_status_updates(workflow_id: int) -> Any:
    """Returns all tasks in the database that have the specified status.

    Args:
        workflow_id (int): the ID of the workflow.
    """
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    data = cast(Dict, request.get_json())
    logger.info("Get task by status")

    try:
        filter_criteria: Tuple = (
            (Task.workflow_id == workflow_id),
            (Task.status_date >= data["last_sync"]),
        )
    except KeyError:
        filter_criteria = (Task.workflow_id == workflow_id,)

    # get time from db
    session = SessionLocal()
    with session.begin():
        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")

        tasks_by_status_query = (
            select(Task.status, func.group_concat(Task.id))
            .where(*filter_criteria)
            .group_by(Task.status)
        )
        result_dict = {}
        for row in session.execute(tasks_by_status_query):
            result_dict[row[0]] = [int(i) for i in row[1].split(",")]

    resp = jsonify(tasks_by_status=result_dict, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/fetch_workflow_metadata", methods=["GET"])
def fetch_workflow_metadata(workflow_id: int) -> Any:
    """Get metadata associated with specified Workflow ID."""
    # Query for a workflow object
    session = SessionLocal()
    with session.begin():
        wf = session.execute(
            select(Workflow).where(Workflow.id == workflow_id)
        ).scalar()

    if not wf:
        logger.warning(f"No workflow found for ID {workflow_id}")
        return_tuple = ()
    else:
        return_tuple = wf.to_wire_as_distributor_workflow()

    resp = jsonify(workflow=return_tuple)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/get_tasks/<workflow_id>", methods=["GET"])
def get_tasks_from_workflow(workflow_id: int) -> Any:
    """Return tasks associated with specified Workflow ID."""
    max_task_id = request.args.get("max_task_id")
    chunk_size = request.args.get("chunk_size")

    session = SessionLocal()

    with session.begin():
        # Query task table
        query = (
            select(
                Task.id,
                Task.array_id,
                Task.status,
                Task.max_attempts,
                Task.resource_scales,
                Task.fallback_queues,
                TaskResources.requested_resources,
                TaskResources.queue_id,
            )
            .join_from(Task, Array, Task.array_id == Array.id)
            .join_from(Task, TaskResources, Task.task_resources_id == TaskResources.id)
            .where(
                Task.workflow_id == workflow_id,
                # Note: because of this status != "DONE" filter, only the portion of the DAG
                # that is not complete is returned. Assumes that all tasks in a workflow
                # correspond to nodes that belong in the same DAG, and that no downstream
                # nodes can be in DONE for any unfinished task
                Task.status != TaskStatus.DONE,
                # Greater than set by the input max_task_id
                Task.id > max_task_id,
            )
            .order_by(Task.id)
            .limit(chunk_size)
        )
        res = session.execute(query).all()

        queue_map: Dict[int, List[int]] = {}
        array_map: Dict[int, List[int]] = {}
        resp_dict = {}
        for row in res:
            task_id = row[0]
            array_id = row[1]
            queue_id = row[7]
            row_metadata = row[1:7]

            resp_dict[task_id] = list(row_metadata)
            if queue_id not in queue_map:
                queue_map[queue_id] = []
            queue_map[queue_id].append(task_id)
            if array_id not in array_map:
                array_map[array_id] = []
            array_map[array_id].append(task_id)

        # get the queue and cluster
        for queue_id in queue_map.keys():
            queue = session.get(Queue, queue_id)
            queue_name = queue.name
            cluster_name = queue.cluster.name
            for task_id in queue_map[queue_id]:
                resp_dict[task_id].extend([cluster_name, queue_name])

        # get the max concurrency
        for array_id in array_map.keys():
            array = session.get(Array, array_id)
            max_concurrently_running = array.max_concurrently_running
            for task_id in array_map[array_id]:
                resp_dict[task_id].append(max_concurrently_running)

    resp = jsonify(tasks=resp_dict)
    resp.status_code = StatusCodes.OK
    return resp

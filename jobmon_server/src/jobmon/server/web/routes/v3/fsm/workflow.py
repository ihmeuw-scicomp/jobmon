"""Routes for Workflows."""

from collections import defaultdict
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict, List, Optional, Tuple

from fastapi import HTTPException, Request
import pandas as pd
import sqlalchemy
from sqlalchemy import func, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_status import WorkflowStatus
from jobmon.server.web.routes.utils import get_request_username
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage

logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()
_CONFIG = get_jobmon_config()


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


@api_v3_router.post("/workflow")
async def bind_workflow(request: Request) -> Any:
    """Bind a workflow to the database."""
    try:
        data = cast(Dict, await request.json())
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
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    structlog.contextvars.bind_contextvars(
        dag_id=dag_id,
        tool_version_id=tv_id,
        workflow_args_hash=str(whash),
        task_hash=str(thash),
    )
    logger.info("Bind workflow")
    with SessionLocal() as session:
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
                if workflow_attributes and workflow and workflow.id:
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
                            if workflow and workflow.id:
                                _upsert_wf_attribute(workflow.id, name, val, session)
                newly_created = False

        content = {
            "workflow_id": workflow.id,
            "status": workflow.status,
            "newly_created": newly_created,
        }
        resp = JSONResponse(content=content, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/workflow/{workflow_args_hash}")
async def get_matching_workflows_by_workflow_args(
    workflow_args_hash: str, request: Request
) -> Any:
    """Return any dag hashes that are assigned to workflows with identical workflow args."""
    try:
        workflow_args_hash = str(int(workflow_args_hash))
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    structlog.contextvars.bind_contextvars(workflow_args_hash=str(workflow_args_hash))
    logger.info(f"Looking for wf with hash {workflow_args_hash}")

    with SessionLocal() as session:
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

    resp = JSONResponse(content={"matching_workflows": res}, status_code=StatusCodes.OK)
    return resp


def _add_or_get_wf_attribute_type(name: str, session: Session) -> Optional[int]:
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
    if wf_attrib_type:
        return wf_attrib_type.id  # type: ignore
    else:
        raise ValueError(f"Could not find or create attribute type {name}")


def _upsert_wf_attribute(
    workflow_id: int, name: str, value: str, session: Session
) -> None:
    with session.begin_nested():
        wf_attrib_id = _add_or_get_wf_attribute_type(name, session)
        if SessionLocal and "mysql" in _CONFIG.get("db", "sqlalchemy_database_uri"):
            insert_vals1 = mysql_insert(WorkflowAttribute).values(
                workflow_id=workflow_id,
                workflow_attribute_type_id=wf_attrib_id,
                value=value,
            )
            upsert_stmt = insert_vals1.on_duplicate_key_update(
                value=insert_vals1.inserted.value
            )
        elif SessionLocal and "sqlite" in _CONFIG.get("db", "sqlalchemy_database_uri"):
            insert_vals2: sqlalchemy.dialects.sqlite.dml.Insert = sqlite_insert(
                WorkflowAttribute
            ).values(
                workflow_id=workflow_id,
                workflow_attribute_type_id=wf_attrib_id,
                value=value,
            )
            upsert_stmt = insert_vals2.on_conflict_do_update(  # type: ignore
                index_elements=["workflow_id", "workflow_attribute_type_id"],
                set_=dict(value=value),
            )
        session.execute(upsert_stmt)
        session.flush()


@api_v3_router.put("/workflow/{workflow_id}/workflow_attributes")
async def update_workflow_attribute(workflow_id: int, request: Request) -> Any:
    """Update the attributes for a given workflow."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    try:
        workflow_id = int(workflow_id)
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e
    """ Add/update attributes for a workflow """
    data = cast(Dict, await request.json())

    logger.debug("Update attributes")
    attributes = data["workflow_attributes"]
    if attributes:
        session = SessionLocal()
        with session.begin():
            for name, val in attributes.items():
                _upsert_wf_attribute(workflow_id, name, val, session)
        ()
        session.close()
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/workflow/{workflow_id}/set_resume")
async def set_resume(workflow_id: int, request: Request) -> Any:
    """Set resume on a workflow."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    try:
        data = cast(Dict, await request.json())
        user_name = get_request_username(request)

        logger.info("Set resume for workflow")
        reset_running_jobs = bool(data["reset_running_jobs"])
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(Workflow).where(Workflow.id == workflow_id)
            workflow = session.execute(select_stmt).scalars().one_or_none()
            wf_run_select_stmt = (
                select(WorkflowRun)
                .where(WorkflowRun.workflow_id == workflow_id)
                .order_by(WorkflowRun.id.desc())
                .limit(1)
            )
            workflow_run = session.execute(wf_run_select_stmt).scalars().one_or_none()
            if workflow:
                if workflow_run.user != user_name:
                    raise HTTPException(status_code=401, detail="Unauthorized.")
                # trigger resume on active workflow run
                workflow.resume(reset_running_jobs)
                session.flush()
                logger.info(f"Resume set for wf {workflow_id}")

    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/workflow/{workflow_id}/is_resumable")
def workflow_is_resumable(workflow_id: int) -> Any:
    """Check if a workflow is in a resumable state."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(Workflow).where(Workflow.id == workflow_id)
            workflow = session.execute(select_stmt).scalars().one()

        logger.info(f"Workflow is resumable: {workflow.is_resumable}")
        resp = JSONResponse(
            content={"workflow_is_resumable": workflow.is_resumable},
            status_code=StatusCodes.OK,
        )
    return resp


@api_v3_router.get("/workflow/{workflow_id}/get_max_concurrently_running")
async def get_max_concurrently_running(workflow_id: int, request: Request) -> Any:
    """Return the maximum concurrency of this workflow."""
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(Workflow).where(Workflow.id == workflow_id)
            workflow = session.execute(select_stmt).scalars().one()

        resp = JSONResponse(
            content={"max_concurrently_running": workflow.max_concurrently_running},
            status_code=StatusCodes.OK,
        )
    return resp


@api_v3_router.put("/workflow/{workflow_id}/update_max_concurrently_running")
async def update_max_running(workflow_id: int, request: Request) -> Any:
    """Update the number of tasks that can be running concurrently for a given workflow."""
    data = cast(Dict, await request.json())
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    logger.debug("Update workflow max concurrently running")

    try:
        new_limit = data["max_tasks"]
        user_name = get_request_username(request)
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(Workflow).where(Workflow.id == workflow_id)
            workflow = session.execute(select_stmt).scalars().one_or_none()
            wf_run_select_stmt = (
                select(WorkflowRun)
                .where(WorkflowRun.workflow_id == workflow_id)
                .order_by(WorkflowRun.id.desc())
                .limit(1)
            )
            workflow_run = session.execute(wf_run_select_stmt).scalars().one_or_none()

            if workflow:
                if workflow_run.user != user_name:
                    raise HTTPException(status_code=401, detail="Unauthorized.")

            update_stmt = (
                update(Workflow)
                .where(Workflow.id == workflow_id)
                .values(max_concurrently_running=new_limit)
            )
            res = session.execute(update_stmt)
        if res.rowcount == 0:  # Return a warning message if no update was performed
            message = (
                f"No update performed for workflow ID {workflow_id}, "
                f"max_concurrently_running is "
                f"{new_limit}"
            )
        else:
            message = (
                f"Workflow ID {workflow_id} max concurrently "
                f"running updated to {new_limit}"
            )

        resp = JSONResponse(content={"message": message}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.post("/workflow/{workflow_id}/task_status_updates")
async def task_status_updates(workflow_id: int, request: Request) -> Any:
    """Returns all tasks in the database that have the specified status.

    Args:
        workflow_id (int): the ID of the workflow.
        request (Request): the request object.
    """
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    data = cast(Dict, await request.json())
    logger.info("Get task by status")

    try:
        filter_criteria: Tuple = (
            (Task.workflow_id == workflow_id),
            (Task.status_date >= data["last_sync"]),
        )
    except KeyError:
        filter_criteria = (Task.workflow_id == workflow_id,)

    # get time from db
    with SessionLocal() as session:
        with session.begin():
            db_time = session.execute(select(func.now())).scalar()
            str_time = db_time.strftime("%Y-%m-%d %H:%M:%S") if db_time else None

    # Prepare and execute your query without GROUP_CONCAT
    tasks_by_status_query = select(Task.status, Task.id).where(*filter_criteria)

    # Fetch the rows
    result_dict = defaultdict(list)
    for row in session.execute(tasks_by_status_query):
        result_dict[row.status].append(row.id)

    resp = JSONResponse(
        content={"tasks_by_status": result_dict, "time": str_time},
        status_code=StatusCodes.OK,
    )
    return resp


@api_v3_router.get("/workflow/{workflow_id}/fetch_workflow_metadata")
def fetch_workflow_metadata(workflow_id: int) -> Any:
    """Get metadata associated with specified Workflow ID."""
    # Query for a workflow object
    with SessionLocal() as session:
        with session.begin():
            wf = session.execute(
                select(Workflow).where(Workflow.id == workflow_id)
            ).scalar()

        if not wf:
            logger.warning(f"No workflow found for ID {workflow_id}")
            return_tuple = ()
        else:
            return_tuple = wf.to_wire_as_distributor_workflow()

        resp = JSONResponse(
            content={"workflow": return_tuple}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.get("/workflow/get_tasks/{workflow_id}")
def get_tasks_from_workflow(workflow_id: int, max_task_id: int, chunk_size: int) -> Any:
    """Return tasks associated with specified Workflow ID."""
    with SessionLocal() as session:
        with session.begin():
            if max_task_id == 0:
                # Performance suffers heavily if we do a search with WHERE task.id > 0
                # Therefore, select the smallest task ID in the wf to use as the initial
                # floor.
                min_task_id = session.execute(
                    select(func.min(Task.id)).where(Task.workflow_id == workflow_id)
                ).scalar()
                max_task_id = min_task_id - 1 if min_task_id else 0
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
                .join_from(
                    Task, TaskResources, Task.task_resources_id == TaskResources.id
                )
                .where(
                    Task.workflow_id == workflow_id,
                    # Note: because of this status != "DONE" filter, only the portion of the
                    # DAG that is not complete is returned. Assumes that all tasks in a wf
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
                queue_name = queue.name if queue else None
                cluster_name = queue.cluster.name if queue else None  # type: ignore
                for task_id in queue_map[queue_id]:
                    resp_dict[task_id].extend([cluster_name, queue_name])

            # get the max concurrency
            for array_id in array_map.keys():
                array: Any = session.get(Array, array_id)
                max_concurrently_running = array.max_concurrently_running
                for task_id in array_map[array_id]:
                    resp_dict[task_id].append(max_concurrently_running)

    resp = JSONResponse(content={"tasks": resp_dict}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/workflow_status/available_status")
def get_available_workflow_statuses() -> Any:
    """Return all available workflow statuses."""
    # an easy testing route to verify db is loaded
    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(WorkflowStatus.label).distinct()
            res = session.execute(select_stmt).scalars().all()
            print(f"DB Testing Route*******************{res}")
        resp = JSONResponse(
            content={"available_statuses": res}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.put("/workflow/{workflow_id}/update_array_max_concurrently_running")
async def update_array_max_running(workflow_id: int, request: Request) -> Any:
    """Update the number of tasks that can be running concurrently for a given Array."""
    data = cast(Dict, await request.json())
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    logger.debug("Update array max concurrently running")

    try:
        new_limit = int(data["max_tasks"])
        task_template_version_id = data["task_template_version_id"]
        user_name = get_request_username(request)
    except KeyError as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.url.path}", status_code=400
        ) from e

    with SessionLocal() as session:
        with session.begin():
            select_stmt = select(Workflow).where(Workflow.id == workflow_id)
            workflow = session.execute(select_stmt).scalars().one_or_none()
            wf_run_select_stmt = (
                select(WorkflowRun)
                .where(WorkflowRun.workflow_id == workflow_id)
                .order_by(WorkflowRun.id.desc())
                .limit(1)
            )
            workflow_run = session.execute(wf_run_select_stmt).scalars().one_or_none()

            if workflow:
                if workflow_run.user != user_name:
                    raise HTTPException(status_code=401, detail="Unauthorized.")

            update_stmt = (
                update(Array)
                .where(
                    Array.workflow_id == workflow_id,
                    Array.task_template_version_id == task_template_version_id,
                )
                .values(max_concurrently_running=new_limit)
            )

        res = session.execute(update_stmt)
        session.commit()
        if res.rowcount == 0:  # Return a warning message if no update was performed
            message = (
                f"Error updating max_concurrently_running for array ID {workflow_id}."
            )
        resp = JSONResponse(content={"message": message}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/workflow/{workflow_id}/task_template_dag")
async def task_template_dag(workflow_id: str) -> Any:
    """Compute the shape of a Workflow's DAG by TaskTemplate."""
    with SessionLocal() as session:
        with session.begin():
            dag_query = session.query(Workflow.dag_id).filter(
                Workflow.id == workflow_id
            )

            dag_id = dag_query.scalar()

            query = (
                session.query(
                    Edge.node_id,
                    Edge.upstream_node_ids,
                    Edge.downstream_node_ids,
                    TaskTemplate.name,
                )
                .join(Node, Edge.node_id == Node.id)
                .join(
                    TaskTemplateVersion,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                )
                .join(
                    TaskTemplate,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                )
                .filter(Edge.dag_id == dag_id)
            )

            res = session.execute(query).fetchall()

            results_list = [
                {
                    "node_id": row.node_id,
                    "upstream_node_ids": row.upstream_node_ids,
                    "downstream_node_ids": row.downstream_node_ids,
                    "name": row.name,
                }
                for row in res
            ]

    df = pd.DataFrame(results_list)
    task_template_lookup = df[["node_id", "name"]]
    df["downstream_node_ids"] = (
        df["downstream_node_ids"]
        .fillna("[]")
        .str.rstrip('"]')
        .str.lstrip('["')
        .str.split(",")
    )

    # Handle edge case where the is only one node
    df = df.explode("downstream_node_ids")[["node_id", "downstream_node_ids"]]
    df = df.loc[df["downstream_node_ids"] != ""].astype(int)
    df = df.merge(task_template_lookup, on="node_id", how="left")
    df = df.merge(
        task_template_lookup.rename(
            columns={
                "name": "downstream_task_template_id",
                "node_id": "downstream_node_ids",
            }
        ),
        how="left",
        on="downstream_node_ids",
    )

    task_template_names_without_edges = task_template_lookup[["name"]]
    task_template_names_without_edges["downstream_task_template_id"] = None

    df = df[["name", "downstream_task_template_id"]]
    df = pd.concat([df, task_template_names_without_edges]).drop_duplicates()
    resp_content = {"tt_dag": df.to_dict(orient="records")}

    resp = JSONResponse(
        content=resp_content,
        status_code=StatusCodes.OK,
    )

    return resp

"""Routes for Tasks."""

from http import HTTPStatus as StatusCodes
import json
from typing import Any, cast, Dict, List, Set, Union

from fastapi import Request
from sqlalchemy import desc, insert, ScalarResult, select, tuple_, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.sql import func
from starlette.responses import JSONResponse
import structlog

from jobmon.core import constants
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_arg import TaskArg
from jobmon.server.web.models.task_attribute import TaskAttribute
from jobmon.server.web.models.task_attribute_type import TaskAttributeType
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.routes.v3.fsm import fsm_router as api_v3_router
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError

logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()
_CONFIG = get_jobmon_config()


@api_v3_router.put("/task/bind_tasks_no_args")
async def bind_tasks_no_args(request: Request) -> Any:
    """Bind the task objects to the database."""
    all_data = cast(Dict, await request.json())
    tasks = all_data["tasks"]
    workflow_id = int(all_data["workflow_id"])
    mark_created = bool(all_data["mark_created"])
    structlog.contextvars.bind_contextvars(workflow_id=workflow_id)
    logger.info("Binding tasks")
    # receive from client the tasks in a format of:
    # {<hash>:[node_id(1), task_args_hash(2), array_id(3), task_resources_id(4), name(5),
    # command(6), max_attempts(7), reset_if_running(8),resource_scales(9),
    # fallback_queues(10) }

    with SessionLocal() as session:
        with session.begin():
            # Retrieve existing task_ids
            task_select_stmt = select(Task).where(
                (Task.workflow_id == workflow_id),
                tuple_(Task.node_id, Task.task_args_hash).in_(
                    [tuple_(task[0], task[1]) for task in tasks.values()]
                ),
            )
            prebound_tasks = session.execute(task_select_stmt).scalars().all()

            # Bind tasks not present in DB
            tasks_to_add: List = []  # Container for tasks not yet bound to the database
            present_tasks = {
                (task.node_id, task.task_args_hash): task for task in prebound_tasks
            }  # Dictionary mapping existing Tasks to the supplied arguments
            # Dict mapping input tasks to the corresponding args/attributes
            task_hash_lookup = (
                {}
            )  # Reverse dictionary of inputs, maps hash back to values
            for hashval, items in tasks.items():
                (
                    node_id,
                    arg_hash,
                    array_id,
                    task_resources_id,
                    name,
                    command,
                    max_att,
                    reset,
                    resource_scales,
                    fallback_queues,
                ) = items

                id_tuple = (node_id, arg_hash)

                # Conditional logic: Has task already been bound to the DB? If yes, reset the
                # task status and update the args/attributes
                if id_tuple in present_tasks.keys():
                    task = present_tasks[id_tuple]
                    task.reset(
                        name=name,
                        command=command,
                        max_attempts=max_att,
                        reset_if_running=reset,
                    )

                # If not, add the task
                else:
                    task = {  # type: ignore
                        "workflow_id": workflow_id,
                        "node_id": node_id,
                        "task_args_hash": arg_hash,
                        "array_id": array_id,
                        "task_resources_id": task_resources_id,
                        "name": name,
                        "command": command,
                        "max_attempts": max_att,
                        "status": constants.TaskStatus.REGISTERING,
                        "resource_scales": str(resource_scales),
                        "fallback_queues": str(fallback_queues),
                    }
                    tasks_to_add.append(task)

                task_hash_lookup[id_tuple] = hashval

            # Update existing tasks
            if present_tasks:
                # ORM task objects already updated in task.reset, flush the changes
                session.flush()

            # Bind new tasks with raw SQL
            if len(tasks_to_add):
                # This command is guaranteed to succeed,
                # since names are truncated in the client
                task_insert_stmt = insert(Task).values(tasks_to_add)
                session.execute(task_insert_stmt)
                session.flush()

                # Fetch newly bound task ids
                new_task_query = select(Task).where(
                    (Task.workflow_id == workflow_id),
                    tuple_(Task.node_id, Task.task_args_hash).in_(
                        [
                            tuple_(task["node_id"], task["task_args_hash"])
                            for task in tasks_to_add
                        ]
                    ),
                )
                new_tasks = session.execute(new_task_query).scalars().all()

            else:
                # Empty task list
                new_tasks = []

            # Create the response dict of tasks {<hash>: [id, status]}
            # Done here to prevent modifying tasks, and necessitating a refresh.
            return_tasks = {}

            for task in prebound_tasks + new_tasks:  # type: ignore
                id_tuple = (task.node_id, task.task_args_hash)
                hashval = task_hash_lookup[id_tuple]
                return_tasks[hashval] = [task.id, task.status]

            # Set the workflow's created date if this is the last chunk of tasks.
            # Mark that a workflow has completed binding
            if mark_created:
                session.execute(
                    update(Workflow)
                    .where(Workflow.id == workflow_id, Workflow.created_date.is_(None))
                    .values(created_date=func.now())
                )
    resp = JSONResponse(content={"tasks": return_tasks}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.put("/task/bind_task_args")
async def bind_task_args(request: Request) -> Any:
    """Add task args and associated task ids to the database."""
    all_data = cast(Dict, await request.json())
    task_args = all_data["task_args"]
    if any(task_args):
        # Insert task args using INSERT IGNORE to handle conflicts
        task_arg_values = [
            {"task_id": task_id, "arg_id": arg_id, "val": value}
            for task_id, arg_id, value in task_args
        ]
        session = SessionLocal()
        try:
            if SessionLocal and "mysql" in _CONFIG.get("db", "sqlalchemy_database_uri"):
                arg_insert_stmt = (
                    insert(TaskArg).values(task_arg_values).prefix_with("IGNORE")
                )
            elif SessionLocal and "sqlite" in _CONFIG.get(
                "db", "sqlalchemy_database_uri"
            ):
                arg_insert_stmt = (
                    sqlite_insert(TaskArg)
                    .values(task_arg_values)
                    .on_conflict_do_nothing()
                )
            else:
                raise ServerError(
                    "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
                    + _CONFIG.get("db", "sqlalchemy_database_uri")
                    if SessionLocal
                    else "None"
                )
            with SessionLocal() as session:
                with session.begin():
                    session.execute(arg_insert_stmt)

        except (DataError, IntegrityError) as e:
            # Args likely too long, message back
            raise InvalidUsage(
                "Task Args are constrained to 1000 characters, you may have values "
                f"that are too long. Message: {str(e)}",
                status_code=400,
            ) from e
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.put("/task/bind_task_attributes")
async def bind_task_attributes(request: Request) -> Any:
    """Add task attributes and associated attribute types to the database."""
    all_data = cast(Dict, await request.json())
    attributes = all_data["task_attributes"]

    # Map attribute names to attribute_type_ids, insert if necessary
    all_attribute_names = set()
    for attribute in attributes.values():
        all_attribute_names |= set(attribute.keys())

    if any(all_attribute_names):
        with SessionLocal() as session:
            with session.begin():
                attribute_type_ids = _add_or_get_attribute_types(all_attribute_names)
                # Build our insert values. On conflicts, update the existing value
                insert_values = []
                for task_id, attribute_dict in attributes.items():
                    for attribute_name, attribute_val in attribute_dict.items():
                        insert_row = {
                            "task_id": task_id,
                            "task_attribute_type_id": attribute_type_ids[
                                attribute_name
                            ],
                            "value": attribute_val,
                        }
                        insert_values.append(insert_row)

                # Insert and handle the conflicts
                if insert_values:
                    try:
                        if SessionLocal and "mysql" in _CONFIG.get(
                            "db", "sqlalchemy_database_uri"
                        ):
                            attr_insert_stmt = mysql_insert(TaskAttribute).values(
                                insert_values
                            )
                            attr_insert_stmt = attr_insert_stmt.on_duplicate_key_update(
                                value=attr_insert_stmt.inserted.value
                            )
                            session.execute(attr_insert_stmt)

                        elif SessionLocal and "sqlite" in _CONFIG.get(
                            "db", "sqlalchemy_database_uri"
                        ):
                            for attr_to_add in insert_values:
                                attr_insert_stmt = (  # type: ignore
                                    sqlite_insert(TaskAttribute)  # type: ignore
                                    .values(attr_to_add)
                                    .on_conflict_do_update(
                                        index_elements=[
                                            "task_id",
                                            "task_attribute_type_id",
                                        ],
                                        set_=dict(value=attr_to_add["value"]),
                                    )
                                )
                                session.execute(attr_insert_stmt)
                        else:
                            raise ServerError(
                                "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
                                + _CONFIG.get("db", "sqlalchemy_database_uri")
                                if SessionLocal
                                else "None"
                            )
                    except (DataError, IntegrityError) as e:
                        # Attributes too long, message back
                        raise InvalidUsage(
                            "Task attributes are constrained to 255 characters, "
                            f"you may have values that are too long. Message: {str(e)}",
                            status_code=400,
                        ) from e
            ()
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


def _add_or_get_attribute_types(names: Union[List[str], Set[str]]) -> Dict[str, int]:
    # Query for existing attribute types, to avoid integrity conflicts
    names = set(names)
    with SessionLocal() as session:
        with session.begin():
            existing_rows_select = select(TaskAttributeType).where(
                TaskAttributeType.name.in_(names)
            )
            existing_rows_raw: ScalarResult[TaskAttributeType] = session.execute(
                existing_rows_select
            ).scalars()
            existing_rows = {attr.name: attr.id for attr in existing_rows_raw}

            existing_names = set(existing_rows.keys())

            # Insert the remaining names, found from the difference between old and new
            # Keep the IGNORE prefix in case other agents add attributes first, prevent errors
            # while trying to minimize collisions
            new_names = names - existing_names  # type: ignore

            # We'll eventually return the combination of existing + new attribute type ids
            return_dict = existing_rows
            if any(new_names):
                new_attribute_types = [{"name": name} for name in new_names]
                try:
                    if SessionLocal and "mysql" in _CONFIG.get(
                        "db", "sqlalchemy_database_uri"
                    ):
                        insert_stmt = (
                            insert(TaskAttributeType)
                            .values(new_attribute_types)
                            .prefix_with("IGNORE")
                        )
                    elif SessionLocal and "sqlite" in _CONFIG.get(
                        "db", "sqlalchemy_database_uri"
                    ):
                        insert_stmt = (
                            sqlite_insert(TaskAttributeType)
                            .values(new_attribute_types)
                            .on_conflict_do_nothing()
                        )
                    else:
                        raise ServerError(
                            "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
                            + _CONFIG.get("db", "sqlalchemy_database_uri")
                            if SessionLocal
                            else "None"
                        )
                    session.execute(insert_stmt)

                except DataError as e:
                    raise InvalidUsage(
                        "Attribute types are constrained to 255 characters, your "
                        f"attributes might be too long. Message: {str(e)}",
                        status_code=400,
                    ) from e

                # Query the IDs of the newly inserted rows
                new_rows_select = select(TaskAttributeType).where(
                    TaskAttributeType.name.in_(new_names)
                )
                new_attribute_type_ids = (
                    session.execute(new_rows_select).scalars().all()
                )

                # Update our return dict
                return_dict.update(
                    {
                        # Code to keep typechecker happy
                        attribute.name if attribute.name else "NA": (  # type: ignore
                            attribute.id if attribute.id else -1  # type: ignore
                        )
                        for attribute in new_attribute_type_ids
                    }
                )
    return return_dict  # type: ignore


@api_v3_router.post("/task/bind_resources")
async def bind_task_resources(request: Request) -> Any:
    """Add the task resources for a given task."""
    data = cast(Dict, await request.json())

    with SessionLocal() as session:
        with session.begin():
            tr_id = data.get("task_resources_type_id", None)
            req_resc = json.dumps(data.get("requested_resources", None))
            new_resources = TaskResources(
                queue_id=data["queue_id"],
                task_resources_type_id=tr_id,  # type: ignore
                requested_resources=req_resc,  # type: ignore
            )
            session.add(new_resources)
        ()
        resp = JSONResponse(content=new_resources.id, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/task/{task_id}/most_recent_ti_error")
def get_most_recent_ti_error(task_id: int) -> Any:
    """Route to determine the cause of the most recent task_instance's error.

    Args:
        task_id (int): the ID of the task.

    Return:
        error message
    """
    structlog.contextvars.bind_contextvars(task_id=task_id)
    logger.info(f"Getting most recent ji error for ti {task_id}")

    with SessionLocal() as session:
        with session.begin():
            select_stmt = (
                select(TaskInstanceErrorLog)
                .join_from(
                    TaskInstance,
                    TaskInstanceErrorLog,
                    TaskInstance.id == TaskInstanceErrorLog.task_instance_id,
                )
                .where(TaskInstance.task_id == task_id)
                .order_by(desc(TaskInstance.id))
                .limit(1)
            )
            ti_error = session.execute(select_stmt).scalars().one_or_none()

        if ti_error is not None:
            content = {
                "error_description": ti_error.description,
                "task_instance_id": ti_error.task_instance_id,
            }
            resp = JSONResponse(content=content, status_code=StatusCodes.OK)
        else:
            resp = JSONResponse(
                content={"error_description": "", "task_instance_id": None},
                status_code=StatusCodes.OK,
            )
    return resp


@api_v3_router.post("/task/{workflow_id}/set_resume_state")
async def set_task_resume_state(workflow_id: int, request: Request) -> Any:
    """An endpoint to set all tasks to a resumable state for a workflow.

    Conditioned on the workflow already being in an appropriate resume state.
    """
    data = cast(Dict, await request.json())
    reset_if_running = bool(data["reset_if_running"])

    with SessionLocal() as session:
        with session.begin():
            # Ensure that the workflow is resumable
            # Necessary?
            workflow = session.execute(
                select(Workflow).where(Workflow.id == workflow_id)
            ).scalar()
            if workflow and not workflow.is_resumable:
                err_msg = (
                    f"Workflow {workflow_id} is not resumable. Please "
                    f"set the appropriate resume state."
                )
                resp = JSONResponse(
                    content={"err_msg": err_msg}, status_code=StatusCodes.OK
                )
                return resp

            # Set task reset. If calling this bulk route,
            # don't update any metadata besides what's
            # already bound in the database.

            # Logic: reset_if_running -> Reset all tasks not in "D" state
            # else, reset all tasks not in "D" or "R" state
            # for performance, also excclude TaskStatus.REGISTERING
            excluded_states = [TaskStatus.DONE, TaskStatus.REGISTERING]
            if not reset_if_running:
                excluded_states.append(TaskStatus.RUNNING)

            session.execute(
                update(Task)
                .where(
                    Task.status.not_in(excluded_states), Task.workflow_id == workflow_id
                )
                .values(
                    status=TaskStatus.REGISTERING,
                    num_attempts=0,
                    status_date=func.now(),
                )
            )
        ()
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp

"""Routes for Workflow."""

from datetime import datetime
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import Query, Request
import pandas as pd
from sqlalchemy import (
    func,
    Select,
    select,
    text,
    update,
)
from starlette.responses import JSONResponse
import structlog

from jobmon.core.constants import WorkflowStatus as Statuses
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_status import WorkflowStatus
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()

_cli_label_mapping = {
    "A": "PENDING",
    "G": "PENDING",
    "Q": "PENDING",
    "I": "PENDING",
    "E": "PENDING",
    "O": "SCHEDULED",
    "R": "RUNNING",
    "F": "FATAL",
    "D": "DONE",
}

_reversed_cli_label_mapping = {
    "SCHEDULED": ["O"],
    "PENDING": ["A", "G", "Q", "E", "I"],
    "RUNNING": ["R"],
    "FATAL": ["F"],
    "DONE": ["D"],
}

_cli_order = ["PENDING", "SCHEDULED", "RUNNING", "DONE", "FATAL"]


@api_v3_router.post("/workflow_validation")
async def get_workflow_validation_status(request: Request) -> Any:
    """Check if workflow is valid."""
    # initial params
    data = await request.json()
    task_ids = data["task_ids"]

    # if the given list is empty, return True
    if len(task_ids) == 0:
        resp = JSONResponse(content={"validation": True}, status_code=StatusCodes.OK)
        return resp

    with SessionLocal() as session:
        with session.begin():
            # execute query
            query_filter = [Task.workflow_id == Workflow.id, Task.id.in_(task_ids)]
            sql = (
                select(Task.workflow_id, Workflow.status).where(*query_filter)
            ).distinct()
            rows = session.execute(sql).all()
        res = [ti[1] for ti in rows]
        # Validate if all tasks are in the same workflow and the workflow status is dead
        if len(res) == 1 and res[0] in (
            Statuses.FAILED,
            Statuses.DONE,
            Statuses.ABORTED,
            Statuses.HALTED,
        ):
            validation = True
        else:
            validation = False

        resp = JSONResponse(
            content={"validation": validation, "workflow_status": res[0]},
            status_code=StatusCodes.OK,
        )
    return resp


@api_v3_router.get("/workflow/{workflow_id}/workflow_tasks")
def get_workflow_tasks(
    workflow_id: int, limit: int, status: Optional[list[str]] = Query(None)
) -> Any:
    """Get the tasks for a given workflow."""
    status_request = status
    logger.debug(f"Get tasks for workflow in status {status_request}")

    with SessionLocal() as session:
        with session.begin():
            if status_request:
                query_filter = [
                    Workflow.id == Task.workflow_id,
                    Task.status.in_(
                        [
                            i
                            for arg in status_request
                            for i in _reversed_cli_label_mapping[arg]
                        ]
                    ),
                    Workflow.id == int(workflow_id),
                ]
            else:
                query_filter = [
                    Workflow.id == Task.workflow_id,
                    Workflow.id == int(workflow_id),
                ]
            sql = (
                select(Task.id, Task.name, Task.status, Task.num_attempts).where(
                    *query_filter
                )
            ).order_by(Task.id.desc())
            rows = session.execute(sql).all()
        column_names = ("TASK_ID", "TASK_NAME", "STATUS", "RETRIES")
        res = [dict(zip(column_names, ti)) for ti in rows]
        for r in res:
            r["RETRIES"] = 0 if r["RETRIES"] <= 1 else r["RETRIES"] - 1

        if limit:
            res = res[: int(limit)]

        logger.debug(
            f"The following tasks of workflow are in status {status_request}:\n{res}"
        )
        if res:
            # assign to dataframe for serialization
            df = pd.DataFrame(res, columns=res[0].keys())

            # remap to jobmon_cli statuses
            df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)
            df = df.to_json()
            resp = JSONResponse(
                content={"workflow_tasks": df}, status_code=StatusCodes.OK
            )
        else:
            df = pd.DataFrame({}, columns=["TASK_ID", "TASK_NAME", "STATUS", "RETRIES"])
            resp = JSONResponse(
                content={"workflow_tasks": df.to_json()}, status_code=StatusCodes.OK
            )
    return resp


@api_v3_router.get("/workflow/{workflow_id}/validate_username/{username}")
def get_workflow_user_validation(workflow_id: int, username: str) -> Any:
    """Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """
    logger.debug(f"Validate user name {username} for workflow")
    with SessionLocal() as session:
        with session.begin():
            query_filter = [WorkflowRun.workflow_id == workflow_id]
            sql = (select(WorkflowRun.user).where(*query_filter)).distinct()
            rows = session.execute(sql).all()
        usernames = [row[0] for row in rows]
        resp = JSONResponse(
            content={"validation": username in usernames}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.get("/workflow/{workflow_id}/validate_for_workflow_reset/{username}")
def get_workflow_run_for_workflow_reset(workflow_id: int, username: str) -> Any:
    """Last workflow_run_id associated with a given workflow_id started by the username.

    Used to validate for workflow_reset:
        1. The last workflow_run of the current workflow must be in error state.
        2. This last workflow_run must have been started by the input username.
        3. This last workflow_run is in status 'E'
    """
    with SessionLocal() as session:
        with session.begin():
            query_filter = [
                WorkflowRun.workflow_id == workflow_id,
                WorkflowRun.status == "E",
            ]
            sql = (
                select(WorkflowRun.id, WorkflowRun.user).where(*query_filter)
            ).order_by(WorkflowRun.created_date.desc())
            rows = session.execute(sql).all()
        result = None if len(rows) <= 0 else rows[0]
        if result is not None and result[1] == username:
            resp = JSONResponse(
                content={"workflow_run_id": result[0]}, status_code=StatusCodes.OK
            )
        else:
            resp = JSONResponse(
                content={"workflow_run_id": None}, status_code=StatusCodes.OK
            )
    return resp


@api_v3_router.put("/workflow/{workflow_id}/reset")
async def reset_workflow(workflow_id: int, request: Request) -> Any:
    """Update the workflow's status, all its tasks' statuses to 'G'."""
    data = await request.json()
    partial_reset = data.get("partial_reset", False)
    with SessionLocal() as session:
        with session.begin():
            current_time = session.query(func.now()).scalar()

            workflow_query = select(Workflow).where(Workflow.id == workflow_id)
            workflow = session.execute(workflow_query).scalars().one_or_none()
            if workflow:
                workflow.reset(current_time=current_time)
                session.flush()

            # Update task statuses associated with the workflow
            # Default behavior is a full workflow reset, all tasks to registered state
            # User can optionally request only a partial reset if they want to resume
            invalid_statuses = ["G"]
            if partial_reset:
                invalid_statuses.append("D")
            update_filter = [
                Task.workflow_id == workflow_id,
                Task.status.notin_(invalid_statuses),
            ]
            update_stmt = (
                update(Task)
                .where(*update_filter)
                .values(status="G", status_date=func.now(), num_attempts=0)
            )
            session.execute(update_stmt)
            session.commit()

        resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/workflow_status")
def get_workflow_status(
    workflow_id: Optional[Union[int, str, List[Union[int, str]]]] = Query(None),
    limit: Optional[int] = Query(None),
    user: Optional[list[str]] = Query(None),
) -> Any:
    """Get the status of the workflow."""
    # initial params
    params = {}
    user_request = user
    if user_request == "all":  # specifying all is equivalent to None
        user_request = []
    if isinstance(workflow_id, int):
        workflow_request = [workflow_id]
    elif isinstance(workflow_id, str) and workflow_id == "all":
        workflow_request = []
    else:
        workflow_request = workflow_id  # type: ignore
    logger.debug(f"Query for wf {workflow_request} status.")
    # set default to 5 to match status_commands
    limit = int(limit) if limit else 5
    # convert workflow request into sql filter
    if workflow_request:
        workflow_request = [int(w) for w in workflow_request]
        params["workflow_id"] = workflow_request
    else:  # if we don't specify workflow then we use the users
        # convert user request into sql filter
        # directly producing workflow_ids, and thus where_clause
        if user_request:
            session = SessionLocal()
            with session.begin():
                query_filter = [WorkflowRun.user.in_(user_request)]
                sql = (
                    (select(WorkflowRun.workflow_id).where(*query_filter))
                    .distinct()
                    .order_by(WorkflowRun.workflow_id.desc())
                    .limit(limit)
                )
                rows = session.execute(sql).all()
            workflow_request = [int(row[0]) for row in rows]
    # performance improvement one: only query the limited number of workflows
    workflow_request = workflow_request[:limit]
    # performance improvement two: split query
    with SessionLocal() as session:
        with session.begin():
            query_filter = [
                Workflow.id.in_(workflow_request),  # type: ignore
                WorkflowStatus.id == Workflow.status,  # type: ignore
            ]
            sql1: Select[
                Tuple[Optional[int], Optional[str], Optional[str], Optional[datetime]]
            ] = (
                select(
                    Workflow.id,
                    Workflow.name,
                    WorkflowStatus.label,
                    Workflow.created_date,
                )
            ).where(
                *query_filter
            )
            rows1 = session.execute(sql1).all()
    row_map = dict()
    for r in rows1:
        row_map[r[0]] = r
    session = SessionLocal()
    with session.begin():
        query_filter = [
            Task.workflow_id.in_(workflow_request),
        ]
        sql2: Select[Tuple[int, int, str]] = (
            select(
                Task.workflow_id,
                func.count(Task.status),
                Task.status,
            ).where(*query_filter)
        ).group_by(Task.workflow_id, Task.status)
        rows2 = session.execute(sql2).all()

    res = []
    for r in rows2:  # type: ignore
        d = dict()
        d["WF_ID"] = r[0]
        d["WF_NAME"] = row_map[r[0]][1]
        d["WF_STATUS"] = row_map[r[0]][2]
        d["TASKS"] = r[1]
        d["STATUS"] = r[2]
        d["CREATED_DATE"] = row_map[r[0]][3]
        session = SessionLocal()
        with session.begin():
            q_filter = [Task.workflow_id == d["WF_ID"], Task.status == d["STATUS"]]
            q = select(Task.num_attempts).where(*q_filter)
            query_result = session.execute(q).all()
        retries = 0
        for rr in query_result:
            retries += 0 if int(rr[0]) <= 1 else int(rr[0]) - 1
        d["RETRIES"] = retries
        res.append(d)
    if res is not None and len(res) > 0:
        # assign to dataframe for aggregation
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to jobmon_cli statuses
        df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)

        # aggregate totals by workflow and status
        df = df.groupby(
            ["WF_ID", "WF_NAME", "WF_STATUS", "STATUS", "CREATED_DATE"]
        ).agg({"TASKS": "sum", "RETRIES": "sum"})

        # pivot wide by task status
        tasks = df.pivot_table(
            values="TASKS",
            index=["WF_ID", "WF_NAME", "WF_STATUS", "CREATED_DATE"],
            columns="STATUS",
            fill_value=0,
        )
        for col in _cli_order:
            if col not in tasks.columns:
                tasks[col] = 0
        tasks = tasks[_cli_order]

        # aggregate again without status to get the totals by workflow
        retries = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS", "CREATED_DATE"]).agg(
            {"TASKS": "sum", "RETRIES": "sum"}
        )

        # combine datasets
        df = pd.concat([tasks, retries], axis=1)

        # compute pcts and format
        for col in _cli_order:
            df[col + "_pct"] = (df[col].astype(float) / df["TASKS"].astype(float)) * 100
            df[col + "_pct"] = df[[col + "_pct"]].round(1)
            df[col] = (
                df[col].astype(int).astype(str)
                + " ("
                + df[col + "_pct"].astype(str)
                + "%)"
            )

        # df.replace(to_replace={"0 (0.0%)": "NA"}, inplace=True)
        # final order
        df = df[["TASKS"] + _cli_order + ["RETRIES"]]
        df = df.reset_index()
        df = df.to_json()
        resp = JSONResponse(content={"workflows": df}, status_code=StatusCodes.OK)
    else:
        df = pd.DataFrame(
            {},
            columns=[
                "WF_ID",
                "WF_NAME",
                "WF_STATUS",
                "CREATED_DATE",
                "TASKS",
                "PENDING",
                "RUNNING",
                "DONE",
                "FATAL",
                "RETRIES",
            ],
        ).to_json()
        resp = JSONResponse(content={"workflows": df}, status_code=StatusCodes.OK)

    return resp


@api_v3_router.get("/workflow_status_viz")
def get_workflow_status_viz(workflow_ids: list[int] = Query(None)) -> Any:
    """Get the status of the workflows for GUI."""
    wf_ids = workflow_ids
    # return DS
    return_dic: Dict[int, Any] = dict()
    for wf_id in wf_ids:
        with SessionLocal() as session:
            with session.begin():
                sql = select(
                    func.min(Task.num_attempts).label("min"),
                    func.max(Task.num_attempts).label("max"),
                    func.avg(Task.num_attempts).label("mean"),
                ).where(Task.workflow_id == wf_id)
                attempts = session.execute(sql).first()

        return_dic[int(wf_id)] = {
            "id": int(wf_id),
            "tasks": 0,
            "PENDING": 0,
            "SCHEDULED": 0,
            "RUNNING": 0,
            "DONE": 0,
            "FATAL": 0,
            "MAXC": 0,
            "num_attempts_avg": float(attempts.mean),  # type: ignore
            "num_attempts_min": int(attempts.min),  # type: ignore
            "num_attempts_max": int(attempts.max),  # type: ignore
        }
    with SessionLocal() as session:
        with session.begin():
            query_filter = [
                Task.workflow_id.in_(wf_ids),
                Task.workflow_id == Workflow.id,
            ]
            sql = select(
                Task.workflow_id, Task.status, Workflow.max_concurrently_running
            ).where(*query_filter)
            rows = session.execute(sql).all()

    for row in rows:
        return_dic[row[0]]["tasks"] += 1
        return_dic[row[0]][_cli_label_mapping[row[1]]] += 1
        return_dic[row[0]]["MAXC"] = row[2]
    resp = JSONResponse(content=return_dic, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/workflow_overview_viz")
def workflows_by_user_form(
    user: Optional[str] = Query(None),
    tool: Optional[str] = Query(None),
    wf_name: Optional[str] = Query(None),
    wf_args: Optional[str] = Query(None),
    wf_attribute_value: Optional[str] = Query(None),
    wf_attribute_key: Optional[str] = Query(None),
    wf_id: Optional[str] = Query(None),
    date_submitted: Optional[str] = Query(None),
    date_submitted_end: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
) -> Any:
    """Fetch associated workflows and workflow runs by username."""
    with SessionLocal() as session:
        with session.begin():
            where_clauses = []
            substitution_dict = {}
            if user:
                where_clauses.append("workflow_run.user = :user")
                substitution_dict["user"] = user
            if tool:
                where_clauses.append("tool.name = :tool")
                substitution_dict["tool"] = tool
            if wf_name:
                where_clauses.append("workflow.name = :wf_name")
                substitution_dict["wf_name"] = wf_name
            if wf_args:
                where_clauses.append("workflow.workflow_args = :wf_args")
                substitution_dict["wf_args"] = wf_args
            if wf_attribute_key:
                where_clauses.append("workflow_attribute_type.name = :wf_attribute_key")
                substitution_dict["wf_attribute_key"] = wf_attribute_key
            if wf_attribute_value:
                where_clauses.append("workflow_attribute.value = :wf_attribute_value")
                substitution_dict["wf_attribute_value"] = wf_attribute_value
            if wf_id:
                where_clauses.append("workflow.id = :wf_id")
                substitution_dict["wf_id"] = wf_id  # type: ignore
            if date_submitted:
                where_clauses.append("workflow.created_date >= :date_submitted")
                substitution_dict["date_submitted"] = date_submitted
            if date_submitted_end:
                where_clauses.append("workflow.created_date <= :date_submitted_end")
                substitution_dict["date_submitted_end"] = date_submitted_end
            if status:
                where_clauses.append("workflow.status = :status")
                substitution_dict["status"] = status

            if where_clauses:
                inner_where_clause = " WHERE " + (" AND ".join(where_clauses))
            else:
                inner_where_clause = ""

            query = text(
                f"""
                SELECT
                    workflow.id,
                    workflow.name,
                    workflow.created_date,
                    workflow.status_date,
                    workflow.workflow_args,
                    count(distinct workflow_run.id) as num_attempts,
                    workflow_status.label,
                    tool.name
                FROM
                    workflow
                    JOIN (
                        SELECT
                            distinct queue_id,
                            workflow_id
                        FROM
                            task
                            JOIN task_resources ON task_resources.id = task.task_resources_id
                        WHERE
                            task.workflow_id IN (
                                SELECT
                                    workflow_run.workflow_id
                                FROM
                                    workflow
                                    JOIN tool_version ON
                                        workflow.tool_version_id = tool_version.id
                                    JOIN tool ON tool.id = tool_version.tool_id
                                    JOIN workflow_run ON workflow.id = workflow_run.workflow_id
                                    LEFT JOIN workflow_attribute
                                        ON workflow.id = workflow_attribute.workflow_id
                                    LEFT JOIN workflow_attribute_type
                                        ON workflow_attribute.workflow_attribute_type_id =
                                            workflow_attribute_type.id
                                {inner_where_clause}
                            )
                        GROUP BY
                            workflow_id, queue_id
                    ) workflow_queue ON workflow.id = workflow_queue.workflow_id
                    JOIN queue ON queue.id = workflow_queue.queue_id
                    JOIN workflow_run ON workflow.id = workflow_run.workflow_id
                    JOIN tool_version ON workflow.tool_version_id = tool_version.id
                    JOIN tool ON tool.id = tool_version.tool_id
                    JOIN workflow_status ON workflow.status = workflow_status.id
                WHERE
                    cluster_id != 1
                GROUP BY
                    workflow.id
                ORDER BY
                    workflow.id DESC
        """
            )
            rows = session.execute(query, substitution_dict).all()

        def serialize_datetime(obj: datetime) -> str:
            """Serialize datetime objects into string format."""
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {obj.__class__.__name__} not serializable")

        column_names = (
            "wf_id",
            "wf_name",
            "wf_submitted_date",
            "wf_status_date",
            "wf_args",
            "wfr_count",
            "wf_status",
            "wf_tool",
        )
        # Initialize all possible states as 0.
        # No need to return data since it will be refreshed
        # on demand anyways.
        initial_status_counts = {
            label_mapping: 0 for label_mapping in set(_cli_label_mapping.values())
        }
        result = [
            {
                **dict(zip(column_names, row)),
                **initial_status_counts,
                "wf_submitted_date": serialize_datetime(row[2]),
                "wf_status_date": serialize_datetime(row[3]),
            }
            for row in rows
        ]

        res = JSONResponse(content={"workflows": result}, status_code=StatusCodes.OK)
    return res


@api_v3_router.get("/task_table_viz/{workflow_id}")
def task_details_by_wf_id(workflow_id: int, tt_name: str) -> Any:
    """Fetch Task details associated with Workflow ID and TaskTemplate name."""
    task_template_name = tt_name
    with SessionLocal() as session:
        with session.begin():
            sql = (
                select(
                    Task.id,
                    Task.name,
                    Task.status,
                    Task.command,
                    Task.num_attempts,
                    Task.status_date,
                    Task.max_attempts,
                )
                .where(
                    Task.workflow_id == workflow_id,
                    Task.node_id == Node.id,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                    TaskTemplate.name == task_template_name,
                )
                .order_by(Task.id.asc())
            )
            rows = session.execute(sql).all()

    column_names = (
        "task_id",
        "task_name",
        "task_status",
        "task_command",
        "task_num_attempts",
        "task_status_date",
        "task_max_attempts",
    )

    result = [dict(zip(column_names, row)) for row in rows]
    for r in result:
        r["task_status"] = _cli_label_mapping[r["task_status"]]
        r["task_status_date"] = str(r["task_status_date"])
    res = JSONResponse(content={"tasks": result}, status_code=StatusCodes.OK)
    return res


@api_v3_router.get("/workflow_details_viz/{workflow_id}")
def wf_details_by_wf_id(workflow_id: int) -> Any:
    """Fetch name, args, dates, tool for a Workflow provided WF ID."""
    session = SessionLocal()
    with session.begin():
        latest_workflow_run_subquery = (
            session.query(WorkflowRun.workflow_id, func.max(WorkflowRun.heartbeat_date))
            .group_by(WorkflowRun.workflow_id)
            .subquery()
        )

        sql = (
            select(
                Workflow.name,
                Workflow.workflow_args,
                Workflow.created_date,
                Workflow.status_date,
                Tool.name,
                Workflow.status,
                WorkflowStatus.description,
                WorkflowRun.jobmon_version,
                WorkflowRun.heartbeat_date,
                WorkflowRun.user,
            )
            .select_from(Workflow)
            .join(ToolVersion, Workflow.tool_version_id == ToolVersion.id)
            .join(Tool, ToolVersion.tool_id == Tool.id)
            .join(WorkflowStatus, WorkflowStatus.id == Workflow.status)
            .join(WorkflowRun, WorkflowRun.workflow_id == Workflow.id)
            .join(
                latest_workflow_run_subquery,
            )
            .where(
                Workflow.id == workflow_id,
            )
        )
        rows = session.execute(sql).all()

    column_names = (
        "wf_name",
        "wf_args",
        "wf_created_date",
        "wf_status_date",
        "tool_name",
        "wf_status",
        "wf_status_desc",
        "wfr_jobmon_version",
        "wfr_heartbeat_date",
        "wfr_user",
    )

    result = [dict(zip(column_names, row)) for row in rows]
    date_fields = ["wf_status_date", "wf_created_date", "wfr_heartbeat_date"]

    for row in result:
        for field in date_fields:
            if field in row and isinstance(row[field], datetime):
                row[field] = row[field].isoformat()

    resp = JSONResponse(content=result, status_code=200)
    return resp

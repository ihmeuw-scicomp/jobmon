"""Routes for Workflow."""
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, Optional

from flask import jsonify, request
from flask_cors import cross_origin
import pandas as pd
from sqlalchemy import func, select, text, update
import structlog

from jobmon.core.constants import WorkflowStatus as Statuses
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_status import WorkflowStatus
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.cli import blueprint


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)

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


@blueprint.route("/workflow_validation", methods=["POST"])
@cross_origin()
def get_workflow_validation_status() -> Any:
    """Check if workflow is valid."""
    # initial params
    data = request.get_json()
    task_ids = data["task_ids"]

    # if the given list is empty, return True
    if len(task_ids) == 0:
        resp = jsonify(validation=True)
        resp.status_code = StatusCodes.OK
        return resp

    session = SessionLocal()
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

    resp = jsonify(validation=validation, workflow_status=res[0])
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/workflow_tasks", methods=["GET"])
def get_workflow_tasks(workflow_id: int) -> Any:
    """Get the tasks for a given workflow."""
    limit = request.args.get("limit")
    status_request = request.args.getlist("status", None)
    logger.debug(f"Get tasks for workflow in status {status_request}")

    session = SessionLocal()
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
        resp = jsonify(workflow_tasks=df)
    else:
        df = pd.DataFrame({}, columns=["TASK_ID", "TASK_NAME", "STATUS", "RETRIES"])
        resp = jsonify(workflow_tasks=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow/<workflow_id>/validate_username/<username>", methods=["GET"]
)
def get_workflow_user_validation(workflow_id: int, username: str) -> Any:
    """Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """
    logger.debug(f"Validate user name {username} for workflow")
    session = SessionLocal()
    with session.begin():
        query_filter = [WorkflowRun.workflow_id == workflow_id]
        sql = (select(WorkflowRun.user).where(*query_filter)).distinct()
        rows = session.execute(sql).all()
    usernames = [row[0] for row in rows]

    resp = jsonify(validation=username in usernames)

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow/<workflow_id>/validate_for_workflow_reset/<username>", methods=["GET"]
)
def get_workflow_run_for_workflow_reset(workflow_id: int, username: str) -> Any:
    """Last workflow_run_id associated with a given workflow_id started by the username.

    Used to validate for workflow_reset:
        1. The last workflow_run of the current workflow must be in error state.
        2. This last workflow_run must have been started by the input username.
        3. This last workflow_run is in status 'E'
    """
    session = SessionLocal()
    with session.begin():
        query_filter = [
            WorkflowRun.workflow_id == workflow_id,
            WorkflowRun.status == "E",
        ]
        sql = (select(WorkflowRun.id, WorkflowRun.user).where(*query_filter)).order_by(
            WorkflowRun.created_date.desc()
        )
        rows = session.execute(sql).all()
    result = None if len(rows) <= 0 else rows[0]
    if result is not None and result[1] == username:
        resp = jsonify({"workflow_run_id": result[0]})
    else:
        resp = jsonify({"workflow_run_id": None})

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("workflow/<workflow_id>/reset", methods=["PUT"])
def reset_workflow(workflow_id: int) -> Any:
    """Update the workflow's status, all its tasks' statuses to 'G'."""
    session = SessionLocal()
    data = request.get_json()
    partial_reset = data.get("partial_reset", False)
    with session.begin():

        current_time = session.query(func.now()).scalar()

        workflow_query = select(Workflow).where(Workflow.id == workflow_id)
        workflow = session.execute(workflow_query).scalars().one_or_none()
        workflow.reset(current_time=current_time)
        session.flush()

        # Update task statuses associated with the workflow
        # Default behavior is a full workflow reset, all tasks to registered state
        # User can optionally request only a partial reset if they want to resume this workflow
        invalid_statuses = ["G"]
        if partial_reset:
            invalid_statuses.append("D")
        update_filter = [Task.workflow_id == workflow_id, Task.status.notin_(invalid_statuses)]
        update_stmt = (
            update(Task)
            .where(*update_filter)
            .values(status="G", status_date=func.now(), num_attempts=0)
        )
        session.execute(update_stmt)
        session.commit()

    resp = jsonify({})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_status", methods=["GET"])
@cross_origin()
def get_workflow_status() -> Any:
    """Get the status of the workflow."""
    # initial params
    params = {}
    user_request = request.args.getlist("user")
    if user_request == "all":  # specifying all is equivalent to None
        user_request = []
    workflow_request = request.args.getlist("workflow_id")
    logger.debug(f"Query for wf {workflow_request} status.")
    if workflow_request == "all":  # specifying all is equivalent to None
        workflow_request = []
    limit = request.args.get("limit")
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
    session = SessionLocal()
    with session.begin():
        query_filter = [
            Workflow.id.in_(workflow_request),
            WorkflowStatus.id == Workflow.status,
        ]
        sql = (
            select(
                Workflow.id, Workflow.name, WorkflowStatus.label, Workflow.created_date
            )
        ).where(*query_filter)
        rows1 = session.execute(sql).all()
    row_map = dict()
    for r in rows1:
        row_map[r[0]] = r
    session = SessionLocal()
    with session.begin():
        query_filter = [
            Task.workflow_id.in_(workflow_request),
        ]
        sql = (
            select(
                Task.workflow_id,
                func.count(Task.status),
                Task.status,
            ).where(*query_filter)
        ).group_by(Task.workflow_id, Task.status)
        rows2 = session.execute(sql).all()

    res = []
    for r in rows2:
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
        resp = jsonify(workflows=df)
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
        resp = jsonify(workflows=df)

    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_status_viz", methods=["GET"])
def get_workflow_status_viz() -> Any:
    """Get the status of the workflows for GUI."""
    session = SessionLocal()
    wf_ids = request.args.getlist("workflow_ids[]")
    # return DS
    return_dic: Dict[int, Any] = dict()
    for wf_id in wf_ids:
        with session.begin():
            sql = select(
                func.min(Task.num_attempts).label("min"),
                func.max(Task.num_attempts).label("max"),
                func.avg(Task.num_attempts).label("mean"),
            ).where(Task.workflow_id == wf_id)
            attempts = session.execute(sql).all()

        return_dic[int(wf_id)] = {
            "id": int(wf_id),
            "tasks": 0,
            "PENDING": 0,
            "SCHEDULED": 0,
            "RUNNING": 0,
            "DONE": 0,
            "FATAL": 0,
            "MAXC": 0,
            "num_attempts_avg": float(attempts[0]["mean"]),
            "num_attempts_min": int(attempts[0]["min"]),
            "num_attempts_max": int(attempts[0]["max"]),
        }

    with session.begin():
        query_filter = [Task.workflow_id.in_(wf_ids), Task.workflow_id == Workflow.id]
        sql = select(
            Task.workflow_id, Task.status, Workflow.max_concurrently_running
        ).where(*query_filter)
        rows = session.execute(sql).all()

    for row in rows:
        return_dic[row[0]]["tasks"] += 1
        return_dic[row[0]][_cli_label_mapping[row[1]]] += 1
        return_dic[row[0]]["MAXC"] = row[2]

    resp = jsonify(return_dic)
    resp.status_code = 200
    return resp


@blueprint.route("/workflow_overview_viz", methods=["GET"])
def workflows_by_user_form() -> Any:
    """Fetch associated workflows and workflow runs by username."""
    arguments = request.args
    clean_args: Dict[str, Optional[str]] = {}
    for key, value in arguments.items():
        if value is None or value == "undefined" or value == "":
            clean_args[key] = None
        else:
            clean_args[key] = value
    user = arguments.get("user")
    tool = arguments.get("tool")
    wf_name = arguments.get("wf_name")
    wf_args = arguments.get("wf_args")
    wf_attribute = arguments.get("wf_attribute")
    date_submitted = arguments.get("date_submitted")
    status = arguments.get("status")

    session = SessionLocal()
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
        if wf_attribute:
            where_clauses.append("workflow_attribute.value = :wf_attribute")
            substitution_dict["wf_attribute"] = wf_attribute
        if date_submitted:
            where_clauses.append("workflow.created_date >= :date_submitted")
            substitution_dict["date_submitted"] = date_submitted
        if status:
            where_clauses.append("workflow.status = :status")
            substitution_dict["status"] = status
        inner_where_clause = " AND ".join(where_clauses)
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
                        JOIN task_resources on task_resources.id = task.task_resources_id
                    WHERE
                        task.workflow_id IN (
                            SELECT
                                workflow_run.workflow_id
                            FROM
                                workflow
                                JOIN tool_version on workflow.tool_version_id = tool_version.id
                                JOIN tool on tool.id = tool_version.tool_id
                                JOIN workflow_run on workflow.id = workflow_run.workflow_id
                                LEFT JOIN workflow_attribute on workflow.id = workflow_attribute.workflow_id
                            WHERE
                                {inner_where_clause}
                        )
                    GROUP BY
                        workflow_id
                ) workflow_queue on workflow.id = workflow_queue.workflow_id
                JOIN queue on queue.id = workflow_queue.queue_id
                JOIN workflow_run on workflow.id = workflow_run.workflow_id
                JOIN tool_version on workflow.tool_version_id = tool_version.id
                JOIN tool on tool.id = tool_version.tool_id
                JOIN workflow_status on workflow.status = workflow_status.id
            WHERE
                cluster_id != 1
            GROUP BY
                workflow.id
            ORDER BY
                workflow.id DESC
    """
        )
        rows = session.execute(query, substitution_dict).all()

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
    # Initialize all possible states as 0. No need to return data since it will be refreshed
    # on demand anyways.
    initial_status_counts = {
        label_mapping: 0 for label_mapping in set(_cli_label_mapping.values())
    }
    result = [dict(zip(column_names, row), **initial_status_counts) for row in rows]

    res = jsonify(workflows=result)
    res.return_code = StatusCodes.OK
    return res


@blueprint.route("/task_table_viz/<workflow_id>", methods=["GET"])
def task_details_by_wf_id(workflow_id: int) -> Any:
    """Fetch Task details associated with Workflow ID and TaskTemplate name."""
    task_template_name = request.args.get("tt_name")
    session = SessionLocal()
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

    res = jsonify(tasks=result)
    res.return_code = StatusCodes.OK
    return res


@blueprint.route("/workflow_details_viz/<workflow_id>", methods=["GET"])
def wf_details_by_wf_id(workflow_id: int) -> Any:
    """Fetch name, args, dates, tool for a Workflow provided WF ID."""
    session = SessionLocal()
    with session.begin():
        sql = select(
            Workflow.name,
            Workflow.workflow_args,
            Workflow.created_date,
            Workflow.status_date,
            Tool.name,
            Workflow.status,
            WorkflowStatus.description,
        ).where(
            Workflow.id == workflow_id,
            Workflow.tool_version_id == ToolVersion.id,
            ToolVersion.tool_id == Tool.id,
            WorkflowStatus.id == Workflow.status,
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
    )

    result = [dict(zip(column_names, row)) for row in rows]
    resp = jsonify(result)
    resp.status_code = 200
    return resp

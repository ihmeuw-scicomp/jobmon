from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import structlog
from sqlalchemy import Select, func, select, text, update
from sqlalchemy.orm import Session

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
from jobmon.server.web.schemas.workflow import (
    TaskTableItem,
    TaskTableResponse,
    WorkflowDetailsItem,
    WorkflowOverviewItem,
    WorkflowOverviewResponse,
    WorkflowRunForResetResponse,
    WorkflowStatusResponse,
    WorkflowTasksResponse,
    WorkflowUserValidationResponse,
    WorkflowValidationResponse,
)

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


class WorkflowRepository:
    def __init__(self, session: Session) -> None:
        """Initialize the workflow repository."""
        self.session = session

    def get_workflow_validation_status(
        self, task_ids: List[int]
    ) -> WorkflowValidationResponse:
        """Check if workflow is valid."""
        # if the given list is empty, return True
        if len(task_ids) == 0:
            return WorkflowValidationResponse(validation=True)

        # execute query
        query_filter = [Task.workflow_id == Workflow.id, Task.id.in_(task_ids)]
        sql = (
            select(Task.workflow_id, Workflow.status).where(*query_filter)
        ).distinct()
        rows = self.session.execute(sql).all()

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

        return WorkflowValidationResponse(
            validation=validation, workflow_status=res[0] if res else None
        )

    def get_workflow_tasks(
        self, workflow_id: int, limit: int, status: Optional[List[str]] = None
    ) -> WorkflowTasksResponse:
        """Get the tasks for a given workflow."""
        logger.debug(f"Get tasks for workflow in status {status}")

        if status:
            query_filter = [
                Workflow.id == Task.workflow_id,
                Task.status.in_(
                    [i for arg in status for i in _reversed_cli_label_mapping[arg]]
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
        rows = self.session.execute(sql).all()

        column_names = ("TASK_ID", "TASK_NAME", "STATUS", "RETRIES")
        res = [dict(zip(column_names, ti)) for ti in rows]
        for r in res:
            r["RETRIES"] = 0 if r["RETRIES"] <= 1 else r["RETRIES"] - 1

        if limit:
            res = res[: int(limit)]

        logger.debug(f"The following tasks of workflow are in status {status}:\n{res}")
        if res:
            # assign to dataframe for serialization
            df = pd.DataFrame(res, columns=list(res[0].keys()))

            # remap to jobmon_cli statuses
            df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)
            df_json = df.to_json()
        else:
            df = pd.DataFrame({}, columns=["TASK_ID", "TASK_NAME", "STATUS", "RETRIES"])
            df_json = df.to_json()

        return WorkflowTasksResponse(workflow_tasks=df_json)

    def get_workflow_user_validation(
        self, workflow_id: int, username: str
    ) -> WorkflowUserValidationResponse:
        """Return all usernames associated with a given workflow_id's workflow runs."""
        logger.debug(f"Validate user name {username} for workflow")

        query_filter = [WorkflowRun.workflow_id == workflow_id]
        sql = (select(WorkflowRun.user).where(*query_filter)).distinct()
        rows = self.session.execute(sql).all()

        usernames = [row[0] for row in rows]
        return WorkflowUserValidationResponse(validation=username in usernames)

    def get_workflow_run_for_reset(
        self, workflow_id: int, username: str
    ) -> WorkflowRunForResetResponse:
        """Get last workflow_run_id for workflow reset validation."""
        query_filter = [
            WorkflowRun.workflow_id == workflow_id,
            WorkflowRun.status == "E",
        ]
        sql = (select(WorkflowRun.id, WorkflowRun.user).where(*query_filter)).order_by(
            WorkflowRun.created_date.desc()
        )
        rows = self.session.execute(sql).all()

        result = None if len(rows) <= 0 else rows[0]
        if result is not None and result[1] == username:
            workflow_run_id = result[0]
        else:
            workflow_run_id = None

        return WorkflowRunForResetResponse(workflow_run_id=workflow_run_id)

    def reset_workflow(self, workflow_id: int, partial_reset: bool = False) -> None:
        """Update the workflow's status, all its tasks' statuses to 'G'."""
        current_time = self.session.query(func.now()).scalar()

        workflow_query = select(Workflow).where(Workflow.id == workflow_id)
        workflow = self.session.execute(workflow_query).scalars().one_or_none()
        if workflow:
            workflow.reset(current_time=current_time)
            self.session.flush()

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
        self.session.execute(update_stmt)
        self.session.commit()

    def get_workflow_status(
        self,
        workflow_id: Optional[Union[int, str, List[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        user: Optional[List[str]] = None,
    ) -> WorkflowStatusResponse:
        """Get the status of the workflow."""
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
        else:  # if we don't specify workflow then we use the users
            # convert user request into sql filter
            # directly producing workflow_ids, and thus where_clause
            if user_request:
                query_filter = [WorkflowRun.user.in_(user_request)]
                sql = (
                    (select(WorkflowRun.workflow_id).where(*query_filter))
                    .distinct()
                    .order_by(WorkflowRun.workflow_id.desc())
                    .limit(limit)
                )
                rows = self.session.execute(sql).all()
                workflow_request = [int(row[0]) for row in rows]
        # performance improvement one: only query the limited number of workflows
        workflow_request = workflow_request[:limit]
        # performance improvement two: split query
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
        rows1 = self.session.execute(sql1).all()

        row_map = dict()
        for r in rows1:
            row_map[r[0]] = r

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
        rows2 = self.session.execute(sql2).all()

        res = []
        for r in rows2:  # type: ignore
            d = dict()
            d["WF_ID"] = r[0]
            d["WF_NAME"] = row_map[r[0]][1]
            d["WF_STATUS"] = row_map[r[0]][2]
            d["TASKS"] = r[1]
            d["STATUS"] = r[2]
            d["CREATED_DATE"] = row_map[r[0]][3]
            q_filter = [Task.workflow_id == d["WF_ID"], Task.status == d["STATUS"]]
            q = select(Task.num_attempts).where(*q_filter)
            query_result = self.session.execute(q).all()
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
                df[col + "_pct"] = (
                    df[col].astype(float) / df["TASKS"].astype(float)
                ) * 100
                df[col + "_pct"] = df[[col + "_pct"]].round(1)
                df[col] = (
                    df[col].astype(int).astype(str)
                    + " ("
                    + df[col + "_pct"].astype(str)
                    + "%)"
                )

            # final order
            df = df[["TASKS"] + _cli_order + ["RETRIES"]]
            df = df.reset_index()
            df_json = df.to_json()
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
            df_json = df

        return WorkflowStatusResponse(workflows=df_json)

    def get_workflow_status_viz(self, workflow_ids: List[int]) -> Dict[int, Any]:
        """Get the status of the workflows for GUI."""
        wf_ids = workflow_ids
        # return DS
        return_dic: Dict[int, Any] = dict()
        for wf_id in wf_ids:
            attempts_sql = select(
                func.coalesce(func.min(Task.num_attempts), 0).label("min"),
                func.coalesce(func.max(Task.num_attempts), 0).label("max"),
                func.coalesce(func.avg(Task.num_attempts), 0.0).label("mean"),
            ).where(Task.workflow_id == wf_id)
            attempts = self.session.execute(attempts_sql).first()

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

        query_filter = [
            Task.workflow_id.in_(wf_ids),
            Task.workflow_id == Workflow.id,
        ]
        status_sql: Select[Tuple[int, str, int]] = select(
            Task.workflow_id, Task.status, Workflow.max_concurrently_running
        ).where(*query_filter)
        rows = self.session.execute(status_sql).all()

        for row in rows:
            return_dic[row[0]]["tasks"] += 1
            return_dic[row[0]][_cli_label_mapping[row[1]]] += 1
            return_dic[row[0]]["MAXC"] = row[2]

        return return_dic

    def _add_multi_value_filter(
        self,
        value: Optional[str],
        column: str,
        param_name: str,
        where_clauses: list,
        substitution_dict: dict,
    ) -> None:
        """Add a filter that supports comma-separated values with OR logic."""
        if not value:
            return

        value_list = [v.strip() for v in value.split(",") if v.strip()]
        if not value_list:
            return

        if len(value_list) == 1:
            where_clauses.append(f"{column} = :{param_name}")
            substitution_dict[param_name] = value_list[0]
        else:
            placeholders = ",".join([f":{param_name}_{i}" for i in range(len(value_list))])
            where_clauses.append(f"{column} IN ({placeholders})")
            for i, v in enumerate(value_list):
                substitution_dict[f"{param_name}_{i}"] = v

    def get_workflow_overview(
        self,
        user: Optional[str] = None,
        tool: Optional[str] = None,
        wf_name: Optional[str] = None,
        wf_args: Optional[str] = None,
        wf_attribute_value: Optional[str] = None,
        wf_attribute_key: Optional[str] = None,
        wf_id: Optional[str] = None,
        date_submitted: Optional[str] = None,
        date_submitted_end: Optional[str] = None,
        status: Optional[str] = None,
    ) -> WorkflowOverviewResponse:
        """Fetch associated workflows and workflow runs by username."""
        where_clauses = []
        substitution_dict = {}

        self._add_multi_value_filter(
            user, "workflow_run.user", "user", where_clauses, substitution_dict
        )
        self._add_multi_value_filter(
            tool, "tool.name", "tool", where_clauses, substitution_dict
        )
        self._add_multi_value_filter(
            status, "workflow.status", "status", where_clauses, substitution_dict
        )

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
        rows = self.session.execute(query, substitution_dict).all()

        def serialize_datetime(obj: Union[datetime, str]) -> str:
            """Serialize datetime objects into string format."""
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, str):
                # Handle case where database returns datetime as string (e.g., SQLite)
                return obj
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

        workflows = []
        for row in rows:
            workflow_data = dict(zip(column_names, row))
            workflow_data.update(initial_status_counts)
            workflow_data["wf_submitted_date"] = serialize_datetime(row[2])
            workflow_data["wf_status_date"] = serialize_datetime(row[3])
            workflows.append(WorkflowOverviewItem(**workflow_data))

        return WorkflowOverviewResponse(workflows=workflows)

    def get_task_details_by_workflow_id(
        self, workflow_id: int, tt_name: str
    ) -> TaskTableResponse:
        """Fetch Task details associated with Workflow ID and TaskTemplate name."""
        task_template_name = tt_name
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
        rows = self.session.execute(sql).all()

        column_names = (
            "task_id",
            "task_name",
            "task_status",
            "task_command",
            "task_num_attempts",
            "task_status_date",
            "task_max_attempts",
        )

        tasks = []
        for row in rows:
            task_data = dict(zip(column_names, row))
            task_data["task_status"] = _cli_label_mapping[task_data["task_status"]]
            task_data["task_status_date"] = str(task_data["task_status_date"])
            tasks.append(TaskTableItem(**task_data))

        return TaskTableResponse(tasks=tasks)

    def get_workflow_details_by_id(self, workflow_id: int) -> List[WorkflowDetailsItem]:
        """Fetch name, args, dates, tool for a Workflow provided WF ID."""
        latest_workflow_run_subquery = (
            self.session.query(
                WorkflowRun.workflow_id, func.max(WorkflowRun.heartbeat_date)
            )
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
        rows = self.session.execute(sql).all()

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

        # Convert to Pydantic models
        workflow_details = [WorkflowDetailsItem(**row) for row in result]
        return workflow_details

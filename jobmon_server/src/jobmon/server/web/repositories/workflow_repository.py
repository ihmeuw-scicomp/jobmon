from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import structlog
from sqlalchemy import Select, func, select, update
from sqlalchemy.orm import Session, aliased

from jobmon.core.constants import WorkflowStatus as Statuses
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_status import TaskStatus
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import (
    TaskTemplateVersion,
)
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import (
    WorkflowAttribute,
)
from jobmon.server.web.models.workflow_attribute_type import (
    WorkflowAttributeType,
)
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_status import WorkflowStatus
from jobmon.server.web.schemas.workflow import (
    TaskTableItem,
    TaskTableResponse,
    WorkflowDetailsItem,
    WorkflowOverviewFilters,
    WorkflowOverviewItem,
    WorkflowOverviewResponse,
    WorkflowRunForResetResponse,
    WorkflowStatusResponse,
    WorkflowTasksResponse,
    WorkflowUserValidationResponse,
    WorkflowValidationResponse,
)
from jobmon.server.web.services.transition_service import TransitionService

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
        invalid_statuses = [TaskStatus.REGISTERING]
        if partial_reset:
            invalid_statuses.append(TaskStatus.DONE)

        # Get tasks that will be reset for audit logging
        tasks_to_reset = self.session.execute(
            select(Task.id, Task.status).where(
                Task.workflow_id == workflow_id,
                Task.status.notin_(invalid_statuses),
            )
        ).all()

        if tasks_to_reset:
            # Update tasks
            update_filter = [
                Task.workflow_id == workflow_id,
                Task.status.notin_(invalid_statuses),
            ]
            update_stmt = (
                update(Task)
                .where(*update_filter)
                .values(
                    status=TaskStatus.REGISTERING,
                    status_date=func.now(),
                    num_attempts=0,
                )
            )
            self.session.execute(update_stmt)

            # Create audit records for the reset (properly closes previous records)
            audit_records = [
                {
                    "task_id": task_id,
                    "workflow_id": workflow_id,
                    "previous_status": prev_status,
                    "new_status": TaskStatus.REGISTERING,
                }
                for task_id, prev_status in tasks_to_reset
            ]
            TransitionService.create_audit_records_bulk(
                session=self.session, records=audit_records
            )

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

    @staticmethod
    def _build_csv_condition(
        value: Optional[str],
        column: Any,
        exclude: bool = False,
    ) -> Optional[Any]:
        """Return an ORM condition for comma-separated values.

        Args:
            value: Comma-separated string of values to filter.
            column: SQLAlchemy column to filter on.
            exclude: If True, use NOT IN / != logic.

        Returns:
            A SQLAlchemy BinaryExpression, or None.
        """
        if not value:
            return None
        value_list = [v.strip() for v in value.split(",") if v.strip()]
        if not value_list:
            return None
        if exclude:
            if len(value_list) == 1:
                return column != value_list[0]
            return column.notin_(value_list)
        if len(value_list) == 1:
            return column == value_list[0]
        return column.in_(value_list)

    @staticmethod
    def _build_text_filter(
        value: Optional[str],
        column: Any,
        contains: bool = False,
    ) -> Optional[Any]:
        """Build a text filter with optional wildcard/contains.

        Wildcards: ``*`` in *value* is converted to SQL ``%``.
        Contains:  wraps the value in ``%…%`` for substring
        matching.

        Returns:
            A SQLAlchemy BinaryExpression, or None.
        """
        if not value:
            return None
        # Escape SQL LIKE metacharacters before converting
        # user-facing wildcards.
        escaped = value.replace("%", "\\%").replace("_", "\\_")
        if contains:
            pattern = escaped.replace("*", "%")
            return column.like(f"%{pattern}%")
        if "*" in value:
            return column.like(escaped.replace("*", "%"))
        return column == value

    def get_workflow_overview(
        self,
        f: Optional[WorkflowOverviewFilters] = None,
    ) -> WorkflowOverviewResponse:
        """Fetch workflow overview filtered by the given criteria.

        The query is structured as three layers:
        1. Inner subquery: find workflow IDs matching filters.
        2. Middle subquery: find distinct (queue, workflow)
           pairs via task → task_resources.
        3. Outer query: join back to workflow for final
           SELECT / GROUP BY / ORDER BY.
        """
        if f is None:
            f = WorkflowOverviewFilters()

        # -- build filter conditions for the inner subquery --
        filters: List[Any] = []

        for val, col, exc in [
            (f.user, WorkflowRun.user, False),
            (f.tool, Tool.name, False),
            (f.status, Workflow.status, False),
            (f.user_exclude, WorkflowRun.user, True),
            (f.tool_exclude, Tool.name, True),
            (f.status_exclude, Workflow.status, True),
        ]:
            cond = self._build_csv_condition(val, col, exc)
            if cond is not None:
                filters.append(cond)

        cond = self._build_text_filter(
            f.wf_name,
            Workflow.name,
            f.wf_name_contains,
        )
        if cond is not None:
            filters.append(cond)

        cond = self._build_text_filter(
            f.wf_args,
            Workflow.workflow_args,
            f.wf_args_contains,
        )
        if cond is not None:
            filters.append(cond)

        if f.wf_id:
            filters.append(Workflow.id == int(f.wf_id))
        if f.date_submitted:
            filters.append(Workflow.created_date >= f.date_submitted)
        if f.date_submitted_end:
            filters.append(Workflow.created_date <= f.date_submitted_end)

        # -- Layer 1: inner subquery (matching workflow IDs) --
        inner_q = (
            select(WorkflowRun.workflow_id)
            .select_from(Workflow)
            .join(
                ToolVersion,
                Workflow.tool_version_id == ToolVersion.id,
            )
            .join(Tool, ToolVersion.tool_id == Tool.id)
            .join(
                WorkflowRun,
                Workflow.id == WorkflowRun.workflow_id,
            )
        )

        # Attribute joins: only added when filters require them.
        # Merge legacy single-pair params into the list.
        attrs = list(f.wf_attributes or [])
        if f.wf_attribute_key and f.wf_attribute_value:
            attrs.append((f.wf_attribute_key, f.wf_attribute_value))
        elif f.wf_attribute_key:
            attrs.append((f.wf_attribute_key, ""))
        elif f.wf_attribute_value:
            attrs.append(("", f.wf_attribute_value))

        if attrs:
            for i, (attr_key, attr_val) in enumerate(attrs):
                wa = aliased(WorkflowAttribute, name=f"wa{i}")
                wat = aliased(WorkflowAttributeType, name=f"wat{i}")
                inner_q = inner_q.join(wa, Workflow.id == wa.workflow_id)
                inner_q = inner_q.join(
                    wat,
                    wa.workflow_attribute_type_id == wat.id,
                )
                if attr_key:
                    filters.append(wat.name == attr_key)
                if attr_val:
                    filters.append(wa.value == attr_val)

        if filters:
            inner_q = inner_q.where(*filters)

        # -- Layer 2: queue subquery --
        workflow_queue_sq = (
            select(
                TaskResources.queue_id.label("queue_id"),
                Task.workflow_id.label("workflow_id"),
            )
            .select_from(Task)
            .join(
                TaskResources,
                TaskResources.id == Task.task_resources_id,
            )
            .where(Task.workflow_id.in_(inner_q))
            .group_by(Task.workflow_id, TaskResources.queue_id)
        ).subquery("workflow_queue")

        # -- Layer 3: outer query --
        stmt = (
            select(
                Workflow.id,
                Workflow.name,
                Workflow.created_date,
                Workflow.status_date,
                Workflow.workflow_args,
                func.count(func.distinct(WorkflowRun.id)).label("num_attempts"),
                WorkflowStatus.label,
                Tool.name,
            )
            .select_from(Workflow)
            .join(
                workflow_queue_sq,
                Workflow.id == workflow_queue_sq.c.workflow_id,
            )
            .join(
                Queue,
                Queue.id == workflow_queue_sq.c.queue_id,
            )
            .join(
                WorkflowRun,
                Workflow.id == WorkflowRun.workflow_id,
            )
            .join(
                ToolVersion,
                Workflow.tool_version_id == ToolVersion.id,
            )
            .join(Tool, ToolVersion.tool_id == Tool.id)
            .join(
                WorkflowStatus,
                WorkflowStatus.id == Workflow.status,
            )
            .where(Queue.cluster_id != 1)
            .group_by(Workflow.id)
            .order_by(Workflow.id.desc())
        )

        rows = self.session.execute(stmt).all()

        # -- serialise results --
        def serialize_datetime(
            obj: Union[datetime, str],
        ) -> str:
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, str):
                return obj
            raise TypeError(f"Type {obj.__class__.__name__} " "not serializable")

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
        initial_status_counts = {lbl: 0 for lbl in set(_cli_label_mapping.values())}

        workflows = []
        for row in rows:
            wf = dict(zip(column_names, row))
            wf.update(initial_status_counts)
            wf["wf_submitted_date"] = serialize_datetime(row[2])
            wf["wf_status_date"] = serialize_datetime(row[3])
            workflows.append(WorkflowOverviewItem(**wf))

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
                WorkflowRun.workflow_id, func.max(WorkflowRun.id).label("max_wfr_id")
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
                WorkflowRun.id.label("wfr_id"),
            )
            .select_from(Workflow)
            .join(ToolVersion, Workflow.tool_version_id == ToolVersion.id)
            .join(Tool, ToolVersion.tool_id == Tool.id)
            .join(WorkflowStatus, WorkflowStatus.id == Workflow.status)
            .join(
                latest_workflow_run_subquery,
                latest_workflow_run_subquery.c.workflow_id == Workflow.id,
            )
            .join(
                WorkflowRun,
                WorkflowRun.id == latest_workflow_run_subquery.c.max_wfr_id,
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
            "wfr_id",
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

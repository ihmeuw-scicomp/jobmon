"""Routes for TaskTemplate."""

import json
from decimal import Decimal
from http import HTTPStatus as StatusCodes
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd  # type:ignore
import structlog
from fastapi import Depends, HTTPException, Query
from sqlalchemy import Row, Select, and_, join, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from starlette.responses import JSONResponse

from jobmon.server.web.db import get_dialect_name, get_sessionmaker
from jobmon.server.web.db.deps import get_db
from jobmon.server.web.error_log_clustering import cluster_error_logs
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.repositories.task_template_repository import (
    TaskTemplateRepository,
)
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.routes.v3.cli.workflow import _cli_label_mapping
from jobmon.server.web.schemas.task_template import (
    TaskResourceVizItem,
    TaskTemplateResourceUsageRequest,
    TaskTemplateResourceUsageResponse,
)

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)
SessionMaker = get_sessionmaker()
DIALECT = get_dialect_name()


@api_v3_router.get("/get_task_template_details")
def get_task_template_details_for_workflow(
    workflow_id: int = Query(..., ge=1),
    task_template_id: int = Query(..., ge=1),
) -> Any:
    """Fetch Task Template details (ID, Name, and Version) for a given Workflow."""
    with SessionMaker() as session:
        sql = (
            select(
                TaskTemplate.id,
                TaskTemplate.name,
                TaskTemplateVersion.id.label("task_template_version_id"),
            )
            .select_from(
                join(
                    join(
                        join(Task, Node, Task.node_id == Node.id),
                        TaskTemplateVersion,
                        Node.task_template_version_id == TaskTemplateVersion.id,
                    ),
                    TaskTemplate,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                )
            )
            .where(
                and_(
                    Task.workflow_id == workflow_id, TaskTemplate.id == task_template_id
                )
            )
            .distinct()
            .limit(1)
        )

        row = session.execute(sql).one_or_none()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail="Task Template not found for the given workflow.",
            )

        tt_details_data = {
            "task_template_id": row.id,
            "task_template_name": row.name,
            "task_template_version_id": row.task_template_version_id,
        }

        return JSONResponse(content=tt_details_data, status_code=StatusCodes.OK)


@api_v3_router.get("/get_task_template_version")
def get_task_template_version_for_tasks(
    task_id: Optional[int] = None, workflow_id: Optional[int] = None
) -> Any:
    """Get the task_template_version_ids."""
    # parse args
    t_id = task_id
    wf_id = workflow_id
    # This route only accept one task id or one wf id;
    # If provided both, ignor wf id
    with SessionMaker() as session:
        with session.begin():
            if t_id:
                query_filter = [
                    Task.id == t_id,
                    Task.node_id == Node.id,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                ]
                sql = select(
                    TaskTemplateVersion.id,
                    TaskTemplate.name,
                ).where(*query_filter)

            else:
                query_filter = [
                    Task.workflow_id == wf_id,
                    Task.node_id == Node.id,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                ]
                sql = (
                    select(
                        TaskTemplateVersion.id,
                        TaskTemplate.name,
                    ).where(*query_filter)
                ).distinct()
            rows = session.execute(sql).all()
        column_names = ("id", "name")
        ttvis = [dict(zip(column_names, ti)) for ti in rows]
        resp = JSONResponse(
            content={"task_template_version_ids": ttvis}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.get("/get_requested_cores")
def get_requested_cores(task_template_version_ids: Optional[str] = None) -> Any:
    """Get the min, max, and arg of requested cores."""
    # parse args
    ttvis = task_template_version_ids
    if ttvis is None:
        raise ValueError(
            "No task_template_version_ids returned in /get_requested_cores"
        )
    ttvis = [int(i) for i in ttvis[1:-1].split(",")]  # type: ignore
    # null core should be treated as 1 instead of 0
    with SessionMaker() as session:
        with session.begin():
            query_filter = [
                TaskTemplateVersion.id.in_(ttvis),
                TaskTemplateVersion.id == Node.task_template_version_id,
                Task.node_id == Node.id,
                Task.task_resources_id == TaskResources.id,
            ]

            sql = select(
                TaskTemplateVersion.id, TaskResources.requested_resources
            ).where(*query_filter)
        rows_raw = session.execute(sql).all()
        column_names = ("id", "rr")
        rows: List[Dict[str, Any]] = [dict(zip(column_names, ti)) for ti in rows_raw]

        core_info = []
        if rows:
            result_dir: Dict = dict()
            for r in rows:
                # json loads hates single quotes
                j_str = r["rr"].replace("'", '"')  # type: ignore
                j_dir = json.loads(j_str)
                core = 1 if "num_cores" not in j_dir.keys() else int(j_dir["num_cores"])
                if r["id"] in result_dir.keys():  # type: ignore
                    result_dir[r["id"]].append(core)  # type: ignore
                else:
                    result_dir[r["id"]] = [core]  # type: ignore
            for k in result_dir.keys():
                item_min = int(np.min(result_dir[k]))
                item_max = int(np.max(result_dir[k]))
                item_mean = round(np.mean(result_dir[k]))
                core_info.append(
                    {"id": k, "min": item_min, "max": item_max, "avg": item_mean}
                )
        resp = JSONResponse(
            content={"core_info": core_info}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.get("/get_most_popular_queue")
def get_most_popular_queue(
    task_template_version_ids: Optional[str] = Query(...),
) -> Any:
    """Get the most popular queue of the task template."""
    # parse args
    ttvis = task_template_version_ids
    if ttvis is None:
        raise ValueError(
            "No task_template_version_ids returned in /get_most_popular_queue."
        )
    ttvis = [int(i) for i in ttvis[1:-1].split(",")]  # type: ignore
    with SessionMaker() as session:
        with session.begin():
            query_filter = [
                TaskTemplateVersion.id.in_(ttvis),
                TaskTemplateVersion.id == Node.task_template_version_id,
                Task.node_id == Node.id,
                TaskInstance.task_id == Task.id,
                TaskInstance.task_resources_id == TaskResources.id,
                TaskResources.queue_id.isnot(None),
            ]
            sql = select(TaskTemplateVersion.id, TaskResources.queue_id).where(
                *query_filter
            )

        rows_raw = session.execute(sql).all()
        column_names = ("id", "queue_id")
        rows: List[Dict[str, Any]] = [dict(zip(column_names, ti)) for ti in rows_raw]
        # return a "standard" json format for cli routes
        queue_info = []
        if rows:
            result_dir: Dict = dict()
            for r in rows:
                ttvi = r["id"]  # type: ignore
                q = r["queue_id"]  # type: ignore
                if ttvi in result_dir.keys():
                    if q in result_dir[ttvi].keys():
                        result_dir[ttvi][q] += 1
                    else:
                        result_dir[ttvi][q] = 1
                else:
                    result_dir[ttvi] = dict()
                    result_dir[ttvi][q] = 1
            for ttvi in result_dir.keys():
                # assign to a variable to keep typecheck happy
                max_usage = 0
                for q in result_dir[ttvi].keys():
                    if result_dir[ttvi][q] > max_usage:
                        popular_q = q
                        max_usage = result_dir[ttvi][q]
                # get queue name; and return queue id with it
                with session:
                    query_filter = [Queue.id == popular_q]
                    sql1: Select[Tuple[str]] = select(Queue.name).where(*query_filter)
                popular_q_name = session.execute(sql1).one()[0]
                queue_info.append(
                    {"id": ttvi, "queue": popular_q_name, "queue_id": popular_q}
                )
        resp = JSONResponse(
            content={"queue_info": queue_info}, status_code=StatusCodes.OK
        )
    return resp


@api_v3_router.post(
    "/task_template_resource_usage", response_model=TaskTemplateResourceUsageResponse
)
async def get_task_template_resource_usage(
    request_data: TaskTemplateResourceUsageRequest, db: Session = Depends(get_db)
) -> TaskTemplateResourceUsageResponse:
    """Unified endpoint for task template resource usage.

    Returns modern Pydantic models suitable for both GUI frontend
    and Python client consumption with full type safety.
    """
    repo = TaskTemplateRepository(db)

    try:
        # Get task details using the repository
        task_details = repo.get_task_resource_details(
            task_template_version_id=request_data.task_template_version_id,
            workflows=request_data.workflows,
            node_args=request_data.node_args,
        )

        # Calculate statistics using repository method
        stats = repo.calculate_resource_statistics(
            task_details=task_details,
            confidence_interval=request_data.ci,
            task_template_version_id=request_data.task_template_version_id,
        )

        # Prepare viz data if requested
        viz_data = None
        if request_data.viz and task_details:
            viz_data = []
            for detail_item in task_details:
                viz_data.append(
                    TaskResourceVizItem(
                        r=detail_item.r,
                        m=detail_item.m,
                        node_id=detail_item.node_id,
                        task_id=detail_item.task_id,
                        requested_resources=detail_item.requested_resources,
                        attempt_number_of_instance=detail_item.attempt_number_of_instance,
                        status=detail_item.status,
                    )
                )

        # Return unified Pydantic response
        return TaskTemplateResourceUsageResponse(
            num_tasks=stats.num_tasks,
            min_mem=stats.min_mem,
            max_mem=stats.max_mem,
            mean_mem=stats.mean_mem,
            min_runtime=stats.min_runtime,
            max_runtime=stats.max_runtime,
            mean_runtime=stats.mean_runtime,
            median_mem=stats.median_mem,
            median_runtime=stats.median_runtime,
            ci_mem=stats.ci_mem,
            ci_runtime=stats.ci_runtime,
            result_viz=viz_data,
        )

    except Exception as e:
        logger.error(f"Error fetching resource usage: {e}")
        raise HTTPException(
            status_code=StatusCodes.INTERNAL_SERVER_ERROR,
            detail="Error processing resource usage data.",
        ) from e


@api_v3_router.get("/workflow_tt_status_viz/{workflow_id}")
def get_workflow_tt_status_viz(workflow_id: int) -> Any:
    """Get the status of the workflows for GUI."""
    # return DS
    return_dic: Dict[int, Any] = dict()

    def serialize_decimal(value: Union[Decimal, float]) -> float:
        """Convert Decimal to float for JSON serialization."""
        if isinstance(value, Decimal):
            return float(value)
        return value

    with SessionMaker() as session:
        with session.begin():
            # user subquery as the Array table has to be joined on two columns
            sub_query = (
                select(
                    Array.id,
                    Array.task_template_version_id,
                    Array.max_concurrently_running,
                ).where(Array.workflow_id == workflow_id)
            ).subquery()
            join_table = (
                Task.__table__.join(Node, Task.node_id == Node.id)
                .join(
                    TaskTemplateVersion,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                )
                .join(
                    TaskTemplate,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                )
                # Arrays were introduced in 3.1.0, hence the outer-join for 3.0.* workflows
                .join(
                    sub_query,
                    sub_query.c.task_template_version_id == TaskTemplateVersion.id,
                    isouter=True,
                )
            )
            # Order by the task submitted date in each task template
            sql = (
                select(
                    TaskTemplate.id,
                    TaskTemplate.name,
                    Task.id,
                    Task.status,
                    sub_query.c.max_concurrently_running,
                    TaskTemplateVersion.id,
                )
                .select_from(join_table)
                .where(Task.workflow_id == workflow_id)
                .order_by(Task.id)
            )
            # Find all task by these task templates
            # Uses STRAIGHT_JOIN to ensure the join order for large queries, where
            # the optimizer may choose a suboptimal execution plan for large datasets.
            # Has to be conditional since not all database engines support STRAIGHT_JOIN.
            if DIALECT == "mysql":
                sql = sql.prefix_with("STRAIGHT_JOIN")
            rows = session.execute(sql).all()
            session.flush()

            join_table = (
                Task.__table__.join(Node, Task.node_id == Node.id)
                .join(
                    TaskTemplateVersion,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                )
                .join(
                    TaskTemplate,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                )
            )
            sql = (
                select(
                    TaskTemplate.id.label("task_template_id"),
                    TaskTemplate.name.label("task_template_name"),
                    func.min(Task.num_attempts).label("min"),
                    func.max(Task.num_attempts).label("max"),
                    func.avg(Task.num_attempts).label("mean"),
                )
                .select_from(join_table)
                .where(Task.workflow_id == workflow_id)
                .group_by(TaskTemplate.id)
            )
            if DIALECT == "mysql":
                sql = sql.prefix_with("STRAIGHT_JOIN")
            attempts0 = session.execute(sql).all()

        attempts: Dict[Any, Row[Any]] = {attempt[0]: attempt for attempt in attempts0}

        for r in rows:
            # Avoiding magic numbers
            task_template_id: str = r[0]
            task_template_name: str = r[1]
            task_status: str = r[3]
            max_concurrently = r[4]
            task_template_version_id: int = int(r[5])

            if int(task_template_id) in return_dic.keys():
                pass
            else:
                attempt = attempts.get(task_template_id)
                *_, min_, max_, mean = attempt if attempt else (None, None, None, None)

                return_dic[int(task_template_id)] = {
                    "id": int(task_template_id),
                    "name": task_template_name,
                    "tasks": 0,
                    "PENDING": 0,
                    "SCHEDULED": 0,
                    "RUNNING": 0,
                    "DONE": 0,
                    "FATAL": 0,
                    "MAXC": 0,
                    "num_attempts_min": min_,
                    "num_attempts_max": max_,
                    "num_attempts_avg": mean,
                    "task_template_version_id": task_template_version_id,
                }
            return_dic[int(task_template_id)]["tasks"] += 1
            return_dic[int(task_template_id)][_cli_label_mapping[task_status]] += 1
            return_dic[int(task_template_id)]["MAXC"] = (
                max_concurrently if max_concurrently is not None else "NA"
            )

        return_dic_serializable = {
            key: {
                k: serialize_decimal(v) if isinstance(v, (Decimal, float)) else v
                for k, v in val.items()
            }
            for key, val in return_dic.items()
        }

        resp = JSONResponse(content=return_dic_serializable, status_code=StatusCodes.OK)
    return resp


@api_v3_router.get("/tt_error_log_viz/{wf_id}/{tt_id}")
@api_v3_router.get("/tt_error_log_viz/{wf_id}/{tt_id}/{ti_id}")
def get_tt_error_log_viz(
    wf_id: int,
    tt_id: Optional[int] = None,
    ti_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 10,
    just_recent_errors: str = "false",
    cluster_errors: str = "false",
) -> Any:
    """Get the error logs for a task template id for GUI."""
    return_list: List[Any] = []
    recent_errors = just_recent_errors.lower() == "true"
    output_clustered_errors = cluster_errors.lower() == "true"
    offset = (page - 1) * page_size

    with SessionMaker() as session:
        with session.begin():
            query_filter = [
                TaskTemplateVersion.task_template_id == tt_id,
                Task.workflow_id == wf_id,
            ]

            where_conditions = query_filter[:]
            if recent_errors:
                where_conditions.extend(
                    [
                        (
                            TaskInstance.id
                            == select(func.max(TaskInstance.id))
                            .where(TaskInstance.task_id == Task.id)
                            .correlate(Task)
                            .scalar_subquery()
                        ),
                        (
                            TaskInstance.workflow_run_id
                            == select(func.max(WorkflowRun.id))
                            .where(WorkflowRun.workflow_id == Task.workflow_id)
                            .correlate(Task)
                            .scalar_subquery()
                        ),
                    ]
                )
            if ti_id:
                where_conditions.extend(
                    [
                        (TaskInstance.id == ti_id),
                    ]
                )

            total_count_query = (
                select(func.count(TaskInstanceErrorLog.id))
                .join_from(
                    TaskInstanceErrorLog,
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .join_from(TaskInstance, Task, TaskInstance.task_id == Task.id)
                .join_from(
                    TaskInstance,
                    WorkflowRun,
                    TaskInstance.workflow_run_id == WorkflowRun.id,
                )
                .join_from(Task, Node, Task.node_id == Node.id)
                .join_from(
                    Node,
                    TaskTemplateVersion,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                )
                .join_from(
                    TaskTemplateVersion,
                    TaskTemplate,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                )
                .where(*where_conditions)
            )
            total_count = session.execute(total_count_query).scalar()

            sql = (
                select(
                    Task.id,
                    TaskInstance.id,
                    TaskInstanceErrorLog.id,
                    TaskInstanceErrorLog.error_time,
                    TaskInstanceErrorLog.description,
                    TaskInstance.stderr_log,
                    TaskInstance.workflow_run_id,
                    Task.workflow_id,
                )
                .join_from(
                    TaskInstanceErrorLog,
                    TaskInstance,
                    TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
                )
                .join_from(TaskInstance, Task, TaskInstance.task_id == Task.id)
                .join_from(
                    TaskInstance,
                    WorkflowRun,
                    TaskInstance.workflow_run_id == WorkflowRun.id,
                )
                .join_from(Task, Node, Task.node_id == Node.id)
                .join_from(
                    Node,
                    TaskTemplateVersion,
                    Node.task_template_version_id == TaskTemplateVersion.id,
                )
                .join_from(
                    TaskTemplateVersion,
                    TaskTemplate,
                    TaskTemplateVersion.task_template_id == TaskTemplate.id,
                )
                .where(*where_conditions)
                .order_by(TaskInstanceErrorLog.id.desc())
                .offset(offset)
                .limit(page_size)
            )

            rows = session.execute(sql).all()
            session.commit()

        for r in rows:
            return_list.append(
                {
                    "task_id": r[0],
                    "task_instance_id": r[1],
                    "task_instance_err_id": r[2],
                    "error_time": str(r[3]),
                    "error": str(r[4]),
                    "task_instance_stderr_log": str(r[5]),
                    "workflow_run_id": r[6],
                    "workflow_id": r[7],
                }
            )
        errors_df = pd.DataFrame(return_list)

        if output_clustered_errors:
            if errors_df.shape[0] > 0:
                errors_df = cluster_error_logs(errors_df)
            total_count = errors_df.shape[0]
            return_dict = {
                "error_logs": errors_df.to_dict(orient="records"),
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
            }
        else:
            return_dict = {
                "error_logs": errors_df.to_dict(orient="records"),
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
            }
        resp = JSONResponse(content=return_dict, status_code=StatusCodes.OK)
    return resp

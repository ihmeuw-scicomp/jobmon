"""Routes for TaskTemplate."""

from decimal import Decimal
from http import HTTPStatus as StatusCodes
import json
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import HTTPException, Query, Request
import numpy as np
import pandas as pd  # type:ignore
import scipy.stats as st  # type:ignore
from sqlalchemy import Row, Select, select
from sqlalchemy.sql import func
from starlette.responses import JSONResponse
import structlog

from jobmon.core.serializers import SerializeTaskTemplateResourceUsage
from jobmon.server.web.db import get_dialect_name, get_sessionmaker
from jobmon.server.web.error_log_clustering import cluster_error_logs
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.array import Array
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router
from jobmon.server.web.routes.v3.cli.workflow import _cli_label_mapping
from jobmon.server.web.server_side_exception import InvalidUsage

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
        with session.begin():
            query_filter = [
                Task.workflow_id == workflow_id,
                Task.node_id == Node.id,
                Node.task_template_version_id == TaskTemplateVersion.id,
                TaskTemplateVersion.task_template_id == task_template_id,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            ]

            sql = (
                select(
                    TaskTemplate.id,
                    TaskTemplate.name,
                    TaskTemplateVersion.id.label("task_template_version_id"),
                )
                .where(*query_filter)
                .distinct()
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


@api_v3_router.post("/task_template_resource_usage")
async def get_task_template_resource_usage(request: Request) -> Any:
    """Return the aggregate resource usage for a give TaskTemplate.

    Need to use cross_origin decorator when using the GUI to call a post route.
    This enables Cross Origin Resource Sharing (CORS) on the route. Default is
    most permissive settings.
    """
    data = await request.json()
    try:
        task_template_version_id = data.pop("task_template_version_id")
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to /task_template_resource_usage", status_code=400
        ) from e

    workflows = data.pop("workflows", None)
    node_args = data.pop("node_args", None)
    ci = data.pop("ci", None)
    viz = bool(data.pop("viz", False))

    # Initialize resp to ensure it's defined even if the with block fails early
    resp = JSONResponse(content={}, status_code=StatusCodes.INTERNAL_SERVER_ERROR)
    with SessionMaker() as session:
        with session.begin():
            query_filter = [
                TaskTemplateVersion.id == task_template_version_id,
                Task.status == "D",
                TaskInstance.status == "D",
                TaskTemplateVersion.id == Node.task_template_version_id,
                Node.id == Task.node_id,
                Task.id == TaskInstance.task_id,
            ]
            if workflows:
                query_filter += [
                    TaskInstance.workflow_run_id == WorkflowRun.id,
                    WorkflowRun.workflow_id == Workflow.id,
                    Workflow.id.in_(workflows),
                ]
            sql = select(
                TaskInstance.wallclock, TaskInstance.maxrss, Node.id, Task.id
            ).where(*query_filter)
            rows_raw = session.execute(sql).all()
            session.commit()
        column_names = ("r", "m", "node_id", "task_id")
        rows: List[Dict[str, Any]] = [dict(zip(column_names, ti)) for ti in rows_raw]
        result = []
        if rows:
            for r in rows:
                if r["r"] is None:  # type: ignore
                    r["r"] = 0
                if node_args:
                    session = SessionMaker()
                    with session.begin():
                        node_f = [
                            NodeArg.arg_id == Arg.id,
                            NodeArg.node_id == r["node_id"],
                        ]  # type: ignore
                        node_s = select(Arg.name, NodeArg.val).where(*node_f)
                        node_rows = session.execute(node_s).all()
                        session.commit()
                    _include = False
                    for n in node_rows:
                        if not _include:
                            if n[0] in node_args.keys() and n[1] in node_args[n[0]]:
                                _include = True
                    if _include:
                        result.append(r)
                else:
                    result.append(r)

        if len(result) == 0:
            resource_usage = SerializeTaskTemplateResourceUsage.to_wire(
                None, None, None, None, None, None, None, None, None, None, None
            )
        else:
            runtimes = []
            mems = []
            for row in result:
                runtimes.append(int(row["r"]))  # type: ignore
                mems.append(max(0, 0 if row["m"] is None else int(row["m"])))  # type: ignore

            num_tasks = len(runtimes)
            # set 0 to NaN; thus, numpy ignores them
            if 0 in mems:
                mems.remove(0)
            if 0 in runtimes:
                runtimes.remove(0)
            if len(mems) > 0:
                min_mem = int(np.min(mems))
                max_mem = int(np.max(mems))
                mean_mem = round(float(np.mean(mems)), 2)
                median_mem = round(float(np.percentile(mems, 50)), 2)
            else:
                min_mem = 0
                max_mem = 0
                mean_mem = 0
                median_mem = 0
            if len(runtimes) > 0:
                min_runtime = int(np.min(runtimes))
                max_runtime = int(np.max(runtimes))
                mean_runtime = round(float(np.mean(runtimes)), 2)
                median_runtime = round(float(np.percentile(runtimes, 50)), 2)
            else:
                min_runtime = 0
                max_runtime = 0
                mean_runtime = 0
                median_runtime = 0

            if ci is None:
                ci_mem = [None, None]
                ci_runtime = [None, None]
            else:
                try:
                    ci_float = float(ci)

                    def _calculate_ci(d: List, ci: float) -> List[Any]:
                        # Need at least 2 data points for CI calculation with st.sem
                        if len(d) < 2:
                            return [None, None]
                        try:
                            interval = st.t.interval(
                                confidence=ci,
                                df=len(d) - 1,
                                loc=np.mean(d),
                                scale=st.sem(d),
                            )
                            # Handle potential NaN results from st.sem if variance is zero
                            if np.isnan(interval[0]) or np.isnan(interval[1]):
                                return [None, None]
                            return [
                                round(float(interval[0]), 2),
                                round(float(interval[1]), 2),
                            ]
                        except ValueError as e:
                            logger.warn(
                                f"Unable to convert {ci} to float for CI. Use None. "
                                f"Exception: {str(e)}"
                            )
                            return [None, None]
                        except Exception as e:  # Catch any other unexpected errors
                            logger.warn(
                                f"Unexpected error during CI calculation: {str(e)}"
                            )
                            return [None, None]

                    ci_mem = _calculate_ci(mems, ci_float)
                    ci_runtime = _calculate_ci(runtimes, ci_float)

                except ValueError as e:
                    logger.warn(
                        f"Unable to convert {ci} to float for CI. Use None. "
                        f"Exception: {str(e)}"
                    )
                except Exception as e:  # Catch any other unexpected errors
                    logger.warn(f"Unexpected error during CI calculation: {str(e)}")

            resource_usage = SerializeTaskTemplateResourceUsage.to_wire(
                num_tasks,
                min_mem,
                max_mem,
                mean_mem,
                min_runtime,
                max_runtime,
                mean_runtime,
                median_mem,
                median_runtime,
                ci_mem,
                ci_runtime,
            )

        if viz:
            resource_usage += (result,)
        resp = JSONResponse(content=resource_usage, status_code=StatusCodes.OK)
    # Move the return statement outside the with block
    return resp


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

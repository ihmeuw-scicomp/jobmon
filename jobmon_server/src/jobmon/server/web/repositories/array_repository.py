"""Repository for Array operations."""

from typing import Optional

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from jobmon.server.web.models.array import Array
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.schemas.array import ArrayTaskInstance, ArrayTasksResponse

logger = structlog.get_logger(__name__)


class ArrayRepository:
    def __init__(self, session: Session) -> None:
        """Initialize the ArrayRepository with a database session."""
        self.session = session

    def get_array_tasks(
        self,
        workflow_id: int,
        array_name: str,
        job_name: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ArrayTasksResponse:
        """Get array task instances."""
        query_filters = [
            Task.workflow_id == workflow_id,
            TaskInstance.task_id == Task.id,
            Task.array_id == Array.id,
        ]

        if array_name:
            query_filters.append(Array.name == array_name)

        if job_name:
            query_filters.append(Task.name == job_name)

        select_stmt = select(
            Task.id.label("task_id"),
            Task.name.label("task_name"),
            Array.name.label("array_name"),
            TaskInstance.id.label("task_instance_id"),
            TaskInstance.stdout.label("OUTPUT_PATH"),
            TaskInstance.stderr.label("ERROR_PATH"),
        ).where(*query_filters)

        if limit:
            select_stmt = select_stmt.limit(limit)

        result = self.session.execute(select_stmt).all()

        array_tasks = [
            ArrayTaskInstance(
                task_id=row.task_id,
                task_name=row.task_name,
                array_name=row.array_name,
                task_instance_id=row.task_instance_id,
                OUTPUT_PATH=row.OUTPUT_PATH or "",
                ERROR_PATH=row.ERROR_PATH or "",
            )
            for row in result
        ]

        return ArrayTasksResponse(array_tasks=array_tasks)

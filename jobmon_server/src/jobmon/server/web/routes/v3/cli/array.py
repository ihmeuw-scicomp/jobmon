from typing import Any, Optional

from fastapi import Depends, Query
from sqlalchemy.orm import Session

from jobmon.server.web.db.deps import get_db
from jobmon.server.web.repositories.array_repository import ArrayRepository
from jobmon.server.web.routes.v3.cli import cli_router as api_v3_router


@api_v3_router.get("/array/{workflow_id}/get_array_tasks")
def get_array_task_instances(
    workflow_id: int,
    array_name: str,
    job_name: Optional[str] = Query(None),
    limit: Optional[int] = Query(None),
    db: Session = Depends(get_db),
) -> Any:
    """Return error/output filepaths for task instances filtered by array name.

    The user can also optionally filter by job name as well.

    To avoid overly-large returned results, the user must also pass in a workflow ID.
    """
    array_repo = ArrayRepository(db)
    array_tasks = array_repo.get_array_tasks(workflow_id, array_name, job_name, limit)
    return array_tasks

# Task Status Audit Design

## Inventory of v2 FSM Routes that Modify Task Status

### Direct Task Status Modifications

#### 1. `/task/bind_tasks_no_args` (PUT)
- **File**: `task.py:35`
- **Method**: Direct assignment and `task.reset()` method
- **Status Changes**: 
  - New tasks: `REGISTERING` (initial status)
  - Existing tasks: Reset via `task.reset()` method
- **Type**: Model method + direct assignment

#### 2. `/task/{workflow_id}/set_resume_state` (POST)
- **File**: `task.py:397`
- **Method**: Direct SQL update
- **Status Changes**: `REGISTERING` (bulk update)
- **SQL**: 
  ```python
  update(Task).values(
      status=TaskStatus.REGISTERING,
      num_attempts=0,
      status_date=func.now()
  )
  ```

#### 3. `/array/{array_id}/queue_task_batch` (POST)
- **File**: `array.py:107`
- **Method**: Direct SQL update
- **Status Changes**: `QUEUED`
- **SQL**:
  ```python
  update(Task).values(
      status=TaskStatus.QUEUED,
      status_date=func.now(),
      num_attempts=(Task.num_attempts + 1)
  )
  ```

#### 4. `/array/{array_id}/transition_to_launched` (POST)
- **File**: `array.py:213`
- **Method**: Direct SQL update
- **Status Changes**: `LAUNCHED`
- **SQL**:
  ```python
  update(Task).values(
      status=TaskStatus.LAUNCHED, 
      status_date=func.now()
  )
  ```

#### 5. `/task_instance/instantiate_task_instances` (POST)
- **File**: `task_instance.py:485`
- **Method**: Direct SQL update
- **Status Changes**: `INSTANTIATING`
- **SQL**:
  ```python
  update(Task).values(
      status=constants.TaskStatus.INSTANTIATING, 
      status_date=func.now()
  )
  ```

#### 6. `/array/{array_id}/transition_to_killed` (POST)
- **File**: `array.py:400`
- **Method**: Direct SQL update
- **Status Changes**: `ERROR_FATAL`
- **SQL**:
  ```python
  update(Task).values(
      status=TaskStatus.ERROR_FATAL, 
      status_date=func.now()
  )
  ```

### Indirect Task Status Modifications (via Task Instance Transitions)

#### 7. Task Instance Transitions that Trigger Task Status Changes
- **File**: `task_instance.py` (multiple endpoints)
- **Method**: `task_instance.transition()` → `task.transition()` or `task.transition_after_task_instance_error()`
- **Endpoints**:
  - `/task_instance/{id}/log_running`
  - `/task_instance/{id}/log_done`
  - `/task_instance/{id}/log_error_worker_node`
  - `/task_instance/{id}/log_known_error`
  - `/task_instance/{id}/log_unknown_error`
  - `/task_instance/{id}/log_no_distributor_id`
  - `/task_instance/{id}/log_distributor_id`

## Simple Task Status Audit Model

### Database Schema

```sql
CREATE TABLE task_status_audit (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    task_id INTEGER NOT NULL,
    previous_status VARCHAR(1) NULL,
    new_status VARCHAR(1) NOT NULL,
    changed_at DATETIME NOT NULL DEFAULT NOW(),
    
    FOREIGN KEY (task_id) REFERENCES task(id),
    FOREIGN KEY (previous_status) REFERENCES task_status(id),
    FOREIGN KEY (new_status) REFERENCES task_status(id),
    
    INDEX idx_task_status_audit_task_id (task_id),
    INDEX idx_task_status_audit_changed_at (changed_at)
);
```

### Model Implementation

```python
# jobmon_server/src/jobmon/server/web/models/task_status_audit.py
"""Task Status Audit Database Table."""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from jobmon.server.web.models import Base


class TaskStatusAudit(Base):
    """Task Status Audit Database Table - tracks task status changes."""

    __tablename__ = "task_status_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[int] = mapped_column(Integer, ForeignKey("task.id"), nullable=False)
    previous_status: Mapped[Optional[str]] = mapped_column(String(1), ForeignKey("task_status.id"), nullable=True)
    new_status: Mapped[str] = mapped_column(String(1), ForeignKey("task_status.id"), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), nullable=False)

    # ORM relationships
    task = relationship("Task")

    __table_args__ = (
        Index("idx_task_status_audit_task_id", "task_id"),
        Index("idx_task_status_audit_changed_at", "changed_at"),
    )
```

### Audit Service

```python
# jobmon_server/src/jobmon/server/web/services/task_status_audit_service.py
"""Service for task status audit logging."""

from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, insert

from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.task import Task


class TaskStatusAuditService:
    """Service for logging task status changes."""

    @staticmethod
    def log_status_change(
        session: Session,
        task_id: int,
        previous_status: Optional[str],
        new_status: str
    ) -> None:
        """Log a single task status change."""
        audit_entry = TaskStatusAudit(
            task_id=task_id,
            previous_status=previous_status,
            new_status=new_status
        )
        session.add(audit_entry)

    @staticmethod
    def log_bulk_status_changes(
        session: Session,
        task_ids: List[int],
        new_status: str
    ) -> None:
        """Log bulk status changes by first fetching current statuses."""
        # Get current statuses for all affected tasks
        current_statuses = session.execute(
            select(Task.id, Task.status).where(Task.id.in_(task_ids))
        ).all()
        
        # Create audit entries
        audit_entries = [
            {
                "task_id": task_id,
                "previous_status": current_status,
                "new_status": new_status
            }
            for task_id, current_status in current_statuses
        ]
        
        if audit_entries:
            session.execute(insert(TaskStatusAudit).values(audit_entries))
```

### Enhanced Task Model

```python
# Updates to jobmon_server/src/jobmon/server/web/models/task.py

from sqlalchemy.orm import object_session

class Task(Base):
    # ... existing code ...

    def _log_status_change(self, previous_status: Optional[str], new_status: str) -> None:
        """Log status change to audit table."""
        from jobmon.server.web.services.task_status_audit_service import TaskStatusAuditService
        
        session = object_session(self)
        if session is not None:
            TaskStatusAuditService.log_status_change(
                session=session,
                task_id=self.id,
                previous_status=previous_status,
                new_status=new_status
            )

    def transition(self, new_state: str) -> None:
        """Transition the Task to a new state with audit logging."""
        # Store current state for audit
        previous_status = self.status
        
        # Validate and perform transition
        logger.info(f"Transitioning task from {self.status} to {new_state}")
        self._validate_transition(new_state)
        
        if new_state == TaskStatus.QUEUED:
            self.num_attempts = self.num_attempts + 1
            
        self.status = new_state
        self.status_date = func.now()
        
        # Log the status change
        self._log_status_change(previous_status, new_state)

    def reset(
        self, 
        name: str, 
        command: str, 
        max_attempts: int, 
        reset_if_running: bool
    ) -> None:
        """Reset status and number of attempts on a Task with audit logging."""
        # Store current state for audit
        previous_status = self.status
        
        # Only reset undone tasks
        if self.status != TaskStatus.DONE:
            if self.status != TaskStatus.RUNNING or reset_if_running:
                self.status = TaskStatus.REGISTERING
                self.num_attempts = 0
                self.name = name
                self.command = command
                self.max_attempts = max_attempts
                self.status_date = func.now()
                
                # Log the status change
                self._log_status_change(previous_status, TaskStatus.REGISTERING)
```

### Updated Route Implementations

#### 1. Update `/task/{workflow_id}/set_resume_state`

```python
# In jobmon_server/src/jobmon/server/web/routes/v2/fsm/task.py

@api_v2_router.post("/task/{workflow_id}/set_resume_state")
async def set_task_resume_state(workflow_id: int, request: Request) -> Any:
    """An endpoint to set all tasks to a resumable state for a workflow."""
    data = cast(Dict, await request.json())
    reset_if_running = bool(data["reset_if_running"])

    with SessionMaker() as session:
        with session.begin():
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

            excluded_states = [TaskStatus.DONE, TaskStatus.REGISTERING]
            if not reset_if_running:
                excluded_states.append(TaskStatus.RUNNING)

            # Get affected task IDs for audit logging
            affected_tasks = session.execute(
                select(Task.id).where(
                    Task.status.not_in(excluded_states), 
                    Task.workflow_id == workflow_id
                )
            ).scalars().all()

            # Log audit entries before the update
            if affected_tasks:
                TaskStatusAuditService.log_bulk_status_changes(
                    session=session,
                    task_ids=affected_tasks,
                    new_status=TaskStatus.REGISTERING
                )

            # Perform the bulk update
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
    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp
```

#### 2. Update `/array/{array_id}/queue_task_batch`

```python
# In jobmon_server/src/jobmon/server/web/routes/v2/fsm/array.py

@api_v2_router.post("/array/{array_id}/queue_task_batch")
async def record_array_batch_num(array_id: int, request: Request) -> Any:
    """Record a batch number to associate sets of task instances with an array submission."""
    data = cast(Dict, await request.json())
    array_id = int(array_id)
    task_ids = [int(task_id) for task_id in data["task_ids"]]
    task_resources_id = int(data["task_resources_id"])
    workflow_run_id = int(data["workflow_run_id"])
    task_condition = and_(
        Task.id.in_(task_ids),
        Task.status.in_([TaskStatus.REGISTERING, TaskStatus.ADJUSTING_RESOURCES]),
    )

    with SessionMaker() as session:
        with session.begin():
            # Acquire locks on tasks to be updated
            task_locks = (
                select(Task.id)
                .where(task_condition)
                .with_for_update()
                .execution_options(synchronize_session=False)
            )
            affected_task_ids = session.execute(task_locks).scalars().all()

            # Log audit entries before the update
            if affected_task_ids:
                TaskStatusAuditService.log_bulk_status_changes(
                    session=session,
                    task_ids=affected_task_ids,
                    new_status=TaskStatus.QUEUED
                )

            # update task status to acquire lock
            update_stmt = (
                update(Task)
                .where(task_condition)
                .values(
                    status=TaskStatus.QUEUED,
                    status_date=func.now(),
                    num_attempts=(Task.num_attempts + 1),
                )
            )
            session.execute(update_stmt)

            # ... rest of the existing code for task instance creation ...
```

#### 3. Update `/array/{array_id}/transition_to_launched`

```python
# In jobmon_server/src/jobmon/server/web/routes/v2/fsm/array.py

@api_v2_router.post("/array/{array_id}/transition_to_launched")
async def transition_array_to_launched(array_id: int, request: Request) -> Any:
    """Transition TIs associated with an array_id and batch_num to launched."""
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]
    next_report = data["next_report_increment"]

    with SessionMaker() as session:
        with session.begin():
            # Acquire a lock and update tasks to launched
            task_ids_query = (
                select(TaskInstance.task_id)
                .where(
                    TaskInstance.array_id == array_id,
                    TaskInstance.array_batch_num == batch_num,
                )
                .execution_options(synchronize_session=False)
            )

            task_ids = session.execute(task_ids_query).scalars().all()

            task_condition = and_(
                Task.array_id == array_id,
                Task.id.in_(task_ids),
                Task.status == TaskStatus.INSTANTIATING,
            )

            task_locks = (
                select(Task.id)
                .where(task_condition)
                .with_for_update()
                .execution_options(synchronize_session=False)
            )
            affected_task_ids = session.execute(task_locks).scalars().all()

            # Log audit entries before the update
            if affected_task_ids:
                TaskStatusAuditService.log_bulk_status_changes(
                    session=session,
                    task_ids=affected_task_ids,
                    new_status=TaskStatus.LAUNCHED
                )

            update_task_stmt = (
                update(Task)
                .where(task_condition)
                .values(status=TaskStatus.LAUNCHED, status_date=func.now())
            ).execution_options(synchronize_session=False)
            session.execute(update_task_stmt)

    # Update the task instances in a separate session
    _update_task_instance(array_id, batch_num, next_report)

    resp = JSONResponse(content={}, status_code=StatusCodes.OK)
    return resp
```

#### 4. Update `/task_instance/instantiate_task_instances`

```python
# In jobmon_server/src/jobmon/server/web/routes/v2/fsm/task_instance.py

@api_v2_router.post("/task_instance/instantiate_task_instances")
async def instantiate_task_instances(request: Request) -> Any:
    """Sync status of given task intance IDs."""
    data = cast(Dict, await request.json())
    task_instance_ids_list = tuple([int(tid) for tid in data["task_instance_ids"]])

    with SessionMaker() as session:
        with session.begin():
            # update the task table where FSM allows it
            sub_query = (
                select(Task.id)
                .join(TaskInstance, TaskInstance.task_id == Task.id)
                .where(
                    and_(
                        TaskInstance.id.in_(task_instance_ids_list),
                        Task.status == constants.TaskStatus.QUEUED,
                    )
                )
            ).alias("derived_table")
            
            # Get affected task IDs for audit logging
            affected_task_ids = session.execute(
                select(sub_query.c.id)
            ).scalars().all()

            # Log audit entries before the update
            if affected_task_ids:
                TaskStatusAuditService.log_bulk_status_changes(
                    session=session,
                    task_ids=affected_task_ids,
                    new_status=constants.TaskStatus.INSTANTIATING
                )

            task_update = (
                update(Task)
                .where(Task.id.in_(select(sub_query.c.id)))
                .values(
                    status=constants.TaskStatus.INSTANTIATING, status_date=func.now()
                )
                .execution_options(synchronize_session=False)
            )
            session.execute(task_update)

            # ... rest of the existing code ...
```

#### 5. Update `/array/{array_id}/transition_to_killed`

```python
# In jobmon_server/src/jobmon/server/web/routes/v2/fsm/array.py

@api_v2_router.post("/array/{array_id}/transition_to_killed")
async def transition_to_killed(array_id: int, request: Request) -> Any:
    """Transition TIs from KILL_SELF to ERROR_FATAL."""
    structlog.contextvars.bind_contextvars(array_id=array_id)

    data = cast(Dict, await request.json())
    batch_num = data["batch_number"]

    with SessionMaker() as session:
        with session.begin():
            # Find Task IDs belonging to TIs in this array & batch
            task_ids_query = (
                select(TaskInstance.task_id)
                .where(
                    TaskInstance.array_id == array_id,
                    TaskInstance.array_batch_num == batch_num,
                    TaskInstance.status == TaskInstanceStatus.KILL_SELF,
                )
                .execution_options(synchronize_session=False)
            )
            task_ids = session.execute(task_ids_query).scalars().all()

            killable_task_states = (
                TaskStatus.LAUNCHED,
                TaskStatus.RUNNING,
            )
            task_condition = and_(
                Task.array_id == array_id,
                Task.id.in_(task_ids),
                Task.status.in_(killable_task_states),
            )

            # Lock them with_for_update
            task_locks = (
                select(Task.id)
                .where(task_condition)
                .with_for_update()
                .execution_options(synchronize_session=False)
            )
            affected_task_ids = session.execute(task_locks).scalars().all()

            # Log audit entries before the update
            if affected_task_ids:
                TaskStatusAuditService.log_bulk_status_changes(
                    session=session,
                    task_ids=affected_task_ids,
                    new_status=TaskStatus.ERROR_FATAL
                )

            # Transition them to ERROR_FATAL
            update_task_stmt = (
                update(Task)
                .where(task_condition)
                .values(status=TaskStatus.ERROR_FATAL, status_date=func.now())
            ).execution_options(synchronize_session=False)
            session.execute(update_task_stmt)

    # Update task instances
    _update_task_instance_killed(array_id, batch_num)

    return JSONResponse(content={}, status_code=StatusCodes.OK)
```

### Query API Endpoint

```python
# jobmon_server/src/jobmon/server/web/routes/v2/cli/task_status_audit.py
"""Routes for Task Status Audit queries."""

from http import HTTPStatus as StatusCodes
from typing import Any, Optional
import structlog
from fastapi import Query
from sqlalchemy import select
from starlette.responses import JSONResponse

from jobmon.server.web.db import get_sessionmaker
from jobmon.server.web.models.task_status_audit import TaskStatusAudit
from jobmon.server.web.models.task import Task
from jobmon.server.web.routes.v2.cli import cli_router as api_v2_router

logger = structlog.get_logger(__name__)
SessionMaker = get_sessionmaker()


@api_v2_router.get("/task/{task_id}/status_history")
async def get_task_status_history(
    task_id: int,
    limit: Optional[int] = Query(100, description="Maximum number of entries to return"),
) -> Any:
    """Get status change history for a specific task."""
    logger.info(f"Getting status history for task {task_id}")
    
    with SessionMaker() as session:
        query = (
            select(TaskStatusAudit)
            .where(TaskStatusAudit.task_id == task_id)
            .order_by(TaskStatusAudit.changed_at.desc())
        )
        
        if limit:
            query = query.limit(limit)
            
        audit_entries = session.execute(query).scalars().all()
        
        result = [
            {
                "id": entry.id,
                "task_id": entry.task_id,
                "previous_status": entry.previous_status,
                "new_status": entry.new_status,
                "changed_at": entry.changed_at.isoformat(),
            }
            for entry in audit_entries
        ]
        
    return JSONResponse(
        content={"status_history": result, "task_id": task_id},
        status_code=StatusCodes.OK
    )


@api_v2_router.get("/workflow/{workflow_id}/task_status_changes")
async def get_workflow_task_status_changes(
    workflow_id: int,
    limit: Optional[int] = Query(1000, description="Maximum number of entries to return"),
) -> Any:
    """Get recent status changes for all tasks in a workflow."""
    logger.info(f"Getting task status changes for workflow {workflow_id}")
    
    with SessionMaker() as session:
        query = (
            select(TaskStatusAudit)
            .join(TaskStatusAudit.task)
            .where(Task.workflow_id == workflow_id)
            .order_by(TaskStatusAudit.changed_at.desc())
        )
        
        if limit:
            query = query.limit(limit)
            
        audit_entries = session.execute(query).scalars().all()
        
        result = [
            {
                "id": entry.id,
                "task_id": entry.task_id,
                "previous_status": entry.previous_status,
                "new_status": entry.new_status,
                "changed_at": entry.changed_at.isoformat(),
            }
            for entry in audit_entries
        ]
        
    return JSONResponse(
        content={"status_changes": result, "workflow_id": workflow_id},
        status_code=StatusCodes.OK
    )
```

## Summary

This trigger-free design provides:

1. **Complete Coverage**: Captures all task status changes from the 6 identified routes
2. **Application-Level Audit**: Uses a service class to handle audit logging
3. **Bulk Operation Support**: Fetches current statuses before bulk updates to log proper transitions
4. **Simple Schema**: Only tracks task_id, previous_status, new_status, and timestamp
5. **Enhanced Routes**: Each route that performs direct SQL updates now includes audit logging
6. **Query APIs**: Simple endpoints to retrieve status history

The key improvement is the `TaskStatusAuditService.log_bulk_status_changes()` method that fetches current task statuses before bulk updates, ensuring we capture the proper previous_status values without needing database triggers. 
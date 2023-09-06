"""Task Table for the Database."""

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    VARCHAR,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import structlog

from jobmon.core.exceptions import InvalidStateTransition
from jobmon.core.serializers import SerializeDistributorTask, SerializeSwarmTask
from jobmon.server.web.models import Base
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus


logger = structlog.get_logger(__name__)


class Task(Base):
    """Task Database object."""

    __tablename__ = "task"

    def to_wire_as_distributor_task(self) -> tuple:
        """Serialize executor task object."""
        array_id = self.array.id if self.array is not None else None
        serialized = SerializeDistributorTask.to_wire(
            task_id=self.id,
            array_id=array_id,
            name=self.name,
            command=self.command,
            requested_resources=self.task_resources.requested_resources,
        )
        return serialized

    def to_wire_as_swarm_task(self) -> tuple:
        """Serialize swarm task."""
        serialized = SerializeSwarmTask.to_wire(task_id=self.id, status=self.status)
        return serialized

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey("workflow.id"))
    node_id = Column(Integer, ForeignKey("node.id"))
    task_args_hash = Column(VARCHAR(50), index=True)
    array_id = Column(Integer, ForeignKey("array.id"), default=None)
    name = Column(String(255), index=True)
    command = Column(Text)
    task_resources_id = Column(Integer, ForeignKey("task_resources.id"), default=None)
    num_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=1)
    resource_scales = Column(String(1000), default=None)
    fallback_queues = Column(String(1000), default=None)
    status = Column(String(1), ForeignKey("task_status.id"))
    status_date = Column(DateTime, default=func.now(), index=True)

    # ORM relationships
    task_instances = relationship("TaskInstance", back_populates="task")
    task_resources = relationship("TaskResources", foreign_keys=[task_resources_id])
    array = relationship("Array", foreign_keys=[array_id])

    __table_args__ = (
        Index("ix_workflow_id_status_date", "workflow_id", "status_date"),
    )

    # Finite state machine
    valid_transitions = [
        (TaskStatus.REGISTERING, TaskStatus.QUEUED),
        (TaskStatus.ADJUSTING_RESOURCES, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.INSTANTIATING),
        (TaskStatus.INSTANTIATING, TaskStatus.LAUNCHED),
        (TaskStatus.INSTANTIATING, TaskStatus.ERROR_RECOVERABLE),
        (TaskStatus.LAUNCHED, TaskStatus.RUNNING),
        (TaskStatus.LAUNCHED, TaskStatus.ERROR_RECOVERABLE),
        (TaskStatus.INSTANTIATING, TaskStatus.ERROR_RECOVERABLE),
        (TaskStatus.INSTANTIATING, TaskStatus.RUNNING),
        (TaskStatus.RUNNING, TaskStatus.DONE),
        (TaskStatus.RUNNING, TaskStatus.ERROR_RECOVERABLE),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.ADJUSTING_RESOURCES),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.QUEUED),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.ERROR_FATAL),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.REGISTERING),
    ]

    def reset(
        self, name: str, command: str, max_attempts: int, reset_if_running: bool
    ) -> None:
        """Reset status and number of attempts on a Task."""
        # only reset undone tasks
        if self.status != TaskStatus.DONE:
            # only reset if the task is not currently running or if we are
            # resetting running tasks
            if self.status != TaskStatus.RUNNING or reset_if_running:
                self.status = TaskStatus.REGISTERING
                self.num_attempts = 0
                self.name = name
                self.command = command
                self.max_attempts = max_attempts
                self.status_date = func.now()

    def transition(self, new_state: str) -> None:
        """Transition the Task to a new state."""
        # bind_to_logger(workflow_id=self.workflow_id, task_id=self.id)
        logger.info(f"Transitioning task from {self.status} to {new_state}")
        self._validate_transition(new_state)
        if new_state == TaskStatus.QUEUED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state
        self.status_date = func.now()

    def transition_after_task_instance_error(
        self, job_instance_error_state: str
    ) -> None:
        """Transition the task to an error state."""
        # bind_to_logger(workflow_id=self.workflow_id, task_id=self.id)
        logger.info("Transitioning task to ERROR_RECOVERABLE")
        self.transition(TaskStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            logger.info("Giving up task after max attempts.")
            self.transition(TaskStatus.ERROR_FATAL)
        else:
            if job_instance_error_state == TaskInstanceStatus.RESOURCE_ERROR:
                logger.debug("Adjust resource for task.")
                self.transition(TaskStatus.ADJUSTING_RESOURCES)
            else:
                logger.debug("Retrying Task.")
                self.transition(TaskStatus.REGISTERING)

    def _validate_transition(self, new_state: str) -> None:
        """Ensure the task state transition is valid."""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition("Task", self.id, self.status, new_state)

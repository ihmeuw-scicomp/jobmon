"""Task Instance Database Table."""
import json
from typing import Tuple

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import structlog

from jobmon.core.exceptions import InvalidStateTransition
from jobmon.core.serializers import SerializeTaskInstance
from jobmon.server.web.models import Base
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


class TaskInstance(Base):
    """Task Instance Database Table."""

    __tablename__ = "task_instance"

    def to_wire_as_distributor_task_instance(self) -> Tuple:
        """Serialize task instance object."""
        return SerializeTaskInstance.to_wire_distributor(
            self.id,
            self.task_id,
            self.workflow_run_id,
            self.task.workflow_id,
            self.status,
            self.distributor_id,
            self.cluster_id,
            self.task_resources_id,
            self.array_id,
            self.array_batch_num,
            self.array_step_id,
        )

    def to_wire_as_worker_node_task_instance(self) -> Tuple:
        """Serialize task instance object."""
        requested_resources = json.loads(self.task_resources.requested_resources)
        return SerializeTaskInstance.to_wire_worker_node(
            self.id,
            self.status,
            self.workflow_run_id,
            self.task.id,
            self.task.name,
            self.task.command,
            self.task.workflow_id,
            requested_resources.get("stdout"),
            requested_resources.get("stderr"),
        )

    id = Column(Integer, primary_key=True)
    workflow_run_id = Column(Integer, ForeignKey("workflow_run.id"))
    array_id = Column(Integer, ForeignKey("array.id"), default=None)
    task_id = Column(Integer, ForeignKey("task.id"))
    task_resources_id = Column(Integer, ForeignKey("task_resources.id"), index=True)
    array_batch_num = Column(Integer, index=True)
    array_step_id = Column(Integer, index=True)

    distributor_id = Column(String(20), index=True)

    # usage
    nodename = Column(String(150))
    process_group_id = Column(Integer)
    usage_str = Column(String(250))
    wallclock = Column(String(50))
    maxrss = Column(String(50))
    maxpss = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    stdout = Column(String(2048))
    stderr = Column(String(2048))
    stdout_log = Column(Text)
    stderr_log = Column(Text)

    # status/state
    status = Column(
        String(1),
        ForeignKey("task_instance_status.id"),
        default=TaskInstanceStatus.QUEUED,
    )
    submitted_date = Column(DateTime)
    status_date = Column(DateTime, default=func.now())
    report_by_date = Column(DateTime)

    # ORM relationships
    task = relationship("Task", back_populates="task_instances")
    errors = relationship("TaskInstanceErrorLog", back_populates="task_instance")
    task_resources = relationship("TaskResources")

    __table_args__ = (
        Index(
            "ix_array_batch_index",
            "array_id",
            "array_batch_num",
            "array_step_id",
        ),
        Index("ix_status_status_date", "status", "status_date"),
    )

    # finite state machine transition information
    valid_transitions = [
        # task instance is moved from queued to instantiated by distributor
        (TaskInstanceStatus.QUEUED, TaskInstanceStatus.INSTANTIATED),
        # task instance is queued and waiting to instantiate when a new workflow run starts and
        # tells it to die
        (TaskInstanceStatus.QUEUED, TaskInstanceStatus.KILL_SELF),
        # task instance is launched by distributor
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.LAUNCHED),
        # task instance submission hit weird bug and didn't get an distributor_id
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.NO_DISTRIBUTOR_ID),
        # task instance is mid submission and a new workflow run starts and
        # tells it to die
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.KILL_SELF),
        # task instance logs running before submitted due to race condition
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.RUNNING),
        # task instance running after transitioning from launched
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.RUNNING),
        # task instance disappeared from distributor heartbeat and never logged
        # running. The distributor has no accounting of why it died
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.UNKNOWN_ERROR),
        # task instance disappeared from distributor heartbeat and never logged
        # running. The distributor discovered a resource error exit status.
        # This seems unlikely but is valid for the purposes of the FSM
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.RESOURCE_ERROR),
        # task instance is submitted to the batch distributor waiting to launch.
        # new workflow run is created and this task is told to kill
        # itself
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.KILL_SELF),
        # allow task instance to transit to F to immediately fail the task if there is an env
        # mismatch
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.ERROR_FATAL),
        # task instance triaging after transitioning from running
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.TRIAGING),
        # task instance hits an application error (happy path)
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.ERROR),
        # task instance stops logging heartbeats. reconciler can't find an exit
        # status
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.UNKNOWN_ERROR),
        # 1) task instance stops logging heartbeats. reconciler discovers a
        # resource error.
        # 2) worker node detects a resource error
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.RESOURCE_ERROR),
        # task instance is running. another workflow run starts and tells it to
        # die
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.KILL_SELF),
        # task instance finishes normally (happy path)
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.DONE),
        # task instance launched after transitioning from triaging
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.RUNNING),
        # task instance resource_error after transitioning from triaging
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.RESOURCE_ERROR),
        # task instance unknown_error after transitioning from triaging
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.UNKNOWN_ERROR),
        # task instance error_fatal after transitioning from triaging
        (TaskInstanceStatus.TRIAGING, TaskInstanceStatus.ERROR_FATAL),
        # task instance error after transitioning from kill_self
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.ERROR_FATAL),
    ]

    untimely_transitions = [
        # task instance logs running before the distributor logs submitted due to
        # race condition. this is unlikely but happens and is valid for the
        # purposes of the FSM
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.LAUNCHED),
        # A worker node instance can error before the distributor moves it to launched.
        (TaskInstanceStatus.ERROR, TaskInstanceStatus.LAUNCHED),
        # task instance stops logging heartbeats and reconciler is looking for
        # remote exit status but can't find it so logs an unknown error. task
        # finishes with an application error. We can't update state because
        # the task may already be running again due to a race with the JIF
        (TaskInstanceStatus.ERROR, TaskInstanceStatus.UNKNOWN_ERROR),
        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to finish gracefully but reconciler won the race
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.DONE),
        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to report an application error but cant' because
        # the task could be running again alread and we don't want to update
        # task state
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.ERROR),
        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to report a resource error but cant' because
        # the task could be running again alread and we don't want to update
        # task state
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.RESOURCE_ERROR),
        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker reports a resource error before reconciler logs an
        # unknown error.
        (TaskInstanceStatus.RESOURCE_ERROR, TaskInstanceStatus.UNKNOWN_ERROR),
        # task instance stops logging heartbeats and reconciler is looking for
        # remote exit status but can't find it so logs an unknown error.
        # The worker finishes gracefully before reconciler can log an unknown
        # error
        (TaskInstanceStatus.DONE, TaskInstanceStatus.UNKNOWN_ERROR),
        # task is reset by workflow resume and worker finishes gracefully but
        # resume won the race
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.DONE),
        # task is reset by workflow resume and reconciler or worker node
        # discovers resource error, but resume won the race
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.RESOURCE_ERROR),
    ]

    error_states = [
        TaskInstanceStatus.NO_DISTRIBUTOR_ID,
        TaskInstanceStatus.ERROR,
        TaskInstanceStatus.UNKNOWN_ERROR,
        TaskInstanceStatus.RESOURCE_ERROR,
        TaskInstanceStatus.KILL_SELF,
    ]

    def transition(self, new_state: str) -> None:
        """Transition the TaskInstance status."""
        # if the transition is timely, move to new state. Otherwise do nothing
        # bind_to_logger(
        #     workflow_run_id=self.workflow_run_id,
        #     task_id=self.task_id,
        #     task_instance_id=self.id,
        # )
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            logger.info(
                f"Transitioning task_instance from {self.status} to {new_state}"
            )
            self.status = new_state
            self.status_date = func.now()
            if new_state == TaskInstanceStatus.QUEUED:
                self.task.transition(TaskStatus.QUEUED)
            if new_state == TaskInstanceStatus.INSTANTIATED:
                self.task.transition(TaskStatus.INSTANTIATING)
            if new_state == TaskInstanceStatus.LAUNCHED:
                self.task.transition(TaskStatus.LAUNCHED)
            if new_state == TaskInstanceStatus.RUNNING:
                self.task.transition(TaskStatus.RUNNING)
            elif new_state == TaskInstanceStatus.DONE:
                self.task.transition(TaskStatus.DONE)
            elif new_state in self.error_states:
                self.task.transition_after_task_instance_error(new_state)
            elif new_state == TaskInstanceStatus.ERROR_FATAL:
                # if the task instance is F, the task status should be F too
                self.task.transition(TaskStatus.ERROR_RECOVERABLE)
                self.task.transition(TaskStatus.ERROR_FATAL)

    def _validate_transition(self, new_state: str) -> None:
        """Ensure the TaskInstance status transition is valid."""
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition(
                "TaskInstance", self.id, self.status, new_state
            )

    def _is_timely_transition(self, new_state: str) -> bool:
        """Check if the transition is invalid due to a race condition."""
        if (self.status, new_state) in self.__class__.untimely_transitions:
            msg = str(
                InvalidStateTransition("TaskInstance", self.id, self.status, new_state)
            )
            msg += (
                ". This is an untimely transition likely caused by a race "
                " condition between the distributor_service and the worker_node."
            )
            logger.warning(msg)
            return False
        else:
            return True

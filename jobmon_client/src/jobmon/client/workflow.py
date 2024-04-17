"""The overarching framework to create tasks and dependencies within."""

from __future__ import annotations

import copy
import hashlib
import itertools
import logging
import logging.config
import os
from subprocess import PIPE, Popen, TimeoutExpired
import sys
from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Sequence, TYPE_CHECKING, Union
import uuid

import psutil

from jobmon.client.array import Array
from jobmon.client.dag import Dag
from jobmon.client.logging import JobmonLoggerConfig
from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.client.task import Task
from jobmon.client.task_resources import TaskResources
from jobmon.client.tool_version import ToolVersion
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.cluster import Cluster
from jobmon.core.configuration import JobmonConfig
from jobmon.core.constants import (
    MaxConcurrentlyRunning,
    TaskStatus,
    WorkflowRunStatus,
    WorkflowStatus,
)
from jobmon.core.exceptions import (
    ConfigError,
    DistributorStartupTimeout,
    DuplicateNodeArgsError,
    WorkflowAlreadyComplete,
    WorkflowAlreadyExists,
)
from jobmon.core.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.tool import Tool


logger = logging.getLogger(__name__)


class DistributorContext:
    def __init__(self, cluster_name: str, workflow_run_id: int, timeout: int) -> None:
        """Initialization of the DistributorContext."""
        self._cluster_name = cluster_name
        self._workflow_run_id = workflow_run_id
        self._timeout = timeout

    def __enter__(self) -> DistributorContext:
        """Starts the Distributor Process."""
        logger.info("Starting Distributor Process")

        # construct env
        env = os.environ.copy()
        entry_point = self.derive_jobmon_command_from_env()
        if entry_point is not None:
            env["JOBMON__DISTRIBUTOR__WORKER_NODE_ENTRY_POINT"] = f'"{entry_point}"'

        # Start the distributor. Write stderr to a file.
        cmd = [
            sys.executable,
            "-m",  # safest way to find the entrypoint
            "jobmon.distributor.cli",
            "start",
            "--cluster_name",
            self._cluster_name,
            "--workflow_run_id",
            str(self._workflow_run_id),
        ]
        self.process = Popen(
            cmd,
            stderr=PIPE,
            universal_newlines=True,
            env=env,
        )

        # check if stderr contains "ALIVE"
        assert self.process.stderr is not None  # keep mypy happy on optional type
        stderr_val = self.process.stderr.read(5)
        if stderr_val != "ALIVE":
            err = self._shutdown()
            raise DistributorStartupTimeout(
                f"Distributor process did not start, stderr='{err}'"
            )
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        """Stops the Distributor Process."""
        logger.info("Stopping Distributor Process")
        err = self._shutdown()
        logger.info(f"Got {err} from Distributor Process")

    def alive(self) -> bool:
        self.process.poll()
        return self.process.returncode is None

    def _shutdown(self) -> str:
        self.process.terminate()
        try:
            _, err = self.process.communicate(timeout=self._timeout)
        except TimeoutExpired:
            err = ""

        if "SHUTDOWN" not in err:
            try:
                parent = psutil.Process(self.process.pid)
                for child in parent.children(recursive=True):
                    child.kill()
            except psutil.NoSuchProcess:
                pass
            self.process.kill()
            self.process.wait()

        return err

    @staticmethod
    def derive_jobmon_command_from_env() -> Optional[str]:
        """If a singularity path is provided, use it when running the worker node."""
        singularity_img_path = os.environ.get("IMGPATH", None)
        if singularity_img_path:
            return f"singularity run --app jobmon_command {singularity_img_path}"
        return None


class Workflow(object):
    """(aka Batch, aka Swarm).

    A Workflow is a framework by which a user may define the relationship
    between tasks and define the relationship between multiple runs of the same
    set of tasks. The great benefit of the Workflow is that it's resumable.
    A Workflow can only be re-loaded if two things are shown to be exact
    matches to a previous Workflow:

    1. WorkflowArgs: It is recommended to pass a meaningful unique identifier
        to workflow_args, to ease resuming. However, if the Workflow is a
        one-off project, you may instantiate the Workflow anonymously, without
        WorkflowArgs. Under the hood, the WorkflowArgs will default to a UUID
        which, as it is randomly generated, will be harder to remember and thus
        harder to resume.

        Workflow args must be hashable. For example, CodCorrect or Como version
        might be passed as Args to the Workflow. For now, the assumption is
        WorkflowArgs is a string.

    2. The tasks added to the workflow. A Workflow is built up by
        using Workflow.add_task(). In order to resume a Workflow, all the same
        tasks must be added with the same dependencies between tasks.
    """

    def __init__(
        self,
        tool_version: ToolVersion,
        workflow_args: str = "",
        name: str = "",
        description: str = "",
        workflow_attributes: Optional[Union[List, dict]] = None,
        max_concurrently_running: int = MaxConcurrentlyRunning.MAXCONCURRENTLYRUNNING,
        requester: Optional[Requester] = None,
        chunk_size: int = 500,  # TODO: should be in the config
    ) -> None:
        """Initialization of the client workflow.

        Args:
            tool_version: ToolVersion this workflow is associated
            workflow_args: Unique identifier of a workflow
            name: Name of the workflow
            description: Description of the workflow
            workflow_attributes: Attributes that make this workflow different from other
                workflows that the user wants to record.
            max_concurrently_running: How many running jobs to allow in parallel
            requester: object to communicate with the flask services.
            chunk_size: how many tasks to bind in a single request
            default_max_attempts: the default max attempts of the workflow for each array
        """
        self._tool_version = tool_version
        self.name = name
        self.description = description
        self.max_concurrently_running: int = max_concurrently_running

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

        self._dag = Dag(requester)
        # hash to task object mapping. ensure only 1
        self.tasks: Dict[int, Task] = {}
        self.arrays: Dict[str, Array] = {}
        self._chunk_size: int = chunk_size

        if workflow_args:
            self.workflow_args = workflow_args
        else:
            self.workflow_args = str(uuid.uuid4())
            logger.info(
                "Workflow_args defaulting to uuid {}. To resume this "
                "workflow, you must re-instantiate Workflow and pass "
                "this uuid in as the workflow_args. As a uuid is hard "
                "to remember, we recommend you name your workflows and"
                " make workflow_args a meaningful unique identifier. "
                "Then add the same tasks to this workflow".format(self.workflow_args)
            )
        self.workflow_args_hash = int(
            hashlib.sha256(self.workflow_args.encode("utf-8")).hexdigest(), 16
        )

        self.workflow_attributes: Dict[str, Any] = {}
        if workflow_attributes:
            if isinstance(workflow_attributes, List):
                for attr in workflow_attributes:
                    self.workflow_attributes[attr] = None
            elif isinstance(workflow_attributes, dict):
                for attr, val in workflow_attributes.items():
                    self.workflow_attributes[str(attr)] = str(val)
            else:
                raise ValueError(
                    "workflow_attributes must be provided as a list of attributes or a "
                    "dictionary of attributes and their values"
                )

        # Cache for clusters and task resources
        self._clusters: Dict[str, Cluster] = {}
        self._task_resources: Dict[int, TaskResources] = {}
        self.default_cluster_name: str = ""
        self._default_max_attempts: Optional[int] = None
        self.default_compute_resources_set: Dict[str, Dict[str, Any]] = {}
        self.default_resource_scales_set: Dict[str, Dict[str, float]] = {}

        self._fail_after_n_executions = 1_000_000_000
        self.last_workflow_run_id: Optional[int] = None

    @property
    def tool(self) -> Tool:
        """Returns the associated tool to this workflow."""
        return self._tool_version.tool

    @property
    def is_bound(self) -> bool:
        """If the workflow has been bound to the db."""
        if not hasattr(self, "_workflow_id"):
            return False
        else:
            return True

    @property
    def workflow_id(self) -> int:
        """If the workflow is bound then it will have been given an id."""
        if not self.is_bound:
            raise AttributeError(
                "workflow_id cannot be accessed before workflow is bound"
            )
        return self._workflow_id

    @property
    def dag_id(self) -> int:
        """If it has been bound, it will have an associated dag_id."""
        if not self.is_bound:
            raise AttributeError("dag_id cannot be accessed before workflow is bound")
        return self._dag.dag_id

    @property
    def task_hash(self) -> int:
        """Hash of all of the tasks."""
        hash_value = hashlib.sha256()
        tasks = sorted(self.tasks.values())
        if len(tasks) > 0:  # if there are no tasks, we want to skip this
            for task in tasks:
                hash_value.update(str(hash(task)).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    @property
    def task_errors(self) -> Dict:
        """Return a dict of error associated with a task."""
        return {
            task.name: task.get_errors()
            for task in self.tasks.values()
            if task.final_status == TaskStatus.ERROR_FATAL
        }

    @property
    def default_max_attempts(self) -> Optional[int]:
        """Return the workflow default max attempts."""
        if self._default_max_attempts is None:
            self._default_max_attempts = self.tool.default_max_attempts
        return self._default_max_attempts

    def add_attributes(self, workflow_attributes: dict) -> None:
        """Users can call either to update values of existing attributes or add new attributes.

        Args:
            workflow_attributes: attributes to be bound to the db that describe
                this workflow.
        """
        app_route = f"/workflow/{self.workflow_id}/workflow_attributes"
        self.requester.send_request(
            app_route=app_route,
            message={"workflow_attributes": workflow_attributes},
            request_type="put",
        )

    def add_task(self, task: Task) -> Task:
        """Add a task to the workflow to be executed.

        Set semantics - add tasks once only, based on hash name.

        Args:
            task: single task to add.
        """
        logger.debug(f"Adding Task {task}")
        if hash(task) in self.tasks.keys():
            raise ValueError(
                f"A task with hash {hash(task)} already exists. "
                f"All tasks in a workflow must have unique "
                f"commands. Your command was: {task.command}"
            )

        try:
            # link array
            self._link_array_and_workflow(task.array)
        except AttributeError:
            # or infer if not already created
            template_name = task.node.task_template_version.task_template.template_name
            try:
                array = self.arrays[template_name]
            except KeyError:
                # create array from the task template version on the node
                array = Array(
                    task_template_version=task.node.task_template_version,
                    task_args=task.task_args,
                    op_args=task.op_args,
                    cluster_name=task.cluster_name,
                    requester=self.requester,
                )
                self._link_array_and_workflow(array)

            # add task to inferred array
            array.add_task(task)
        except ValueError:
            # check if current task array is the same as the one attached to the workflow
            template_name = task.node.task_template_version.task_template.template_name
            if self.arrays[template_name] != task.array:
                raise
        # set array max_attempts
        # task.array.max_attempts = self._default_max_attempts
        # add node to task
        try:
            self._dag.add_node(task.node)
        except DuplicateNodeArgsError:
            raise DuplicateNodeArgsError(
                "All tasks for a given task template in a workflow must have unique node_args."
                f"Found duplicate node args for {task}. task_template_version_id="
                f"{task.node.task_template_version_id}, node_args={task.node.node_args}"
            )

        # add task to workflow
        self.tasks[hash(task)] = task
        task.workflow = self

        logger.debug(f"Task {hash(task)} added")

        return task

    def _link_array_and_workflow(self, array: Array) -> None:
        template_name = array.task_template_version.task_template.template_name
        if template_name in self.arrays.keys():
            raise ValueError(
                f"An array for template_name={template_name} already exists on this workflow."
                f" You can only call TaskTemplate.create_tasks once per task template."
            )
        # add the references
        self.arrays[template_name] = array
        array.workflow = self

    def add_tasks(self, tasks: Sequence[Task]) -> None:
        """Add a list of task to the workflow to be executed."""
        for task in tasks:
            # add the task
            self.add_task(task)

    def set_default_compute_resources_from_yaml(
        self, cluster_name: str, yaml_file: str
    ) -> None:
        """Set default compute resources from a user provided yaml file for workflow level.

        TODO: Implement this method.

        Args:
            cluster_name: name of cluster to set default values for.
            yaml_file: the yaml file that is providing the compute resource values.
        """
        pass

    def set_default_compute_resources_from_dict(
        self, cluster_name: str, dictionary: Dict[str, Any]
    ) -> None:
        """Set default compute resources for a given cluster_name.

        Args:
            cluster_name: name of cluster to set default values for.
            dictionary: dictionary of default compute resources to run tasks
                with. Can be overridden at task template, tool or task level.
        """
        # TODO: Do we need to handle the scenario where no cluster name is specified?
        self.default_compute_resources_set[cluster_name] = dictionary

    def set_default_resource_scales_from_dict(
        self, cluster_name: str, dictionary: Dict[str, float]
    ) -> None:
        """Set default resource scales for a given cluster_name.

        Args:
            cluster_name: name of cluster to set default values for.
            dictionary: dictionary of default resource scales to adjust task
                resources with. Can be overridden at task template or task level.
        """
        # TODO: Do we need to handle the scenario where no cluster name is specified?
        self.default_resource_scales_set[cluster_name] = dictionary

    def set_default_cluster_name(self, cluster_name: str) -> None:
        """Set the default cluster.

        Args:
            cluster_name: name of cluster to set as default.
        """
        self.default_cluster_name = cluster_name

    def set_default_max_attempts(self, value: int) -> None:
        """Set the max attempts.

        Args:
            value: value of max_attempts.
        """
        self._default_max_attempts = value

    def get_tasks_by_node_args(
        self, task_template_name: str, **kwargs: Any
    ) -> List[Task]:
        """Query tasks by node args. Used for setting dependencies."""
        try:
            array = self.arrays[task_template_name]
        except KeyError:
            raise ValueError(
                f"task_template_name={task_template_name} not found on workflow. Known "
                f"template_names are {self.arrays.keys()}."
            )
        tasks = array.get_tasks_by_node_args(**kwargs)
        return tasks

    def set_max_concurrently_running(
        self, task_template_name: str, max_concurrently_running: int
    ) -> None:
        pass

    def run(
        self,
        fail_fast: bool = False,
        seconds_until_timeout: int = 36000,
        resume: bool = False,
        reset_running_jobs: bool = True,
        distributor_startup_timeout: int = 180,
        resume_timeout: int = 300,
        configure_logging: bool = False,
    ) -> Optional[str]:
        """Run the workflow.

        Traverse the dag and submitting new tasks when their tasks have completed successfully.

        Args:
            fail_fast: whether to break out of distributor on first failure.
            seconds_until_timeout: amount of time (in seconds) to wait
                until the whole workflow times out. Submitted jobs will
                continue
            resume: whether the workflow should be resumed or not, if
                it is not set to resume and an identical workflow already
                exists, the workflow will error out
            reset_running_jobs: whether or not to reset running jobs upon resume
            distributor_startup_timeout: amount of time to wait for the distributor process to
                start up
            resume_timeout: seconds to wait for a workflow to become resumable before giving up
            configure_logging: setup jobmon logging. If False, no logging will be configured.
                If True, default logging will be configured.

        Returns:
            str of WorkflowRunStatus
        """
        if configure_logging is True:
            JobmonLoggerConfig.attach_default_handler(
                logger_name="jobmon.client", log_level=logging.INFO
            )

        # bind to database
        logger.info("Adding Workflow metadata to database")
        self.bind()

        config = JobmonConfig()
        try:
            gui_url = config.get("http", "gui_url")
        except ConfigError:
            gui_url = ""

        logger.info(
            f"Workflow ID {self.workflow_id} assigned. Progress can be monitored at "
            f"{gui_url}/#/workflow/{self.workflow_id}/tasks"
        )

        # Check if this workflow is already complete and is runnable
        if self._status == WorkflowStatus.DONE:
            raise WorkflowAlreadyComplete(
                f"Workflow ({self.workflow_id}) is already in done state and cannot be resumed"
            )

        if not self._newly_created and not resume:
            raise WorkflowAlreadyExists(
                "This workflow already exists. If you are trying to resume a workflow, "
                "please set the resume flag. If you are not trying to resume a workflow, make "
                "sure the workflow args are unique or the tasks are unique"
            )
        if self._newly_created and resume:
            logger.warning(
                "The resume flag has been set but no previous workflow_args exist."
                "Note that the workflow will execute as a new workflow."
            )

        # Bind tasks
        logger.info("Adding task metadata to database")
        # Need to wait for resume signal to be sent before resetting tasks, in case of a resume
        factory = WorkflowRunFactory(self.workflow_id)
        if not self._newly_created and resume:
            factory.set_workflow_resume(
                reset_running_jobs=reset_running_jobs, resume_timeout=resume_timeout
            )

        self._bind_tasks(
            reset_if_running=reset_running_jobs, chunk_size=self._chunk_size
        )

        # create workflow_run
        logger.info("Adding WorkflowRun metadata to database")
        wfr = factory.create_workflow_run()
        # Update the workflowrun to BOUND state immediately in this API. All metadata already
        # bound, so the swarm can start immediately.
        wfr._update_status(WorkflowRunStatus.BOUND)
        logger.info(f"WorkflowRun ID {wfr.workflow_run_id} assigned")

        # start distributor
        cluster_name = list(self._clusters.keys())[0]
        with DistributorContext(
            cluster_name, wfr.workflow_run_id, distributor_startup_timeout
        ) as distributor:
            # set up swarm and initial DAG
            swarm = SwarmWorkflowRun(
                workflow_run_id=wfr.workflow_run_id,
                fail_after_n_executions=self._fail_after_n_executions,
                requester=self.requester,
                fail_fast=fail_fast,
                status=wfr.status,
            )
            swarm.from_workflow(self)
            self._num_previously_completed = swarm.num_previously_complete

            try:
                swarm.run(distributor.alive, seconds_until_timeout)
            finally:
                # figure out doneness
                num_new_completed = (
                    len(swarm.done_tasks) - swarm.num_previously_complete
                )
                if swarm.status != WorkflowRunStatus.DONE:
                    logger.info(
                        f"WorkflowRun execution ended, num failed {len(swarm.failed_tasks)}"
                    )
                else:
                    logger.info(
                        f"WorkflowRun execute finished successfully, {num_new_completed} tasks"
                    )

                # update workflow tasks with final status
                for task in self.tasks.values():
                    task.final_status = swarm.tasks[task.task_id].status
                self._num_newly_completed = num_new_completed

        self.last_workflow_run_id = wfr.workflow_run_id

        return swarm.status

    def set_task_template_max_concurrency_limit(
        self, task_template_name: str, limit: int
    ) -> None:
        try:
            array = self.arrays[task_template_name]
        except Exception:
            raise KeyError(
                f"There is no task_template named '{task_template_name}' "
                f"associated with this workflow. Workflow name: {self.name}"
            )
        array.max_concurrently_running = limit

    def validate(self, strict: bool = True, raise_on_error: bool = False) -> None:
        """Confirm that the tasks in this workflow are valid.

        This method will:
        - access the database to confirm the requested resources are valid for
        the specified cluster
        - confirm that the workflow args are valid
        - make sure no task contains up/down stream tasks that are not in the workflow
        """
        # construct task resources
        for task in self.tasks.values():
            # get the cluster for this task
            cluster = self.get_cluster_by_name(task.cluster_name)

            # not dynamic resource request. Construct TaskResources
            if task.compute_resources_callable is None:
                try:
                    queue = cluster.get_queue(task.queue_name)
                except ValueError as e:
                    if raise_on_error:
                        raise e
                    else:
                        logger.info(e)
                        continue

                # validate the constructed resources
                task_resources = TaskResources(
                    requested_resources=task.requested_resources, queue=queue
                )

                is_valid, msg = task_resources.validate_resources(strict)
                if not is_valid:
                    if raise_on_error:
                        raise ValueError(f"Failed validation, reasons: {msg}")
                    else:
                        logger.info(f"Failed validation, reasons: {msg}")

        for array in self.arrays.values():
            try:
                array.validate()
            except ValueError as e:
                if raise_on_error:
                    raise
                else:
                    logger.info(e)
        try:
            cluster_names = list(self._clusters.keys())
            if len(list(self._clusters.keys())) > 1:
                raise RuntimeError(
                    f"Workflow can only use one cluster. Found cluster_names={cluster_names}"
                )
            # check if workflow is valid
            self._dag.validate()
            self._matching_wf_args_diff_hash()
        except Exception as e:
            if raise_on_error:
                raise
            else:
                logger.info(e)

    def bind(self) -> None:
        """Get a workflow_id."""
        if self.is_bound:
            return

        # strict = False means we can coerce. obviously we need to raise at this point
        self.validate(strict=False, raise_on_error=True)

        # bind dag
        self._dag.bind(self._chunk_size)

        # bind workflow
        app_route = "/workflow"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "tool_version_id": self._tool_version.id,
                "dag_id": self._dag.dag_id,
                "workflow_args_hash": self.workflow_args_hash,
                "task_hash": self.task_hash,
                "description": self.description,
                "name": self.name,
                "workflow_args": self.workflow_args,
                "max_concurrently_running": self.max_concurrently_running,
                "workflow_attributes": self.workflow_attributes,
            },
            request_type="post",
        )

        self._workflow_id = response["workflow_id"]
        self._status = response["status"]
        self._newly_created = response["newly_created"]

    def _bind_tasks(
        self,
        reset_if_running: bool = True,
        chunk_size: int = 500,
    ) -> None:
        app_route = "/task/bind_tasks_no_args"
        remaining_task_hashes = list(self.tasks.keys())

        while remaining_task_hashes:
            # split off first chunk elements from queue.
            task_hashes_chunk = remaining_task_hashes[:chunk_size]
            remaining_task_hashes = remaining_task_hashes[chunk_size:]

            # If this is the last chunk, mark the created_date field in the
            # database.
            mark_created = len(remaining_task_hashes) == 0

            # send to server in a format of:
            # {<hash>:[workflow_id(0), node_id(1), task_args_hash(2), array_id(3),
            # name(4), command(5), max_attempts(6)], reset_if_running(7), task_args(8),
            # task_attributes(9), resource_scales(10), fallback_queues(11)}
            # flat the data structure so that the server won't depend on the client
            task_metadata: Dict[int, List] = {}
            for task_hash in task_hashes_chunk:
                task = self.tasks[task_hash]

                # get array id
                array = task.array
                if not array.is_bound:
                    array.bind()

                # get task resources id
                self._set_original_task_resources(task)

                serializable_resource_scales = copy.copy(task.resource_scales)
                for resource, scaler in task.resource_scales.items():
                    # We can't serialize a callable, so use the function name instead.
                    if callable(scaler):
                        serializable_resource_scales[resource] = getattr(  # type: ignore
                            scaler, "__name__", "Unknown Callable"  # type: ignore
                        )
                    # We can't serialize an iterator, so take the relevant elements as a
                    # list.
                    elif isinstance(scaler, Iterator):
                        serializable_resource_scales[resource] = list(  # type: ignore
                            itertools.islice(
                                copy.deepcopy(scaler), task.max_attempts - 1
                            )
                        )

                task_metadata[task_hash] = [
                    task.node.node_id,
                    str(task.task_args_hash),
                    task.array.array_id,
                    task.original_task_resources.id,
                    task.name,
                    task.command,
                    task.max_attempts,
                    reset_if_running,
                    serializable_resource_scales,
                    task.fallback_queues,
                ]

            parameters = {
                "workflow_id": self.workflow_id,
                "tasks": task_metadata,
                "mark_created": mark_created,
            }
            return_code, response = self.requester.send_request(
                app_route=app_route,
                message=parameters,
                request_type="put",
            )

            # populate returned values onto task dict
            return_tasks = response["tasks"]
            for k in return_tasks.keys():
                task = self.tasks[int(k)]
                task.task_id = return_tasks[k][0]
                task.initial_status = return_tasks[k][1]

        # Bind task arguments and attributes as well
        self._bind_task_args(chunk_size)
        self._bind_task_attributes(chunk_size)

    def _bind_task_args(self, chunk_size: int = 500) -> None:
        """Bind all task args to the database.

        Loop through our bound task dict in chunks in order to bind new args and arg types
        to the database.
        """
        remaining_task_hashes = list(self.tasks.keys())

        while remaining_task_hashes:
            # split off first chunk elements from queue.
            task_hashes_chunk = remaining_task_hashes[:chunk_size]
            remaining_task_hashes = remaining_task_hashes[chunk_size:]

            task_arg_list = []
            for task_hash in task_hashes_chunk:
                task = self.tasks[task_hash]
                task_args = [
                    (task.task_id, arg_id, value)
                    for arg_id, value in task.mapped_task_args.items()
                ]
                task_arg_list.extend(task_args)

            self.requester.send_request(
                app_route="/task/bind_task_args",
                message={"task_args": task_arg_list},
                request_type="put",
            )

    def _bind_task_attributes(self, chunk_size: int = 500) -> None:
        remaining_task_hashes = list(self.tasks.keys())

        while remaining_task_hashes:
            # split off first chunk elements from queue.
            task_hashes_chunk = remaining_task_hashes[:chunk_size]
            remaining_task_hashes = remaining_task_hashes[chunk_size:]

            attribute_dict = {}
            for task_hash in task_hashes_chunk:
                task = self.tasks[task_hash]
                attribute_dict[task.task_id] = task.task_attributes

            # Send the request
            self.requester.send_request(
                app_route="/task/bind_task_attributes",
                message={"task_attributes": attribute_dict},
                request_type="put",
            )

    def get_errors(
        self, limit: int = 1000
    ) -> Optional[Dict[int, Dict[str, Union[int, List[Dict[str, Union[str, int]]]]]]]:
        """Method to get all errors.

        Return a dictionary with the erring task_id as the key, and
        the Task.get_errors content as the value.
        When limit is specifically set as None from the client, this
        return set will pass back all the erred tasks in the workflow.
        """
        errors = {}

        cnt: int = 0
        for task in self.tasks.values():
            task_id = task.task_id
            task_errors = task.get_errors()
            if task_errors is not None and len(task_errors) > 0:
                errors[task_id] = task_errors
                cnt += 1
                if limit is not None and cnt >= limit - 1:
                    break

        return errors

    def get_cluster_by_name(self, cluster_name: str) -> Cluster:
        """Check if the cluster that the task specified is in the cache.

        If the cluster is not in the cache, create it and add to cache.
        """
        try:
            cluster = self._clusters[cluster_name]
        except KeyError:
            cluster = Cluster(cluster_name=cluster_name, requester=self.requester)
            cluster.bind()
            self._clusters[cluster_name] = cluster
        return cluster

    def _set_original_task_resources(self, task: Task) -> None:
        cluster = self.get_cluster_by_name(task.cluster_name)
        queue = cluster.get_queue(task.queue_name)
        task_resources = TaskResources(
            requested_resources=task.requested_resources, queue=queue
        )

        try:
            task_resources = self._task_resources[hash(task_resources)]
        except KeyError:
            task_resources.bind()
            self._task_resources[hash(task_resources)] = task_resources

        task.original_task_resources = task_resources

    def _matching_wf_args_diff_hash(self) -> None:
        """Check that that an existing workflow does not contain different tasks.

        Check that an existing workflow with the same workflow_args does not have a
        different hash, this would indicate that thgat the workflow contains different tasks.
        """
        rc, response = self.requester.send_request(
            app_route=f"/workflow/{str(self.workflow_args_hash)}",
            message={},
            request_type="get",
        )
        bound_workflow_hashes = response["matching_workflows"]
        for task_hash, tool_version_id, dag_hash in bound_workflow_hashes:
            match = self._tool_version.id == tool_version_id and (
                str(self.task_hash) != task_hash or str(hash(self._dag)) != dag_hash
            )
            if match:
                raise WorkflowAlreadyExists(
                    "The unique workflow_args already belong to a workflow "
                    "that contains different tasks than the workflow you are "
                    "creating, either change your workflow args so that they "
                    "are unique for this set of tasks, or make sure your tasks"
                    " match the workflow you are trying to resume"
                )

    def __hash__(self) -> int:
        """Hash to encompass tool version id, workflow args, tasks and dag."""
        hash_value = hashlib.sha256()
        hash_value.update(str(hash(self._tool_version.id)).encode("utf-8"))
        hash_value.update(str(self.workflow_args_hash).encode("utf-8"))
        hash_value.update(str(self.task_hash).encode("utf-8"))
        hash_value.update(str(hash(self._dag)).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    def __repr__(self) -> str:
        """A representation string for a Workflow instance."""
        repr_string = (
            f"Workflow(workflow_args={self.workflow_args}, " f"name={self.name}"
        )
        try:
            repr_string += f", workflow_id={self.workflow_id})"
        except AttributeError:
            # Workflow not yet bound so no ID to add to repr
            repr_string += ")"
        return repr_string

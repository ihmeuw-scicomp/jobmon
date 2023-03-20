"""Serializing data when going to and from the database."""
import ast
from datetime import datetime
import json
from typing import Any, Dict, List, Optional, Tuple, Union


class SerializeDistributorTask:
    """Serialize the data to and from the database for an DistributorTask object."""

    @staticmethod
    def to_wire(
        task_id: int,
        array_id: int,
        name: str,
        command: str,
        requested_resources: dict,
    ) -> tuple:
        """Submitting the above args to the database for an DistributorTask object."""
        return (task_id, array_id, name, command, requested_resources)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Coerce types for all nullables that are cast using ast.literal_eval.

        It is a potential security issue but was the only solution I could find to turning
        the data into json twice.
        """
        return {
            "task_id": int(wire_tuple[0]),
            "array_id": int(wire_tuple[1]) if wire_tuple[1] is not None else None,
            "name": wire_tuple[2],
            "command": wire_tuple[3],
            "requested_resources": {}
            if wire_tuple[4] is None
            else ast.literal_eval(wire_tuple[4]),
        }


class SerializeSwarmTask:
    """Serialize the data to and from the db for a Swarm Task."""

    @staticmethod
    def to_wire(task_id: int, status: str) -> tuple:
        """Submit task id and status to the database from a SwarmTask object."""
        return task_id, status

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get task id and status for a SwarmTask."""
        return {"task_id": int(wire_tuple[0]), "status": wire_tuple[1]}


class SerializeTaskInstance:
    """Serialize the data to and from the database for an DistributorTaskInstance."""

    @staticmethod
    def to_wire_distributor(
        task_instance_id: int,
        task_id: int,
        workflow_run_id: int,
        workflow_id: int,
        status: str,
        distributor_id: Union[int, None],
        cluster_id: Optional[int] = None,
        task_resources_id: Optional[int] = None,
        array_id: Optional[int] = None,
        array_batch_num: Optional[int] = None,
        array_step_id: Optional[int] = None,
    ) -> tuple:
        """Submit the above args for an DistributorTaskInstance object to the database."""
        return (
            task_instance_id,
            task_id,
            workflow_run_id,
            workflow_id,
            status,
            distributor_id,
            cluster_id,
            task_resources_id,
            array_id,
            array_batch_num,
            array_step_id,
        )

    @staticmethod
    def kwargs_from_wire_distributor(wire_tuple: tuple) -> dict:
        """Retrieve the DistributorTaskInstance information from the database."""
        task_instance_id = int(wire_tuple[0])
        task_id = int(wire_tuple[1])
        workflow_run_id = int(wire_tuple[2])
        workflow_id = int(wire_tuple[3])
        status = str(wire_tuple[4])
        distributor_id = int(wire_tuple[5]) if wire_tuple[5] else None
        cluster_id = int(wire_tuple[6]) if wire_tuple[6] else None
        task_resources_id = int(wire_tuple[7]) if wire_tuple[7] else None
        array_id = int(wire_tuple[8]) if wire_tuple[8] else None
        array_batch_num = int(wire_tuple[9]) if wire_tuple[9] else None
        array_step_id = int(wire_tuple[10]) if wire_tuple[10] else None

        return {
            "task_instance_id": task_instance_id,
            "task_id": task_id,
            "workflow_run_id": workflow_run_id,
            "workflow_id": workflow_id,
            "status": status,
            "distributor_id": distributor_id,
            "cluster_id": cluster_id,
            "task_resources_id": task_resources_id,
            "array_id": array_id,
            "array_batch_num": array_batch_num,
            "array_step_id": array_step_id,
        }

    @staticmethod
    def to_wire_worker_node(
        task_instance_id: int,
        status: str,
        workflow_run_id: int,
        task_id: int,
        name: str,
        command: str,
        workflow_id: int,
        stdout_dir: str,
        stderr_dir: str,
    ) -> tuple:
        """Submit the above args for an DistributorTaskInstance object to the database."""
        return (
            task_instance_id,
            status,
            workflow_run_id,
            task_id,
            name,
            command,
            workflow_id,
            stdout_dir,
            stderr_dir,
        )

    @staticmethod
    def kwargs_from_wire_worker_node(wire_tuple: tuple) -> dict:
        """Retrieve the DistributorTaskInstance information from the database."""
        task_instance_id = int(wire_tuple[0])
        status = wire_tuple[1]
        workflow_run_id = int(wire_tuple[2])
        task_id = int(wire_tuple[3])
        name = wire_tuple[4]
        command = wire_tuple[5]
        workflow_id = int(wire_tuple[6])
        stdout_dir = wire_tuple[7]
        stderr_dir = wire_tuple[8]

        return {
            "task_instance_id": task_instance_id,
            "status": status,
            "workflow_run_id": workflow_run_id,
            "task_id": task_id,
            "name": name,
            "command": command,
            "workflow_id": workflow_id,
            "stdout_dir": stdout_dir,
            "stderr_dir": stderr_dir,
        }


class SerializeTaskInstanceErrorLog:
    """Serialize the data to and from the database for an TaskInstanceErrorLog."""

    @staticmethod
    def to_wire(
        task_instance_error_log_id: int, error_time: datetime, description: str
    ) -> tuple:
        """Submit the args for an SerializeTaskInstanceErrorLog object to the database."""
        return task_instance_error_log_id, error_time, description

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the SerializeTaskInstanceErrorLog information from the database."""
        return {
            "task_instance_error_log_id": int(wire_tuple[0]),
            "error_time": str(wire_tuple[1]),
            "description": str(wire_tuple[2]),
        }


class SerializeExecutorTaskInstanceErrorLog:
    """Serialize the data to and from the database for an ExecutorTaskInstanceErrorLog."""

    @staticmethod
    def to_wire(
        task_instance_error_log_id: int, error_time: datetime, description: str
    ) -> tuple:
        """A to_wire method.

        Submit the above args for an SerializeExecutorTaskInstanceErrorLog
        object to the database.
        """
        return task_instance_error_log_id, error_time, description

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the SerializeExecutorTaskInstanceErrorLog information from the database."""
        return {
            "task_instance_error_log_id": int(wire_tuple[0]),
            "error_time": str(wire_tuple[1]),
            "description": str(wire_tuple[2]),
        }


class SerializeClientTool:
    """Serialize the data to and from the database for a Tool object."""

    @staticmethod
    def to_wire(id: int, name: str) -> tuple:
        """Submit the id and name of a Tool to the database."""
        return (id, name)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get a Tool's information from the database."""
        return {"id": int(wire_tuple[0]), "name": wire_tuple[1]}


class SerializeClientToolVersion:
    """Serialize the data to and from the database for a ToolVersion."""

    @staticmethod
    def to_wire(id: int, tool_id: int) -> tuple:
        """Submit the id and tool_id for a Tool Version to the database."""
        return (id, tool_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Tool Version information from thee database."""
        return {"id": int(wire_tuple[0]), "tool_id": int(wire_tuple[1])}


class SerializeClientTaskTemplate:
    """Serialize the data to and from the database for a TaskTemplate."""

    @staticmethod
    def to_wire(id: int, tool_version_id: int, template_name: str) -> tuple:
        """Submit the TaskTemplate information for transfer over http."""
        return (id, tool_version_id, template_name)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Convert packed wire format to kwargs for use in services."""
        return {
            "id": int(wire_tuple[0]),
            "tool_version_id": int(wire_tuple[1]),
            "template_name": wire_tuple[2],
        }


class SerializeClientTaskTemplateVersion:
    """Serialize the data to and from the database for a TaskTemplateVersion."""

    @staticmethod
    def to_wire(
        task_template_version_id: int,
        command_template: str,
        node_args: List[str],
        task_args: List[str],
        op_args: List[str],
        id_name_map: dict,
        task_template_id: int,
    ) -> Tuple[int, str, List[str], List[str], List[str], dict, int]:
        """Submit the TaskTemplateVersion information to the database."""
        return (
            task_template_version_id,
            command_template,
            node_args,
            task_args,
            op_args,
            id_name_map,
            task_template_id,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> Dict:
        """Get the TaskTemplateVersion info from the database."""
        return {
            "task_template_version_id": int(wire_tuple[0]),
            "command_template": wire_tuple[1],
            "node_args": wire_tuple[2],
            "task_args": wire_tuple[3],
            "op_args": wire_tuple[4],
            "id_name_map": wire_tuple[5],
            "task_template_id": wire_tuple[6],
        }


class SerializeWorkflowRun:
    """Serialize the data to and from the database for a WorkflowRun."""

    @staticmethod
    def to_wire(id: int, workflow_id: int) -> tuple:
        """Submit the WorkflowRun information to the database."""
        return (id, workflow_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the WorkflowRun information from the database."""
        return {"id": int(wire_tuple[0]), "workflow_id": int(wire_tuple[1])}


class SerializeClusterType:
    """Serialize the data to and from the database for a ClusterType."""

    @staticmethod
    def to_wire(id: int, name: str, package_location: str) -> tuple:
        """Submit the ClusterType information to the database."""
        return (id, name, package_location)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Cluster information from the database."""
        return {
            "id": int(wire_tuple[0]),
            "name": str(wire_tuple[1]),
            "package_location": str(wire_tuple[2]),
        }


class SerializeCluster:
    """Serialize the data to and from the database for a Cluster."""

    @staticmethod
    def to_wire(
        id: int,
        name: str,
        cluster_type_name: str,
        connection_parameters: str,
    ) -> tuple:
        """Submit the Cluster information to the database."""
        return (id, name, cluster_type_name, connection_parameters)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Cluster information from the database."""
        connection_parameters = (
            json.loads(wire_tuple[3]) if wire_tuple[3] is not None else {}
        )
        return {
            "id": int(wire_tuple[0]),
            "name": str(wire_tuple[1]),
            "cluster_type_name": str(wire_tuple[2]),
            "connection_parameters": connection_parameters,
        }


class SerializeQueue:
    """Serialize the data to and from the database for a Queue."""

    @staticmethod
    def to_wire(id: int, name: str, parameters: str) -> tuple:
        """Submit the Queue information to the database."""
        return (id, name, parameters)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Queue information from the database."""
        return {
            "queue_id": int(wire_tuple[0]),
            "queue_name": str(wire_tuple[1]),
            "parameters": {}
            if wire_tuple[2] is None
            else ast.literal_eval(wire_tuple[2]),
        }


class SerializeTaskResourceUsage:
    """Serialize the data to and from the database for Task resource usage."""

    @staticmethod
    def to_wire(
        num_attempts: Optional[int] = None,
        nodename: Optional[str] = None,
        runtime: Optional[int] = None,
        memory: Optional[int] = None,
    ) -> tuple:
        """Submit the Task resource usage information to the database."""
        return num_attempts, nodename, runtime, memory

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Task resource usage information from the database."""
        return {
            "num_attempts": wire_tuple[0],
            "nodename": wire_tuple[1],
            "runtime": wire_tuple[2],
            "memory": wire_tuple[3],
        }


class SerializeTaskTemplateResourceUsage:
    """Serialize the data to and from the database for TaskTemplate resource usage."""

    @staticmethod
    def to_wire(
        num_tasks: Optional[int] = None,
        min_mem: Optional[int] = None,
        max_mem: Optional[int] = None,
        mean_mem: Optional[float] = None,
        min_runtime: Optional[int] = None,
        max_runtime: Optional[int] = None,
        mean_runtime: Optional[float] = None,
        median_mem: Optional[float] = None,
        median_runtime: Optional[float] = None,
        ci_mem: Optional[List[Any]] = None,
        ci_runtime: Optional[List[Any]] = None,
    ) -> tuple:
        """Submit the TaskTemplate resource usage information to the database."""
        return (
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

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the TaskTemplate resource usage information from the database."""
        return {
            "num_tasks": wire_tuple[0],
            "min_mem": wire_tuple[1],
            "max_mem": wire_tuple[2],
            "mean_mem": wire_tuple[3],
            "min_runtime": wire_tuple[4],
            "max_runtime": wire_tuple[5],
            "mean_runtime": wire_tuple[6],
            "median_mem": wire_tuple[7],
            "median_runtime": wire_tuple[8],
            "ci_mem": wire_tuple[9],
            "ci_runtime": wire_tuple[10],
        }


class SerializeDistributorArray:
    """Serialize the data to and from the database for DistributorArray."""

    @staticmethod
    def to_wire(array_id: int, max_concurrently_running: int, name: str) -> tuple:
        """Submit the TaskTemplate resource usage information to the database."""
        return array_id, max_concurrently_running, name

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the TaskTemplate resource usage information from the database."""
        return {
            "array_id": wire_tuple[0],
            "max_concurrently_running": wire_tuple[1],
            "name": wire_tuple[2],
        }


class SerializeDistributorWorkflow:
    """Serialize the data to and from the database for DistributorWorkflow."""

    @staticmethod
    def to_wire(workflow_id: int, dag_id: int, max_concurrently_running: int) -> tuple:
        """Serialize the workflow metadata used in the distributor."""
        return (
            workflow_id,
            dag_id,
            max_concurrently_running,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Deserialize the workflow metadata used in the distributor."""
        return {"workflow_id": wire_tuple[0], "max_concurrently_running": wire_tuple[1]}


class SerializeTaskResources:
    """Serialize the data to and from the database for Task resources."""

    @staticmethod
    def to_wire(
        task_resources_id: int,
        queue_name: str,
        task_resources_type_id: str,
        requested_resources: str,
    ) -> tuple:
        """Serialize the TaskResources metadata."""
        return (
            task_resources_id,
            queue_name,
            task_resources_type_id,
            requested_resources,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Deserialize the TaskResources metadata."""
        return {
            "task_resources_id": wire_tuple[0],
            "queue_name": wire_tuple[1],
            "task_resources_type_id": wire_tuple[2],
            "requested_resources": wire_tuple[3],
        }


class SerializeTaskInstanceBatch:
    """Serialize the data to and from the database for TaskInstance batch."""

    @staticmethod
    def to_wire(
        array_id: int,
        array_name: str,
        array_batch_num: int,
        task_resources_id: int,
        task_instance_ids: List[int],
    ) -> tuple:
        """Serialize the TaskInstanceBatch metadata."""
        return (
            array_id,
            array_name,
            array_batch_num,
            task_resources_id,
            task_instance_ids,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Deserialize the TaskInstanceBatch metadata."""
        return {
            "array_id": wire_tuple[0],
            "array_name": wire_tuple[1],
            "array_batch_num": wire_tuple[2],
            "task_resources_id": wire_tuple[3],
            "task_instance_ids": wire_tuple[4],
        }

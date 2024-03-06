"""Task Templates are versioned to recognize changes to args and command templates."""

from __future__ import annotations

import hashlib
from http import HTTPStatus as StatusCodes
import logging
from string import Formatter
from typing import Any, Dict, List, Optional, Tuple, Type, TYPE_CHECKING

from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester
from jobmon.core.serializers import SerializeClientTaskTemplateVersion

if TYPE_CHECKING:
    from jobmon.client.task_template import TaskTemplate

logger = logging.getLogger(__name__)


class TaskTemplateVersion:
    """Task Templates are versioned to recognize changes to args and command templates."""

    def __init__(
        self,
        command_template: str,
        node_args: list,
        task_args: list,
        op_args: list,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialization of task template version object."""
        # id vars
        self.command_template = command_template

        # hash args
        self._node_args: set
        self.node_args = set(node_args)
        self._task_args: set
        self.task_args = set(task_args)
        self._op_args: set
        self.op_args = set(op_args)

        # binding attributes
        self._task_template_version_id: int
        self._id_name_map: Dict
        self._task_template: TaskTemplate

        self.default_compute_resources_set: Dict[str, Dict[str, Any]] = {}
        self.default_resource_scales_set: Dict[str, Dict[str, float]] = {}
        self.default_cluster_name: str = ""
        self.default_max_attempts: Optional[int] = None

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

    @classmethod
    def get_task_template_version(
        cls: Type[TaskTemplateVersion],
        task_template: TaskTemplate,
        command_template: str,
        node_args: List[str] = [],
        task_args: List[str] = [],
        op_args: List[str] = [],
    ) -> TaskTemplateVersion:
        """Get a bound TaskTemplateVersion object from parameters.

        Args:
            task_template: TaskTemplate this version should be associated with.
            command_template: an abstract command representing a task, where the arguments to
                the command have defined names but the values are not assigned. eg: '{python}
                {script} --data {data} --para {para} {verbose}'
            node_args: any named arguments in command_template that make the command unique
                within this template for a given workflow run. Generally these are arguments
                that can be parallelized over.
            task_args: any named arguments in command_template that make the command unique
                across workflows if the node args are the same as a previous workflow.
                Generally these are arguments about data moving though the task.
            op_args: any named arguments in command_template that can change without changing
                the identity of the task. Generally these are things like the task executable
                location or the verbosity of the script.
        """
        task_template_version = cls(
            command_template,
            node_args,
            task_args,
            op_args,
            task_template.requester,
        )
        task_template_version.bind(task_template)
        return task_template_version

    @classmethod
    def from_wire(
        cls: Type[TaskTemplateVersion], wire_tuple: Tuple, task_template: TaskTemplate
    ) -> TaskTemplateVersion:
        """Get a bound TaskTemplateVersion object from the http wire format.

        Args:
            task_template: TaskTemplate this version should be associated with.
            wire_tuple: Wire format for ToolVersion defined in jobmon.serializers.
        """
        kwargs = SerializeClientTaskTemplateVersion.kwargs_from_wire(wire_tuple)

        # post bind args should be popped off and added as attrs
        task_template_version_id = kwargs.pop("task_template_version_id")
        id_name_map = kwargs.pop("id_name_map")
        task_template_id = kwargs.pop("task_template_id")
        if task_template_id != task_template.id:
            raise ValueError(
                "task_template_id from wire_tuple does not match task_template. "
                f"Expected {task_template.id} from wire_tuple. Got "
                f"{kwargs['task_template_id']}"
            )

        # instantiate and add attrs
        task_template_version = cls(
            command_template=kwargs["command_template"],
            node_args=kwargs["node_args"],
            task_args=kwargs["task_args"],
            op_args=kwargs["op_args"],
            requester=task_template.requester,
        )
        task_template_version._task_template = task_template
        task_template_version._task_template_version_id = task_template_version_id
        task_template_version._id_name_map = id_name_map
        return task_template_version

    def bind(self, task_template: TaskTemplate) -> None:
        """Bind task template version to the DB.

        Args:
            task_template: the TaskTemplate that this version is associated with.
        """
        if self.is_bound:
            return

        app_route = f"/task_template/{task_template.id}/add_version"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={
                "command_template": self.command_template,
                "arg_mapping_hash": self.arg_mapping_hash,
                "node_args": list(self.node_args),
                "task_args": list(self.task_args),
                "op_args": list(self.op_args),
            },
            request_type="post",
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )

        response_dict = SerializeClientTaskTemplateVersion.kwargs_from_wire(
            response["task_template_version"]
        )

        self._task_template = task_template
        self._task_template_version_id = response_dict["task_template_version_id"]
        self._id_name_map = response_dict["id_name_map"]

    @property
    def is_bound(self) -> bool:
        """If the task template version has been bound to the database."""
        return hasattr(self, "_task_template_version_id")

    @property
    def task_template(self) -> TaskTemplate:
        if not self.is_bound:
            raise AttributeError(
                "task_template cannot be accessed before TaskTemplateVersion is bound"
            )
        return self._task_template

    @property
    def id(self) -> int:
        """The unique ID of the task template version if it has been bound."""
        if not self.is_bound:
            raise AttributeError(
                "id cannot be accessed before TaskTemplateVersion is bound"
            )
        return self._task_template_version_id

    @property
    def id_name_map(self) -> Dict[str, int]:
        """Map of arg ids to arg names if bound to the db."""
        if not self.is_bound:
            raise AttributeError(
                "arg_id_name_map cannot be accessed before TaskTemplateVersion is bound"
            )
        return self._id_name_map

    @property
    def template_args(self) -> set:
        """The argument names in the command template."""
        return set(
            [i[1] for i in Formatter().parse(self.command_template) if i[1] is not None]
        )

    @property
    def node_args(self) -> set:
        """Return task template version node args.

        Any named arguments in command_template that make the command unique within this
        template for a given workflow run. Generally these are arguments that can be
        parallelized over.
        """
        return self._node_args

    @node_args.setter
    def node_args(self, val: set) -> None:
        """Set the node args."""
        if self.is_bound:
            raise AttributeError(
                "Cannot set node_args. node_args must be declared during "
                "instantiation"
            )
        if "name" in val:
            raise ValueError("Name is not allowed as a keyword in a command_template.")
        if not self.template_args.issuperset(val):
            raise ValueError(
                "The format keys declared in command_template must be a "
                "superset of the keys declared in node_args. Values received "
                f"were --- \ncommand_template is: {self.command_template}. "
                f"\ncommand_template format keys are {self.template_args}. "
                f"\nnode_args is: {val}. \nmissing format keys in "
                f"command_template are {set(val) - self.template_args}."
            )
        self._node_args = val

    @property
    def task_args(self) -> set:
        """Task template version task args.

        Any named arguments in command_template that make the command unique
        across workflows if the node args are the same as a previous workflow.
        Generally these are arguments about data moving though the task.
        """
        return self._task_args

    @task_args.setter
    def task_args(self, val: set) -> None:
        """Set the task args."""
        if self.is_bound:
            raise AttributeError(
                "Cannot set task_args. task_args must be declared during "
                "instantiation"
            )
        if "name" in val:
            raise ValueError("Name is not allowed as a keyword in a command_template.")
        if not self.template_args.issuperset(val):
            raise ValueError(
                "The format keys declared in command_template must bes a "
                "superset of the keys declared in task_args. Values received "
                f"were --- \ncommand_template is: {self.command_template}. "
                f"\ncommand_template format keys are {self.template_args}. "
                f"\ntask_args is: {val}. \nmissing format keys in "
                f"command_template are {set(val) - self.template_args}."
            )
        self._task_args = val

    @property
    def op_args(self) -> set:
        """Return the task template version OP args.

        Any named arguments in command_template that can change without changing the
        identity of the task. Generally these are things like the task executable location or
        the verbosity of the script.
        """
        return self._op_args

    @op_args.setter
    def op_args(self, val: set) -> None:
        """Setting op args."""
        if self.is_bound:
            raise AttributeError(
                "Cannot set op_args. op_args must be declared during " "instantiation"
            )
        if "name" in val:
            raise ValueError("Name is not allowed as a keyword in a command_template.")
        if not self.template_args.issuperset(val):
            raise ValueError(
                "The format keys declared in command_template must be a "
                "superset of the keys declared in op_args. Values received "
                f"were --- \ncommand_template is: {self.command_template}. "
                f"\ncommand_template format keys are {self.template_args}. "
                f"\nop_args is: {val}. \nmissing format keys in "
                f"command_template are {set(val) - self.template_args}."
            )
        self._op_args = val

    @property
    def arg_mapping_hash(self) -> int:
        """Hash args to identify unique task_template."""
        node_args = "".join(sorted(self.node_args))
        task_args = "".join(sorted(self.task_args))
        op_args = "".join(sorted(self.op_args))
        hashable = ",".join([node_args, task_args, op_args])
        return int(hashlib.sha256(hashable.encode("utf-8")).hexdigest(), 16)

    def filter_kwargs(self, arg_type: str, **kwargs: str) -> Dict[str, Any]:
        """Return the set of kwargs that are of arg_type.

        Args:
            arg_type: either node_args, task_args, op_args
            kwargs: the key/value pairs to be filtered by type
        """
        arg_type_set_map = {
            "node_args": self.node_args,
            "task_args": self.task_args,
            "op_args": self.op_args,
        }
        arg_set = arg_type_set_map[arg_type]
        result = {}
        for key, val in kwargs.items():
            if key in arg_set:
                result[key] = val
        return result

    def convert_arg_names_to_ids(self, **kwargs: str) -> Dict[int, Any]:
        """Map from names to ids."""
        return {self.id_name_map[k]: str(v) for k, v in kwargs.items()}

    def update_default_compute_resources(
        self, cluster_name: str, **kwargs: Any
    ) -> None:
        """Update compute resources in place only overridding specified keys.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to modify default values for.
            **kwargs: any key/value pair you want to update specified as an argument.
        """
        compute_resources = {cluster_name: kwargs}
        self.default_compute_resources_set.update(compute_resources)

    def update_default_resource_scales(self, cluster_name: str, **kwargs: Any) -> None:
        """Update resource scales in place only overridding specified keys.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to modify default values for.
            **kwargs: any key/value pair you want to update specified as an argument.
        """
        resource_scales = {cluster_name: kwargs}
        self.default_resource_scales_set.update(resource_scales)

    def set_default_compute_resources_from_dict(
        self, cluster_name: str, compute_resources: Dict[str, Any]
    ) -> None:
        """Set compute resources for a given cluster_name.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to set default values for.
            compute_resources: dictionary of default compute resources to run tasks
                with. Can be overridden at task template or task level.
                dict of {resource_name: resource_value}
        """
        self.default_compute_resources_set[cluster_name] = compute_resources

    def set_default_resource_scales_from_dict(
        self, cluster_name: str, resource_scales: Dict[str, float]
    ) -> None:
        """Set compute resources and scales for a given cluster_name.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to set default values for.
            resource_scales: dictionary of default resource scales to adjust task
                resources with. Can be overridden at task level.
                dict of {resource_name: scale_value}
        """
        self.default_resource_scales_set[cluster_name] = resource_scales

    def set_default_max_attempts(self, value: int) -> None:
        """Set default max attempts at tool leve.

        Args:
            value: the default max attempts value.
        """
        self.default_max_attempts = value

    def __hash__(self) -> int:
        """Unique identifier for this object."""
        hash_value = hashlib.sha256()
        hash_value.update(bytes(str(self.arg_mapping_hash).encode("utf-8")))
        hash_value.update(bytes(str(self.command_template).encode("utf-8")))
        return int(hash_value.hexdigest(), 16)

    def __repr__(self) -> str:
        """A representation string for a TaskTemplateVersion instance."""
        repr_string = (
            f"TaskTemplateVersion(command_template={self.command_template}, "
            f"node_args={self.node_args}, "
            f"task_args={self.task_args}, "
            f"op_args={self.op_args}"
        )
        try:
            repr_string += f", id={self.id}"
        except AttributeError:
            repr_string += ")"

        return repr_string

"""A logical instance of a project or model that will be run many times over."""

from __future__ import annotations

from http import HTTPStatus as StatusCodes
import logging
from typing import Any, Dict, Optional, Tuple, Type, TYPE_CHECKING

from jobmon.client.task_template import TaskTemplate
from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester
from jobmon.core.serializers import SerializeClientToolVersion

if TYPE_CHECKING:
    from jobmon.client.tool import Tool


logger = logging.getLogger(__name__)


class ToolVersion:
    """Represents a logical instance of a project or model that will be run many times over."""

    def __init__(
        self, tool_version_id: int, requester: Optional[Requester] = None
    ) -> None:
        """Instantiate a tool version.

        Args:
            tool_version_id: an integer id associated with a Tool
            requester: communicate with the flask services.
        """
        self.id = tool_version_id
        self._tool: Tool

        self.task_templates: Dict[str, TaskTemplate] = {}

        self.default_compute_resources_set: Dict[str, Dict[str, Any]] = {}
        self.default_resource_scales_set: Dict[str, Dict[str, float]] = {}
        self.default_cluster_name: str = ""
        self.default_max_attempt: Optional[int] = None

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

    @classmethod
    def get_tool_version(cls: Type[ToolVersion], tool: Tool) -> ToolVersion:
        """Get a new instance of a ToolVersion from the database.

        Args:
            tool: a Tool to get a version from.
            tool_version_id: tool_version_id to get from the database.
        """
        message = {"tool_id": tool.id}
        app_route = "/tool_version"
        return_code, response = tool.requester.send_request(
            app_route=app_route, message=message, request_type="post"
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        tool_version = cls.from_wire(response["tool_version"], tool)
        return tool_version

    @classmethod
    def from_wire(cls: Type[ToolVersion], wire_tuple: Tuple, tool: Tool) -> ToolVersion:
        """Convert from the wire format of ToolVersion to an instance.

        Args:
            wire_tuple: Wire format for ToolVersion defined in jobmon.serializers.
            tool: The Tool object to verify the right tool_version based on tool_id.
        """
        tool_version_kwargs = SerializeClientToolVersion.kwargs_from_wire(wire_tuple)

        if tool_version_kwargs["tool_id"] != tool.id:
            raise ValueError(
                "tool_id in wire_tuple does not match tool object. "
                f"Expected {tool.id} in wire_tuple. Got "
                f"{tool_version_kwargs['tool_id']}"
            )

        tool_version = cls(tool_version_kwargs["id"], requester=tool.requester)
        tool_version._tool = tool
        return tool_version

    @property
    def tool(self) -> Tool:
        """The Tool this ToolVersion is associated with."""
        return self._tool

    def load_task_templates(self) -> None:
        """Get all task_templates associated with this tool version from the database."""
        app_route = f"/tool_version/{self.id}/task_templates"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get"
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST request through route "
                f"{app_route}. Expected code 200. Response content: {response}"
            )
        task_templates = [
            TaskTemplate.from_wire(wire_tuple, self)
            for wire_tuple in response["task_templates"]
        ]
        for task_template in task_templates:
            self.task_templates[task_template.template_name] = task_template
            task_template.load_task_template_versions()

    def get_task_template(self, template_name: str) -> TaskTemplate:
        """Get a single task_template associated with this tool version from the database."""
        task_template = self.task_templates.get(template_name)
        if task_template is None:
            task_template = TaskTemplate.get_task_template(self, template_name)
            task_template.load_task_template_versions()
            self.task_templates[template_name] = task_template
        return task_template

    def update_default_compute_resources(
        self, cluster_name: str, **kwargs: Any
    ) -> None:
        """Update default compute resources in place only overridding specified keys.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to modify default values for.
            **kwargs: any key/value pair you want to update specified as an argument.
        """
        compute_resources = {cluster_name: kwargs}
        self.default_compute_resources_set.update(compute_resources)

    def update_default_resource_scales(self, cluster_name: str, **kwargs: Any) -> None:
        """Update default resource scales in place only overridding specified keys.

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
        """Set default compute resources for a given cluster_name.

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
        """Set default resource scales for a given cluster_name.

        If no default cluster is specified when this method is called, cluster_name will
        become the default cluster.

        Args:
            cluster_name: name of cluster to set default values for.
            resource_scales: dictionary of default resource scales to adjust task
                resources with. Can be overridden at task template or task level.
                dict of {resource_name: scale_value}
        """
        self.default_resource_scales_set[cluster_name] = resource_scales

    def set_default_max_attempts(self, value: int) -> None:
        """Set default max attempts at tool leve.

        Args:
            value: the default max attempts value.
        """
        if value:
            self.default_max_attempt = value
        else:
            logger.info("The default_max_attempt for tool_version can not be None.")

    def __repr__(self) -> str:
        """A representation string for a ToolVersion instance."""
        return (
            f"ToolVersion(tool_version_id={self.id},"
            f"task_templates: {[t for t in self.task_templates.keys()]})"
        )

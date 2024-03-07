"""The client Task Resources with the resources initiation and binding to Task ID."""

from __future__ import annotations

import hashlib
from http import HTTPStatus as StatusCodes
import json
import logging
from math import ceil
import numbers
import re
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

from jobmon.client.units import MemUnit, TimeUnit
from jobmon.core.cluster_protocol import ClusterQueue
from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester


logger = logging.getLogger(__name__)


class TaskResources:
    """An object representing the resources for a specific task."""

    def __init__(
        self,
        requested_resources: Dict[str, Any],
        queue: ClusterQueue,
        requester: Optional[Requester] = None,
    ) -> None:
        """Initialize the task resource object."""
        for resource, value in requested_resources.items():
            if resource == "memory":
                requested_resources[resource] = self.convert_memory_to_gib(value)
            if resource == "runtime":
                requested_resources[resource] = self.convert_runtime_to_s(value)
        self.requested_resources = requested_resources
        self.queue = queue

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

    @property
    def is_bound(self) -> bool:
        """If the TaskResources has been bound to the database."""
        return hasattr(self, "_id")

    @property
    def id(self) -> int:
        """If the task resources has been bound to the database."""
        if not self.is_bound:
            raise AttributeError(
                "Cannot access id until TaskResources is bound to database"
            )
        return self._id

    def bind(self) -> None:
        """Bind TaskResources to the database."""
        # Check if it's already been bound
        if self.is_bound:
            logger.debug(
                "This task resource has already been bound, and assigned"
                f"task_resources_id {self.id}"
            )
            return

        app_route = "/task/bind_resources"
        msg = {
            "queue_id": self.queue.queue_id,
            "task_resources_type_id": "O",
            "requested_resources": self.requested_resources,
        }
        return_code, response = self.requester.send_request(
            app_route=app_route, message=msg, request_type="post"
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )
        self._id = response

    def validate_resources(
        self: TaskResources, strict: bool = False
    ) -> Tuple[bool, str]:
        is_valid, msg = self.queue.validate_resources(
            strict, **self.requested_resources
        )
        return is_valid, msg

    def coerce_resources(self: TaskResources) -> TaskResources:
        """Coerce TaskResources to fit on queue. If resources change return a new object."""
        valid_resources = self.queue.coerce_resources(**self.requested_resources)
        coerced_task_resources = self.__class__(valid_resources, self.queue)
        if coerced_task_resources != self:
            return coerced_task_resources
        else:
            return self

    def adjust_resources(
        self: TaskResources,
        resource_scales: Dict[
            str, Union[numbers.Number, Callable, Iterator[numbers.Number]]
        ],
        fallback_queues: Optional[List[ClusterQueue]] = None,
    ) -> TaskResources:
        """Adjust TaskResources after a resource error, returning a new object if it changed.

        Args:
            resource_scales: Specifies how much to scale the failed Task's resources by.
                Scale factor can be a numeric value, a Callable that will be applied
                to the existing resources, or an Iterator. Any Callable should take
                a single numeric value as its sole argument. Any Iterator should only yield
                numeric values. Any Iterable can be easily converted to an Iterator by using
                the iter() built-in (e.g. iter([80, 160, 190])).
            fallback_queues: list of queues that users specify. If their jobs exceed the
                resources of a given queue, Jobmon will try to run their jobs on the fallback
                queues.
        """
        if fallback_queues is None:
            fallback_queues = []
        existing_resources = self.requested_resources.copy()
        resource_updates: Dict[str, Any] = {}

        # Only cores, memory, and runtime get scaled
        for resource, scaler in resource_scales.items():
            if resource in existing_resources.keys():
                if isinstance(scaler, numbers.Number):
                    new_resource_value = self.scale_val(
                        existing_resources[resource], scaler  # type: ignore
                    )
                elif callable(scaler):
                    new_resource_value = scaler(existing_resources[resource])
                elif isinstance(scaler, Iterator):
                    try:
                        new_resource_value = next(scaler)  # type: ignore
                    except StopIteration:
                        logger.warning(
                            "Not enough elements left in Iterator, re-using previous value "
                            f"for {resource}: {existing_resources[resource]}"
                        )
                        new_resource_value = existing_resources[resource]
                else:
                    raise ValueError(
                        "Keys in the resource_scales dictionary must be either numeric "
                        f"values, Iterators, or Python Callables; found {scaler}, type "
                        f"{type(scaler)} instead."
                    )
                if not isinstance(new_resource_value, numbers.Number):
                    raise ValueError(
                        "Attemping to update resource to a non-numeric value, "
                        f"{new_resource_value}. If passing an Iterator, elements must be "
                        "numeric values. If passing a Callable, return value must be a "
                        "numeric value."
                    )
                resource_updates[resource] = new_resource_value

        scaled_resources = dict(existing_resources, **resource_updates)

        # If it fails, try the fallback queues.
        queues = [self.queue] + fallback_queues
        while queues:
            next_queue = queues.pop(0)
            is_valid, _ = next_queue.validate_resources(strict=True, **scaled_resources)
            if is_valid:
                valid_resources = scaled_resources
                break
        else:  # no break
            # We've run out of queues so use the final queue and coerce
            valid_resources = next_queue.coerce_resources(**scaled_resources)

        adjust_resources = self.__class__(valid_resources, next_queue)
        if adjust_resources != self:
            return adjust_resources
        else:
            return self

    @staticmethod
    def convert_memory_to_gib(memory_str: str) -> int:
        """Given a memory request with a unit suffix, convert to GiB."""
        try:
            # User could pass in a raw value for memory, assume to be in GiB.
            # This is also the path taken by adjust
            return int(memory_str)
        except ValueError:
            return MemUnit.convert(memory_str, to="G")

    @staticmethod
    def convert_runtime_to_s(time_str: Union[str, float, int]) -> int:
        """Given a runtime request, coerce to seconds for recording in the DB."""
        try:
            # If a numeric is provided, assumed to be in seconds
            return int(time_str)
        except ValueError:
            time_str = str(time_str).lower()

            # convert to seconds if its datetime with a supported format
            try:
                hours, minutes, seconds = time_str.split(":")
                time_seconds = (
                    TimeUnit.hour_to_sec(int(hours))
                    + TimeUnit.min_to_sec(int(minutes))
                    + int(seconds)
                )
                return time_seconds
            except ValueError:
                try:
                    raw_value, unit = re.findall(r"[A-Za-z]+|\d+", time_str)
                except ValueError:
                    # Raised if there are not exactly 2 values to unpack from above regex
                    raise ValueError(
                        "The provided runtime request must be in a format of numbers "
                        "followed by one or two characters indicating the unit. "
                        "E.g. 1h, 60m, 3600s."
                    )

                if "h" in unit:
                    # Hours provided
                    return TimeUnit.hour_to_sec(int(raw_value))
                elif "m" in unit:
                    # Minutes provided
                    return TimeUnit.min_to_sec(int(raw_value))
                elif "s" in unit:
                    return int(raw_value)
                else:
                    raise ValueError("Expected one of h, m, s as the suffixed unit.")

    @staticmethod
    def scale_val(val: int, scaling_factor: float) -> float:
        """Used ceil instead of round or floor, to handle case when resources is 1.

        For example, if runtime was 1, resource scales was 0.2. Then the resource would adjust
        to 1.2, which would be truncated to 1 again if using floor/round.
        """
        return int(ceil(val * (1 + scaling_factor)))

    def __hash__(self) -> int:
        """Determine the hash of a task resources object."""
        # Note: this algorithm assumes all keys and values in the resources dict are
        # JSON-serializable. Since that's a requirement for logging in the database,
        # this assumption should be safe.

        # Uniqueness is determined by queue name and the resources parameter.
        if not hasattr(self, "_hash_val"):
            hashval = hashlib.sha256()
            hashval.update(bytes(str(hash(self.queue.queue_name)).encode("utf-8")))
            resources_str = str(
                hash(json.dumps(self.requested_resources, sort_keys=True))
            )
            hashval.update(bytes(resources_str.encode("utf-8")))
            self._hash_val = int(hashval.hexdigest(), 16)
        return self._hash_val

    def __eq__(self, other: object) -> bool:
        """Check equality of task resources objects."""
        if not isinstance(other, TaskResources):
            return False
        return hash(self) == hash(other)

    def __repr__(self) -> str:
        """A representation string for a TaskResources instance."""
        repr_string = (
            f"TaskResources(queue={self.queue.queue_name}, "
            f"requested_resources={self.requested_resources}"
        )

        try:
            repr_string += f", id={self.id})"
        except AttributeError:
            repr_string += ")"
        return repr_string

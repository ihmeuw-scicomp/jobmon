"""The client for the Multiprocess executor."""
from typing import Dict, List, Tuple, Union

from jobmon.core.cluster_protocol import ClusterQueue


class MultiprocessQueue(ClusterQueue):
    """Implementation of the multiprocess executor queue, derived from ClusterQueue."""

    def __init__(self, queue_id: int, queue_name: str, parameters: Dict) -> None:
        """Intialization of the multiprocess queue.

        Get the limits from the database in the client.
        """
        self._queue_id = queue_id
        self._queue_name = queue_name
        self._parameters = parameters

    def validate_resources(
        self, strict: bool = False, **kwargs: Union[str, int, float]
    ) -> Tuple[bool, str]:
        """Ensure cores requested isn't more than available on that node."""
        is_valid = True
        msg = ""
        cores = kwargs.get("cores")
        core_parameters = self.parameters.get("cores")

        if core_parameters:
            min_cores, max_cores = core_parameters
        else:
            raise ValueError("min_cores and max_cores parameters not set on queue.")

        if cores:
            if cores > max_cores:
                msg += (
                    f"ResourceError: provided cores {cores} exceeds "
                    f"queue limit of {max_cores} "
                    f"for queue {self.queue_name}"
                )
                cores = max_cores
                if strict:
                    is_valid = False
            elif cores < min_cores:
                msg += (
                    f"ResourceError: provided cores {cores} is below "
                    f"queue minimum of {min_cores} "
                    f"for queue {self.queue_name}"
                )
                cores = min_cores
                if strict:
                    is_valid = False
        else:
            # Set cores to the queue minimum
            msg += f"Cores not provided, setting to {self.queue_name} minimum of {min_cores}"
            cores = min_cores
            if strict:
                is_valid = False
        return is_valid, msg

    def coerce_resources(self, **kwargs: Union[str, int, float]) -> Dict:
        return kwargs

    @property
    def queue_id(self) -> int:
        """Return the ID of the queue."""
        return self._queue_id

    @property
    def queue_name(self) -> str:
        """Return the name of the queue."""
        return self._queue_name

    @property
    def parameters(self) -> Dict:
        """Return the dictionary of parameters."""
        return self._parameters

    @property
    def required_resources(self) -> List:
        """No required resources specified for dummy executor, return empty list."""
        return []

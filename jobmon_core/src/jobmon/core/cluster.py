"""Cluster objects define where a user wants their tasks run. e.g. UGE, Azure, Seq."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from jobmon.core.cluster_protocol import (
    ClusterDistributor,
    ClusterQueue,
    ClusterWorkerNode,
)
from jobmon.core.cluster_type import ClusterType
from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import http_request_ok, Requester
from jobmon.core.serializers import SerializeCluster, SerializeQueue


logger = logging.getLogger(__name__)


class Cluster:
    """Cluster objects define where a user wants their tasks run. e.g. UGE, Azure, Seq."""

    def __init__(
        self, cluster_name: str, requester: Optional[Requester] = None
    ) -> None:
        """Initialization of Cluster."""
        self.cluster_name = cluster_name

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

        self.queues: Dict[str, ClusterQueue] = {}

    @classmethod
    def get_cluster(
        cls: Any, cluster_name: str, requester: Optional[Requester] = None
    ) -> Cluster:
        """Get a bound instance of a Cluster.

        Args:
            cluster_name: the name of the cluster
            requester (Requester): requester object to connect to Flask service.
        """
        cluster = cls(cluster_name, requester)
        cluster.bind()
        return cluster

    def bind(self) -> None:
        """Bind Cluster to the database, getting an id back."""
        app_route = f"/cluster/{self.cluster_name}"
        return_code, response = self.requester.send_request(
            app_route=app_route, message={}, request_type="get"
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {response}"
            )
        cluster_kwargs = SerializeCluster.kwargs_from_wire(response["cluster"])

        self._cluster_id = cluster_kwargs["id"]
        cluster_type = ClusterType(cluster_kwargs["cluster_type_name"])
        self._cluster_type = cluster_type
        self._connection_parameters = cluster_kwargs["connection_parameters"]

    @property
    def connection_parameters(self) -> Dict:
        """The connection parameters."""
        return self._connection_parameters

    @property
    def is_bound(self) -> bool:
        """If the Cluster has been bound to the database."""
        return hasattr(self, "_cluster_id")

    @property
    def id(self) -> int:
        """Unique id from database if Cluster has been bound."""
        if not self.is_bound:
            raise AttributeError("Cannot access id until Cluster is bound to database")
        return self._cluster_id

    def get_worker_node(self) -> ClusterWorkerNode:
        """Get the cluster specific worker_node interface."""
        cluster_worker_node_class = self._cluster_type.cluster_worker_node_class
        return cluster_worker_node_class()

    def get_distributor(self) -> ClusterDistributor:
        """Get the cluster specific distributor interface."""
        # TODO: read in cluster args from config here?
        distributor_class = self._cluster_type.cluster_distributor_class
        return distributor_class(self.cluster_name, **self._connection_parameters)

    def get_queue(self, queue_name: str) -> ClusterQueue:
        """Get the ClusterQueue object associated with a given queue_name.

        Checks if queue object is in the cache, if it's not it will query the database and add
        the queue object to the cache.

        Args:
            queue_name: name of the queue you want.
        """
        # this is cached so should be fast
        try:
            queue = self.queues[queue_name]
        except KeyError:
            queue_class = self._cluster_type.cluster_queue_class
            app_route = f"/cluster/{self.id}/queue/{queue_name}"
            return_code, response = self.requester.send_request(
                app_route=app_route, message={}, request_type="get"
            )
            if http_request_ok(return_code) is False:
                raise InvalidResponse(
                    f"Unexpected status code {return_code} from POST "
                    f"request through route {app_route}. Expected code "
                    f"200. Response content: {response}"
                )
            queue_kwargs = SerializeQueue.kwargs_from_wire(response["queue"])
            queue = queue_class(**queue_kwargs)
            self.queues[queue_name] = queue

        return queue

"""jobmon built-in."""

from importlib.metadata import version
from typing import Type

from jobmon.core.cluster_protocol import (
    ClusterDistributor,
    ClusterQueue,
    ClusterWorkerNode,
)

__version__ = version("jobmon_core")


def get_cluster_queue_class() -> Type[ClusterQueue]:
    """Return the queue class for the Multiprocess executor."""
    from jobmon.plugins.multiprocess.multiproc_queue import MultiprocessQueue

    return MultiprocessQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    """Return the cluster distributor for the Multiprocess executor."""
    from jobmon.plugins.multiprocess.multiproc_distributor import (
        MultiprocessDistributor,
    )

    return MultiprocessDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    """Return the cluster worker node class for the Multiprocess executor."""
    from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessWorkerNode

    return MultiprocessWorkerNode

"""jobmon_uge."""

from importlib.metadata import version
from typing import Type

from jobmon.core.cluster_protocol import (
    ClusterDistributor,
    ClusterQueue,
    ClusterWorkerNode,
)

__version__ = version("jobmon_core")


def get_cluster_queue_class() -> Type[ClusterQueue]:
    """Return the queue class for the Sequential executor."""
    from jobmon.plugins.sequential.seq_queue import SequentialQueue

    return SequentialQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    """Return the cluster distributor class for the Sequential executor."""
    from jobmon.plugins.sequential.seq_distributor import SequentialDistributor

    return SequentialDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    """Return the cluster worker node class for the Sequential executor."""
    from jobmon.plugins.sequential.seq_distributor import SequentialWorkerNode

    return SequentialWorkerNode

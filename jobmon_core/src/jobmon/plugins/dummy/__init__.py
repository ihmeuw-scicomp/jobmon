"""Dummy Executor fakes execution for testing purposes."""

from __future__ import annotations

from importlib.metadata import version
import logging
from typing import Type

from jobmon.core.cluster_protocol import (
    ClusterDistributor,
    ClusterQueue,
    ClusterWorkerNode,
)

logger = logging.getLogger(__name__)

__version__ = version("jobmon_core")


def get_cluster_queue_class() -> Type[ClusterQueue]:
    """Return the queue class for the dummy executor."""
    from jobmon.plugins.dummy.dummy_queue import DummyQueue

    return DummyQueue


def get_cluster_distributor_class() -> Type[ClusterDistributor]:
    """Return the cluster distributor for the dummy executor."""
    from jobmon.plugins.dummy.dummy_distributor import DummyDistributor

    return DummyDistributor


def get_cluster_worker_node_class() -> Type[ClusterWorkerNode]:
    """Return the cluster worker node class for the dummy executor."""
    from jobmon.plugins.dummy.dummy_distributor import DummyWorkerNode

    return DummyWorkerNode

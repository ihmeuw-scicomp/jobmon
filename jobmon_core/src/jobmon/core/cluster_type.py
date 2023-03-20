"""Inteface definition for jobmon executor plugins."""
from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Dict, Type

from jobmon.core.cluster_protocol import (
    ClusterDistributor,
    ClusterQueue,
    ClusterWorkerNode,
)
import jobmon.plugins


class ClusterType:
    _interface = [
        "get_cluster_queue_class",
        "get_cluster_distributor_class",
        "get_cluster_worker_node_class",
    ]

    _cache: Dict[str, ClusterType] = {}

    def __new__(cls, *args: str, **kwds: str) -> ClusterType:
        key = args[0] if args else kwds["cluster_type_name"]
        inst = cls._cache.get(key, None)
        if inst is None:
            inst = super(ClusterType, cls).__new__(cls)
            inst.__init__(key)  # type: ignore
            cls._cache[key] = inst
        return inst

    def __init__(self, cluster_type_name: str) -> None:
        """Initialization of ClusterType object."""
        self.cluster_type_name = cluster_type_name

        self._plugins = {
            name: importlib.import_module(name)
            for finder, name, ispkg in pkgutil.iter_modules(
                jobmon.plugins.__path__, jobmon.plugins.__name__ + "."
            )
        }
        self._package_location = ""

    @property
    def plugin(self) -> Any:
        """If the cluster is bound, return the cluster interface for the type of cluster."""
        module = self._plugins.get(f"jobmon.plugins.{self.cluster_type_name}")
        if module is not None:
            msg = ""
            for func in self._interface:
                if not hasattr(module, func):
                    msg += f"Required function {func} missing from plugin interface. \n"
            if msg:
                raise AttributeError(
                    f"Invalid jobmon plugin {self.cluster_type_name}" + msg
                )
        else:
            msg = f"Interface not found for cluster_type_name={self.cluster_type_name}"
            raise ValueError(msg)
        return module

    @property
    def cluster_queue_class(self) -> Type[ClusterQueue]:
        return self.plugin.get_cluster_queue_class()

    @property
    def cluster_distributor_class(self) -> Type[ClusterDistributor]:
        return self.plugin.get_cluster_distributor_class()

    @property
    def cluster_worker_node_class(self) -> Type[ClusterWorkerNode]:
        return self.plugin.get_cluster_worker_node_class()

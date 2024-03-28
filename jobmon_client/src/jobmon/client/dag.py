"""The DAG captures the interconnected graph of tasks and their dependencies."""

import hashlib
from http import HTTPStatus as StatusCodes
import logging
from typing import Dict, List, Optional, Set, Tuple, Union

from jobmon.client.node import Node
from jobmon.core.exceptions import (
    CyclicGraphError,
    DuplicateNodeArgsError,
    InvalidResponse,
    NodeDependencyNotExistError,
)
from jobmon.core.requester import Requester

logger = logging.getLogger(__name__)


class Dag(object):
    """The DAG captures the interconnected graph of tasks and their dependencies."""

    def __init__(self, requester: Optional[Requester] = None) -> None:
        """Directed Acyclic Graph.

        The DAG captures the tasks (nodes) as they are
        related to each other in their dependency structure. The Dag is traversed in
        the order of node dependencies so a workflow run is a single instance of
        traversing through a dag. This object stores the nodes and communicates
        with the server with regard to itself.

        Args:
            requester (str): url to communicate with the flask services.
        """
        self.nodes: Set[Node] = set()

        if requester is None:
            requester = Requester.from_defaults()
        self.requester = requester

    @property
    def dag_id(self) -> int:
        """Database unique ID of this DAG."""
        if not hasattr(self, "_dag_id"):
            raise AttributeError("_dag_id cannot be accessed before dag is bound")
        return self._dag_id

    def add_node(self, node: Node) -> None:
        """Add a node to this dag.

        Args:
            node (jobmon.client.node.Node): Node to add to the dag
        """
        # validate node has unique node args within this task template version
        if node in self.nodes:
            raise DuplicateNodeArgsError(
                "A duplicate node was found for task_template_version_id="
                f"{node.task_template_version_id}. Node args were {node.node_args}"
            )
        # wf.add_task should call ClientNode.add_node() + pass the tasks' node
        self.nodes.add(node)

    def bind(self, chunk_size: int = 500) -> int:
        """Retrieve an id for a matching dag from the server.

        If it doesn't exist, first create one, including its edges.
        """
        if len(self.nodes) == 0:
            raise RuntimeError(
                "No nodes were found in the dag. An empty dag " "cannot be bound."
            )

        self._bulk_bind_nodes(chunk_size)

        dag_hash = hash(self)
        _, response = self.requester.send_request(
            app_route="/dag",
            message={"dag_hash": dag_hash},
            request_type="post",
        )
        dag_id = response["dag_id"]

        # no created date means bind edges
        if response["created_date"] is None:
            self._bulk_insert_edges(dag_id)
        self._dag_id = dag_id
        return dag_id

    def validate(self) -> None:
        """Validate the nodes and their dependencies."""
        nodes_in_dag = self.nodes
        for node in nodes_in_dag:
            # Make sure no task contains up/down stream tasks that are not in the workflow
            for n in node.upstream_nodes:
                if n not in nodes_in_dag:
                    raise NodeDependencyNotExistError(
                        f"Upstream node, {hash(n)}, for node, {hash(node)}, "
                        "does not exist in the dag.Check that every task has been added to "
                        "the workflow and is in the correct order."
                    )
            for n in node.downstream_nodes:
                if n not in nodes_in_dag:
                    raise NodeDependencyNotExistError(
                        f"Downstream node, {hash(n)}, for node, {hash(node)}, "
                        "does not exist in the dag.Check that every task has been added to "
                        "the workflow and is in the correct order."
                    )

        dag_map = {node: node.downstream_nodes for node in nodes_in_dag}
        if self._is_cyclic(dag_map):
            raise CyclicGraphError(
                "Cycle detected in the task graph. Please ensure that your task dependencies "
                "flow in only one direction."
            )

    def _is_cyclic(self, dag_map: Dict[Node, Set[Node]]) -> bool:
        """Return true if the nodes are cyclic.

        This method is effectively a depth-first search looking for already-seen nodes,
        implemented using the "stack of iterators" pattern to get around Python's recursion
        limit of 1000.
        """
        visited = set()
        path = [object()]
        path_set = set(path)
        stack = [iter(dag_map)]
        while stack:
            for v in stack[-1]:
                if v in path_set:
                    return True
                elif v not in visited:
                    visited.add(v)
                    path.append(v)
                    path_set.add(v)
                    stack.append(iter(dag_map.get(v, ())))
                    break
            else:
                path_set.remove(path.pop())
                stack.pop()
        return False

    def _bulk_bind_nodes(self, chunk_size: int) -> None:
        def get_chunk(total_nodes: int, chunk_number: int) -> Optional[Tuple[int, int]]:
            # This function is created for unit testing
            if (chunk_number - 1) * chunk_size >= total_nodes:
                return None
            return (
                (chunk_number - 1) * chunk_size,
                min(total_nodes - 1, chunk_number * chunk_size - 1),
            )

        nodes_in_dag = list(self.nodes)
        nodes_received = {}
        total_nodes = len(self.nodes)
        chunk_number = 1
        chunk_boarder = get_chunk(total_nodes, chunk_number)
        while chunk_boarder is not None:
            # do something to bind
            nodes_to_send = []
            for i in range(chunk_boarder[0], chunk_boarder[1] + 1):
                node = nodes_in_dag[i]
                n = {
                    "task_template_version_id": node.task_template_version_id,
                    "node_args_hash": str(node.node_args_hash),
                    "node_args": node.mapped_node_args,
                }
                nodes_to_send.append(n)
            rc, response = self.requester.send_request(
                app_route="/nodes",
                message={"nodes": nodes_to_send},
                request_type="post",
            )
            nodes_received.update(response["nodes"])
            chunk_number += 1
            chunk_boarder = get_chunk(total_nodes, chunk_number)

        for node in nodes_in_dag:
            k = f"{node.task_template_version_id}:{node.node_args_hash}"
            if k in nodes_received.keys():
                node.node_id = int(nodes_received[k])
            else:
                raise InvalidResponse(
                    f"Fail to find node_id in HTTP response for node_args_hash "
                    f"{node.node_args_hash} and task_template_version_id "
                    f"{node.task_template_version_id} HTTP Response:\n {response}"
                )

    def _get_dag_id(self) -> Optional[int]:
        dag_hash = hash(self)
        logger.info(f"Querying for dag with hash: {dag_hash}")
        return_code, response = self.requester.send_request(
            app_route="/dag",
            message={"dag_hash": dag_hash},
            request_type="get",
        )
        if return_code == StatusCodes.OK:
            return response["dag_id"]
        else:
            raise ValueError(
                f"Unexpected status code {return_code} from GET "
                f"request through route /dag/{dag_hash} . "
                f"Expected code 200. Response content: "
                f"{response}"
            )

    def _bulk_insert_edges(self, dag_id: int, chunk_size: int = 500) -> None:
        # compile full list of edges
        all_edges: List[Dict[str, Union[List, int]]] = []
        for node in self.nodes:
            # get the node ids for all upstream and downstream nodes
            upstream_nodes = [
                upstream_node.node_id for upstream_node in node.upstream_nodes
            ]
            downstream_nodes = [
                downstream_node.node_id for downstream_node in node.downstream_nodes
            ]
            all_edges.append(
                {
                    "node_id": node.node_id,
                    "upstream_node_ids": upstream_nodes,
                    "downstream_node_ids": downstream_nodes,
                }
            )
        logger.debug(f"message included in edge post request: {all_edges}")

        while all_edges:
            # split off first chunk elements from queue.
            edge_chunk, all_edges = all_edges[:chunk_size], all_edges[chunk_size:]

            message: Dict[str, Union[List[Dict], bool]] = {"edges_to_add": edge_chunk}
            # more edges to add. don't mark it created
            if all_edges:
                message["mark_created"] = False
            else:
                message["mark_created"] = True

            app_route = f"/dag/{dag_id}/edges"
            self.requester.send_request(
                app_route=app_route, message=message, request_type="post"
            )

    def __hash__(self) -> int:
        """Determined by hashing all sorted node hashes and their downstream."""
        hash_value = hashlib.sha256()
        if len(self.nodes) > 0:  # if the dag is empty, we want to skip this
            for node in sorted(self.nodes):
                hash_value.update(str(hash(node)).encode("utf-8"))
                for downstream_node in sorted(node.downstream_nodes):
                    hash_value.update(str(hash(downstream_node)).encode("utf-8"))
        return int(hash_value.hexdigest(), 16)

    def __repr__(self) -> str:
        """A representation string for a Dag instance."""
        return f"Dag(hash={self.__hash__()}"

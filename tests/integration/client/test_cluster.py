from jobmon.core.cluster import Cluster
from jobmon.plugins import sequential
from jobmon.plugins.sequential.seq_queue import SequentialQueue


def test_plugin_loading(client_env):
    cluster = Cluster(cluster_name="sequential")
    cluster.bind()
    assert cluster._cluster_type.plugin == sequential


def test_get_queue(client_env):
    cluster = Cluster(cluster_name="sequential")
    cluster.bind()

    sequential_queue = cluster.get_queue(queue_name="null.q")
    assert type(sequential_queue) == SequentialQueue
    assert sequential_queue == cluster.get_queue(queue_name="null.q")

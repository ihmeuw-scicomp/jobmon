import time

from jobmon.plugins.multiprocess.multiproc_distributor import MultiprocessDistributor


def test_multiprocess_distributor(tool, client_env, task_template, array_template):
    # set up a MultiprocessDistributor with 5 consumers.
    dist = MultiprocessDistributor("multiprocess", 5)

    dist.start()

    # submit 2 commands to non-array operation
    dist.submit_to_batch_distributor("echo 1", "echo_1", {"queue": "null.q"})
    dist.submit_to_batch_distributor("echo 2", "echo_2", {"queue": "null.q"})
    assert len(dist.consumers) == 5

    # we expect that dist.task_queue will be consumed by the consumers
    # fairly soon and become empty;
    # at that point, dict._running_or_submitted should have 2 items at most
    # (as _update_internal_states may drain it),
    # with the array_step_id being None, as this is a non-array operation.
    while not dist.task_queue.empty():
        time.sleep(1)
    assert len(dist._running_or_submitted) <= 2
    keys = dist._running_or_submitted.keys()
    for x in keys:
        assert "_" not in x

    dist.stop()

    # reset up a MultiprocessDistributor with 5 consumers.
    dist = MultiprocessDistributor("multiprocess", 5)

    dist.start()

    # submit 2 to array operation with array_length = 3
    dist.submit_array_to_batch_distributor("echo 1", "echo_1", {"queue": "null.q"}, 3)
    dist.submit_array_to_batch_distributor("echo 2", "echo_2", {"queue": "null.q"}, 3)

    # we expect that dist.task_queue will be consumed by the consumers
    # fairly soon and become empty;
    # at that point, dict._running_or_submitted should have 2 * 3 items at most
    # (as _update_internal_states may drain it),
    # with the array_step_id being some int(>0) (array_step_id), as this is an array operation.
    while not dist.task_queue.empty():
        time.sleep(1)
    assert len(dist._running_or_submitted) <= 2 * 3
    keys = dist._running_or_submitted.keys()
    for x in keys:
        assert "_" in x

    dist.stop()
